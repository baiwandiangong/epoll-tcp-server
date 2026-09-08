/* ============================================================================
 * epoll_tcp_server.c -- 基于 epoll 的多客户端 TCP 通信服务器（回显服务器）
 *
 * 目标：用最少的业务代码展示 Linux 网络编程核心能力：
 *   TCP 服务端(socket/bind/listen/accept) + 非阻塞 IO + epoll 事件驱动。
 *
 * 工作流程：
 *   程序启动 -> 创建 TCP 监听 socket -> 加入 epoll
 *   -> epoll 发现监听 fd 可读 -> accept 新连接并加入 epoll
 *   -> epoll 发现某个 client fd 可读 -> recv 并原样回显(echo)
 *   -> send 一次写不完(EAGAIN/部分写)时进入发送队列，注册 EPOLLOUT 继续发送
 *
 * 编译：  gcc -O2 -Wall -Wextra -o epoll_tcp_server src/main.c
 * 运行：  ./epoll_tcp_server [port]         默认端口 9000
 * 测试：  nc 127.0.0.1 9000    或   python3 client.py --stress 200
 * ========================================================================== */

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

/* ---------------------------- 常量定义 ---------------------------- */

#define DEFAULT_PORT   9000
#define MAX_EVENTS     128     /* epoll_wait 单次最多返回的事件数 */
#define READ_BUF_SIZE  4096    /* recv 读取缓冲区 */
#define OUT_BUF_SIZE   65536   /* 每个连接的发送队列上限 */

/* ---------------------------- 数据结构 ---------------------------- */

/* 一个客户端连接对应的全部状态 */
typedef struct {
    int      fd;               /* 客户端 socket fd */
    int      id;               /* 全局连接序号，方便日志观察 */
    char     ip[INET_ADDRSTRLEN];
    uint16_t port;
    size_t   bytes_in;         /* 该连接累计收到多少字节（统计用） */
    int      waiting_out;      /* epoll 上是否已注册 EPOLLOUT */
    char     out[OUT_BUF_SIZE]; /* 发送缓冲：send 没写完的数据先暂存这里 */
    size_t   out_len;           /* 发送队列里待发送的字节数 */
} Client;

static volatile sig_atomic_t g_running = 1;
static int g_total_clients  = 0;  /* 历史累计连接数 */
static int g_online_clients = 0;  /* 当前在线连接数 */

static int echo_to_client(int epoll_fd, Client *c,
                          const char *data, size_t len);

/* ============================ 基础工具函数 ============================ */

static void handle_signal(int sig)
{
    (void)sig;
    g_running = 0;
}

/* 把 fd 设为非阻塞，这是配合 epoll 的前提 */
static int set_nonblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0)
        return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

/* 修改某个客户端在 epoll 中注册的事件（EPOLL_CTL_MOD） */
static void mod_client_events(int epoll_fd, Client *c, uint32_t events)
{
    struct epoll_event ev;

    memset(&ev, 0, sizeof(ev));
    ev.events = events;
    ev.data.ptr = c;
    epoll_ctl(epoll_fd, EPOLL_CTL_MOD, c->fd, &ev);
}

/* ============================ 连接管理 ============================ */

/* 关闭并释放一个客户端连接 */
static void close_client(int epoll_fd, Client *c)
{
    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, c->fd, NULL);
    close(c->fd);
    g_online_clients--;
    printf("[close] client #%d %s:%d left, online=%d\n",
           c->id, c->ip, c->port, g_online_clients);
    free(c);
}

/* 把一个新 accept 出来的 fd 封装成 Client 并加入 epoll */
static void accept_new_connections(int epoll_fd, int listen_fd)
{
    for (;;) {
        struct sockaddr_in peer;
        socklen_t addr_len = sizeof(peer);
        int client_fd = accept(listen_fd, (struct sockaddr *)&peer, &addr_len);

        if (client_fd < 0) {
            if (errno == EINTR)
                continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                break;   /* 本次就绪的连接已全部 accept 完 */
            perror("accept");
            break;
        }

        Client *c = calloc(1, sizeof(*c));
        if (c == NULL) {
            perror("calloc");
            close(client_fd);
            continue;
        }
        if (set_nonblocking(client_fd) != 0) {
            close(client_fd);
            free(c);
            continue;
        }

        c->fd = client_fd;
        c->id = ++g_total_clients;
        inet_ntop(AF_INET, &peer.sin_addr, c->ip, sizeof(c->ip));
        c->port = ntohs(peer.sin_port);

        /* 用 ev.data.ptr 指向 Client，事件到来时直接拿到连接状态 */
        struct epoll_event ev;
        memset(&ev, 0, sizeof(ev));
        ev.events = EPOLLIN;
        ev.data.ptr = c;
        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev) != 0) {
            perror("epoll_ctl(ADD)");
            close(client_fd);
            free(c);
            continue;
        }

        g_online_clients++;
        printf("[accept] client #%d from %s:%d, online=%d\n",
               c->id, c->ip, c->port, g_online_clients);

        /* 连接建立后先发一条欢迎语，走和回显相同的发送路径 */
        char welcome[160];
        int n = snprintf(welcome, sizeof(welcome),
                         "[server] hello client #%d (%s:%d), send me something.\r\n",
                         c->id, c->ip, c->port);
        if (n < 0)
            n = 0;
        else if ((size_t)n >= sizeof(welcome))
            n = (int)sizeof(welcome) - 1;
        if (echo_to_client(epoll_fd, c, welcome, (size_t)n) != 0)
            close_client(epoll_fd, c);
    }
}
/* ============================ 发送逻辑 ============================ */

/* 把 data[0, len) 原样回显给客户端：
 *   1. 发送队列为空时先尝试立即 send；
 *   2. send 返回 EAGAIN 或只写了一半时，把没写完的数据放入发送队列，
 *      并给该连接注册 EPOLLOUT，等 socket 可写时再继续发；
 *   3. 队列放不下时返回 -1（调用方关闭连接，简化处理）。
 * 返回 0 表示正常，-1 表示需要断开该连接。 */
static int echo_to_client(int epoll_fd, Client *c,
                          const char *data, size_t len)
{
    if (c->out_len == 0) {
        ssize_t n = send(c->fd, data, len, 0);

        if (n > 0) {          /* 发了一部分 */
            data += n;
            len -= (size_t)n;
        } else if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
            return -1;        /* 真正的发送错误，断开 */
        }
        if (len == 0)         /* 一次就发完了 */
            return 0;
    }

    /* 剩余数据进入发送队列 */
    if (len > OUT_BUF_SIZE - c->out_len)
        return -1;            /* 队列满：客户端读太慢，断开（简化方案） */

    memcpy(c->out + c->out_len, data, len);
    c->out_len += len;

    if (!c->waiting_out) {    /* 注册 EPOLLOUT，等可写时继续发 */
        mod_client_events(epoll_fd, c, EPOLLIN | EPOLLOUT);
        c->waiting_out = 1;
    }
    return 0;
}

/* EPOLLOUT 触发：把发送队列里的数据尽量发完。
 * 返回 1 表示连接存活，0 表示已关闭。 */
static int flush_output(int epoll_fd, Client *c)
{
    while (c->out_len > 0) {
        ssize_t n = send(c->fd, c->out, c->out_len, 0);

        if (n > 0) {
            c->out_len -= (size_t)n;
            memmove(c->out, c->out + n, c->out_len);  /* 简化：剩余数据前移 */
            continue;
        }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
            return 1;         /* socket 仍不可写，保留 EPOLLOUT 继续等 */
        close_client(epoll_fd, c);
        return 0;
    }

    /* 队列已清空，撤掉 EPOLLOUT，避免可写事件一直触发造成忙等 */
    if (c->waiting_out) {
        mod_client_events(epoll_fd, c, EPOLLIN);
        c->waiting_out = 0;
    }
    return 1;
}

/* ============================ 读写事件处理 ============================ */

/* EPOLLIN 触发：循环 recv 直到 EAGAIN，把读到的数据原样回显。
 * 循环读(而不是只读一次)有两个作用：
 *   1) 一次事件把数据尽量收完，减少 epoll_wait 的次数；
 *   2) 该写法同时兼容水平触发(LT)和边沿触发(ET)。
 * 返回 1 表示连接存活，0 表示已关闭。 */
static int handle_readable(int epoll_fd, Client *c)
{
    char buf[READ_BUF_SIZE];

    for (;;) {
        ssize_t n = recv(c->fd, buf, sizeof(buf), 0);

        if (n > 0) {
            c->bytes_in += (size_t)n;
            if (echo_to_client(epoll_fd, c, buf, (size_t)n) != 0) {
                close_client(epoll_fd, c);
                return 0;
            }
            continue;          /* 继续读到 EAGAIN */
        }
        if (n == 0) {          /* 对端关闭(FIN)：正常断开 */
            printf("[recv] client #%d closed the connection\n", c->id);
            close_client(epoll_fd, c);
            return 0;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK)
            return 1;          /* 本次已读到的数据都处理完了 */
        close_client(epoll_fd, c);   /* 读出错 */
        return 0;
    }
}

/* ============================ 事件循环 ============================ */

static void event_loop(int epoll_fd, int listen_fd)
{
    struct epoll_event events[MAX_EVENTS];

    printf("[server] epoll running, waiting for connections ...\n");
    fflush(stdout);

    while (g_running) {
        int n = epoll_wait(epoll_fd, events, MAX_EVENTS, 1000);

        if (n < 0) {
            if (errno == EINTR)
                continue;
            perror("epoll_wait");
            break;
        }

        for (int i = 0; i < n; i++) {
            Client *c = events[i].data.ptr;

            /* 监听 fd 用 data.ptr == NULL 标记，客户端 fd 用 data.ptr 指向 Client */
            if (c == NULL) {
                accept_new_connections(epoll_fd, listen_fd);
                continue;
            }

            uint32_t rev = events[i].events;

            /* 注意：read 处理里可能已经把连接关闭并 free，
             * 所以后面的分支都要先判断 c 是否还活着。 */
            if ((rev & EPOLLIN) && !handle_readable(epoll_fd, c))
                continue;
            if ((rev & EPOLLOUT) && !flush_output(epoll_fd, c))
                continue;
            if (rev & (EPOLLERR | EPOLLHUP)) {
                printf("[error] client #%d abnormal event, close it\n", c->id);
                close_client(epoll_fd, c);
            }
        }
    }
}

/* ============================ TCP 服务端 ============================ */

/* 创建 TCP 监听 socket：socket -> setsockopt -> bind -> listen -> 非阻塞 */
static int create_listen_socket(int port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return -1;
    }

    int reuse = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons((uint16_t)port);

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        perror("bind");
        close(fd);
        return -1;
    }
    if (listen(fd, SOMAXCONN) != 0) {
        perror("listen");
        close(fd);
        return -1;
    }
    if (set_nonblocking(fd) != 0) {
        perror("fcntl");
        close(fd);
        return -1;
    }
    return fd;
}

/* ============================ 程序入口 ============================ */

int main(int argc, char *argv[])
{
    int port = DEFAULT_PORT;

    if (argc >= 2) {
        port = atoi(argv[1]);
        if (port <= 0 || port > 65535) {
            fprintf(stderr, "Invalid port: %s\n", argv[1]);
            return 1;
        }
    }

    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_sig    启动程序
        ↓
    读取端口参数
        ↓
    注册 SIGINT、SIGTERM、SIGPIPE
        ↓
    创建并配置监听 socket
        ↓
    创建 epoll
        ↓
    监听 socket 加入 epoll
        ↓
    进入 event_loop
        ↓
    处理客户端连接和数据
        ↓
    Ctrl+C
        ↓
    退出 event_loop
        ↓
    关闭 epoll 和监听 socket
        ↓
    正常退出    启动程序
        ↓
    读取端口参数
        ↓
    注册 SIGINT、SIGTERM、SIGPIPE
        ↓
    创建并配置监听 socket
        ↓
    创建 epoll
        ↓
    监听 socket 加入 epoll
        ↓
    进入 event_loop
        ↓
    处理客户端连接和数据
        ↓
    Ctrl+C
        ↓
    退出 event_loop
        ↓
    关闭 epoll 和监听 socket
        ↓
    正常退出nal);
    signal(SIGPIPE, SIG_IGN);   /* 避免往已关闭连接写数据时进程被杀 */

    int listen_fd = create_listen_socket(port);
    if (listen_fd < 0)
        return 1;

    int epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) {
        perror("epoll_create1");
        close(listen_fd);
        return 1;
    }

    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.events = EPOLLIN;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &ev) != 0) {
        perror("epoll_ctl(ADD listen_fd)");
        close(epoll_fd);
        close(listen_fd);
        return 1;
    }

    printf("[server] listening on 0.0.0.0:%d\n", port);
    event_loop(epoll_fd, listen_fd);

    close(epoll_fd);
    close(listen_fd);
    printf("[server] stopped, total clients served: %d\n", g_total_clients);
    return 0;
}
