/* ============================================================================
 * stress_client.c -- C 语言多连接并发压测客户端（配合 epoll_tcp_server 使用）
 *
 * 原理：
 *   父进程 fork() 出 N 个子进程，每个子进程等价于"一个客户端"：
 *     connect -> 读掉欢迎语 -> 循环发送 M 条消息 -> 逐条校验回显 -> close
 *   子进程只通过退出码(0成功/1失败)告诉父进程结果，由父进程统一统计打印，
 *   避免 200 个进程同时往终端打印把画面刷花。
 *
 * 用法：
 *   ./stress_client <ip> <port> [连接数] [每连接消息数]
 *   例：./stress_client 127.0.0.1 9000            # 默认 100 连接 x 5 条
 *   例：./stress_client 127.0.0.1 9000 200 5      # 200 并发，每个连接 5 条
 *
 * 编译： gcc -O2 -Wall -Wextra -o stress_client src/stress.c
 * ========================================================================== */

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define DEFAULT_CONNS  100
#define DEFAULT_MSGS   5
#define CONNECT_TRIES  50     /* 并发连入时可能瞬时失败，重试次数 */

/* 尽量把 len 字节全部发出去（处理部分写） */
static void send_all(int fd, const char *buf, size_t len)
{
    size_t sent = 0;

    while (sent < len) {
        ssize_t n = send(fd, buf + sent, len - sent, 0);
        if (n > 0) {
            sent += (size_t)n;
        } else if (n < 0 && errno == EINTR) {
            continue;
        } else {
            perror("send");
            exit(1);
        }
    }
}

/* 按行读取：读到 '\n'(含)为止，去掉行尾的 \r \n */
static int read_line(int fd, char *buf, size_t size)
{
    size_t i = 0;

    while (i + 1 < size) {
        ssize_t n = recv(fd, buf + i, 1, 0);
        if (n == 1) {
            if (buf[i] == '\n')
                break;
            i++;
        } else if (n < 0 && errno == EINTR) {
            continue;
        } else {
            break;   /* 对端关闭或出错 */
        }
    }
    buf[i] = '\0';
    while (i > 0 && (buf[i - 1] == '\r' || buf[i - 1] == '\n'))
        buf[--i] = '\0';
    return i;
}

/* 连接服务器；并发瞬间可能连不上，失败后短暂等待重试 */
static int connect_server(const char *ip, int port)
{
    for (int attempt = 0; attempt < CONNECT_TRIES; attempt++) {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            perror("socket");
            return -1;
        }

        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons((uint16_t)port);
        if (inet_pton(AF_INET, ip, &addr.sin_addr) <= 0) {
            fprintf(stderr, "invalid ip: %s\n", ip);
            close(fd);
            return -1;
        }

        if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) == 0)
            return fd;

        close(fd);
        usleep(20 * 1000);   /* 20ms 后重试 */
    }
    fprintf(stderr, "connect to %s:%d failed after %d tries\n",
            ip, port, CONNECT_TRIES);
    return -1;
}

/* 模拟一个客户端：连接 -> 连续发 M 条并逐条校验回显。成功返回 0，失败返回 1 */
static int run_one_client(const char *ip, int port, int conn_id, int msgs)
{
    char sendbuf[160];
    char expect[160];
    char reply[160];
    int fd = connect_server(ip, port);

    if (fd < 0)
        return 1;

    read_line(fd, reply, sizeof(reply));   /* 先读掉服务器欢迎语 */

    for (int i = 0; i < msgs; i++) {
        int n = snprintf(sendbuf, sizeof(sendbuf),
                         "conn-%04d-msg-%04d\n", conn_id, i);

        snprintf(expect, sizeof(expect), "conn-%04d-msg-%04d", conn_id, i);
        send_all(fd, sendbuf, (size_t)n);
        read_line(fd, reply, sizeof(reply));

        if (strcmp(expect, reply) != 0) {
            fprintf(stderr,
                    "[child %d] mismatch at msg %d: got '%s'\n",
                    conn_id, i, reply);
            close(fd);
            return 1;
        }
    }
    close(fd);
    return 0;
}

int main(int argc, char *argv[])
{
    const char *ip;
    int port;
    int conns = DEFAULT_CONNS;
    int msgs  = DEFAULT_MSGS;
    int started = 0;
    int passed = 0;
    struct timespec t0, t1;

    if (argc < 3) {
        fprintf(stderr,
                "usage: %s <server-ip> <port> [conns] [msgs-per-conn]\n"
                "  example: %s 127.0.0.1 9000 200 5\n",
                argv[0], argv[0]);
        return 1;
    }

    ip = argv[1];
    port = atoi(argv[2]);
    if (port <= 0 || port > 65535) {
        fprintf(stderr, "invalid port: %s\n", argv[2]);
        return 1;
    }
    if (argc >= 4)
        conns = atoi(argv[3]);
    if (argc >= 5)
        msgs = atoi(argv[4]);
    if (conns <= 0)
        conns = DEFAULT_CONNS;
    if (msgs <= 0)
        msgs = DEFAULT_MSGS;

    printf("[stress] spawning %d clients, each sends %d messages...\n",
           conns, msgs);
    fflush(stdout);   /* 先清空缓冲，避免子进程带着重复的缓冲内容 */

    clock_gettime(CLOCK_MONOTONIC, &t0);

    for (int i = 0; i < conns; i++) {
        pid_t pid = fork();

        if (pid == 0) {   /* 子进程：模拟第 i 个客户端 */
            int rc = run_one_client(ip, port, i, msgs);
            _exit(rc);
        } else if (pid > 0) {
            started++;
        } else {
            perror("fork");
            break;        /* fork 失败则少跑几个连接 */
        }
    }

    /* 父进程统一回收所有子进程并统计 */
    for (int i = 0; i < started; i++) {
        int status;
        waitpid(-1, &status, 0);
        if (WIFEXITED(status) && WEXITSTATUS(status) == 0)
            passed++;
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double secs = (double)(t1.tv_sec - t0.tv_sec) +
                  (double)(t1.tv_nsec - t0.tv_nsec) / 1e9;

    printf("[stress] done: %d/%d clients passed, %d messages verified per client\n",
           passed, started, msgs);
    printf("[stress] elapsed %.3fs, connect rate %.0f conns/s\n",
           secs, secs > 0.0 ? (double)started / secs : 0.0);

    return (passed == started && started > 0) ? 0 : 1;
}
