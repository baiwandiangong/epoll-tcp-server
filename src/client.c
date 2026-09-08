/* ============================================================================
 * tcp_client.c -- 简单的 TCP 回显测试客户端（与 epoll_tcp_server 配套）
 *
 * 功能：演示客户端侧最基本的 socket 编程：socket -> connect -> send -> recv
 *
 * 用法：
 *   ./tcp_client 127.0.0.1 9000            交互模式：输入一行，回显一行
 *   ./tcp_client 127.0.0.1 9000 10000      压测模式：连续发 10000 条并校验回显，
 *                                           最后输出每秒往返次数（round-trips/s）
 *
 * 编译： gcc -O2 -Wall -Wextra -o tcp_client src/client.c
 * ========================================================================== */

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define BUF_SIZE 4096

/* 尽量把 len 字节全部发出去（阻塞式，处理部分写） */
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

/* 按行读取：一直读到 '\n'（含）为止，去掉行尾的 \r \n */
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

static int connect_server(const char *ip, int port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        exit(1);
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, ip, &addr.sin_addr) <= 0) {
        fprintf(stderr, "invalid ip: %s\n", ip);
        exit(1);
    }

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        perror("connect");
        close(fd);
        exit(1);
    }
    return fd;
}

/* 交互模式：把 stdin 的每一行发给服务器并打印回显 */
static void interactive(int fd)
{
    char line[BUF_SIZE];
    char reply[BUF_SIZE];

    read_line(fd, line, sizeof(line));   /* 欢迎语 */
    printf("%s\n", line);

    while (1) {
        printf(">>> ");
        fflush(stdout);
        if (fgets(line, sizeof(line), stdin) == NULL)
            break;

        send_all(fd, line, strlen(line));        /* 原样发一整行 */
        read_line(fd, reply, sizeof(reply));
        printf("%s\n", reply);
    }
}

/* 压测模式：单连接连续发 count 条消息并校验回显 */
static void benchmark(int fd, long count)
{
    char msg[BUF_SIZE];
    char reply[BUF_SIZE];
    long ok = 0;
    struct timespec t0, t1;

    read_line(fd, msg, sizeof(msg));   /* 先读掉欢迎语 */

    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (long i = 0; i < count; i++) {
        int n = snprintf(msg, sizeof(msg), "echo-%ld", i);
        send_all(fd, msg, (size_t)n);
        read_line(fd, reply, sizeof(reply));
        if (strcmp(msg, reply) == 0)
            ok++;
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);

    double secs = (double)(t1.tv_sec - t0.tv_sec) +
                  (double)(t1.tv_nsec - t0.tv_nsec) / 1e9;
    printf("sent %ld, echoed correctly %ld, cost %.3fs\n", count, ok, secs);
    if (secs > 0.0)
        printf("round-trips per second: %.0f\n", (double)ok / secs);
}

int main(int argc, char *argv[])
{
    const char *ip;
    int port;
    int fd;

    if (argc < 3) {
        fprintf(stderr,
                "usage: %s <server-ip> <port> [count]\n"
                "  example: %s 127.0.0.1 9000\n"
                "  example: %s 127.0.0.1 9000 10000\n",
                argv[0], argv[0], argv[0]);
        return 1;
    }

    ip = argv[1];
    port = atoi(argv[2]);
    if (port <= 0 || port > 65535) {
        fprintf(stderr, "invalid port: %s\n", argv[2]);
        return 1;
    }

    fd = connect_server(ip, port);
    printf("[client] connected to %s:%d\n", ip, port);

    if (argc >= 4) {
        long count = atol(argv[3]);
        if (count <= 0)
            count = 1;
        benchmark(fd, count);
    } else {
        interactive(fd);
    }

    close(fd);
    return 0;
}
