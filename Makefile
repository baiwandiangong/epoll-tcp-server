# epoll_tcp_server + tcp_client + stress_client
# 用法： make         编译服务端与两个客户端
#       make run     以 9000 端口运行服务端
#       make clean   清理编译产物

CC      ?= gcc
CFLAGS  ?= -O2 -Wall -Wextra -std=gnu11

SERVER   = epoll_tcp_server
CLIENT   = tcp_client
STRESS   = stress_client

all: $(SERVER) $(CLIENT) $(STRESS)

$(SERVER): src/main.c
	$(CC) $(CFLAGS) -o $@ src/main.c

$(CLIENT): src/client.c
	$(CC) $(CFLAGS) -o $@ src/client.c

$(STRESS): src/stress.c
	$(CC) $(CFLAGS) -o $@ src/stress.c

run: $(SERVER)
	./$(SERVER) 9000

clean:
	rm -f $(SERVER) $(CLIENT) $(STRESS)

.PHONY: all run clean
