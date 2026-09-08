#!/usr/bin/env python3
"""测试客户端：连接 epoll 回显服务器并验证 echo 是否正确。

用法：
  python3 client.py                       # 进入交互模式，输入一行回显一行
  python3 client.py --stress 200          # 200 个并发连接，每个收发 5 条消息
"""
import socket
import sys
import threading
import time

HOST, PORT = "127.0.0.1", 9000


def recv_line(sock):
    """简单按行读取（仅测试用）。"""
    data = b""
    while not data.endswith(b"\n"):
        chunk = sock.recv(1)
        if not chunk:
            break
        data += chunk
    return data.decode(errors="replace")


def one_connection(cid, msg_count=5):
    sock = socket.create_connection((HOST, PORT), timeout=5)
    recv_line(sock)  # 欢迎语
    for i in range(msg_count):
        msg = f"msg-{cid}-{i}-中文测试\n"
        sock.sendall(msg.encode())
        reply = recv_line(sock)
        if reply != msg:
            print(f"[FAIL] conn {cid} reply mismatch")
            sock.close()
            return False
    sock.close()
    return True


def interactive():
    sock = socket.create_connection((HOST, PORT), timeout=5)
    print(recv_line(sock), end="")
    try:
        while True:
            line = input(">>> ")
            sock.sendall((line + "\n").encode())
            print(recv_line(sock), end="")
    except (EOFError, KeyboardInterrupt):
        sock.close()


def stress(n):
    ok = [0]
    lock = threading.Lock()

    def worker(cid):
        if one_connection(cid):
            with lock:
                ok[0] += 1

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    t0 = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print(f"stress done: {ok[0]}/{n} connections passed in {time.time()-t0:.2f}s")
    sys.exit(0 if ok[0] == n else 1)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--stress":
        stress(int(sys.argv[2]) if len(sys.argv) > 2 else 100)
    else:
        interactive()
