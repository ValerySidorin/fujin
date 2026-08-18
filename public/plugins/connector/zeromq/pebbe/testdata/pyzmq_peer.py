#!/usr/bin/env python3
import base64
import json
import sys
import time

import zmq


def decode_messages(raw):
    return [[base64.b64decode(frame) for frame in message] for message in json.loads(raw)]


def encode_messages(messages):
    return json.dumps([[base64.b64encode(frame).decode("ascii") for frame in message] for message in messages])


def socket_type(name):
    return {
        "pub": zmq.PUB,
        "sub": zmq.SUB,
        "push": zmq.PUSH,
        "pull": zmq.PULL,
    }[name]


def main():
    if len(sys.argv) < 6:
        raise SystemExit(
            "usage: pyzmq_peer.py send|receive pub|sub|push|pull endpoint count "
            "bind|connect [subscription] [delay_ms]"
        )
    operation, kind, endpoint, count_text, ownership = sys.argv[1:6]
    subscription = sys.argv[6] if len(sys.argv) > 6 else ""
    delay_ms = int(sys.argv[7]) if len(sys.argv) > 7 else 250
    count = int(count_text)

    context = zmq.Context()
    socket = context.socket(socket_type(kind))
    socket.setsockopt(zmq.LINGER, 0)
    socket.setsockopt(zmq.RCVTIMEO, 5000)
    socket.setsockopt(zmq.SNDTIMEO, 5000)
    if kind == "sub":
        socket.setsockopt(zmq.SUBSCRIBE, subscription.encode())
    if ownership == "bind":
        socket.bind(endpoint)
    elif ownership == "connect":
        socket.connect(endpoint)
    else:
        raise SystemExit(f"unsupported ownership {ownership}")
    time.sleep(delay_ms / 1000)
    print("READY", flush=True)

    if operation == "send":
        messages = decode_messages(sys.stdin.read())
        if len(messages) != count:
            raise SystemExit(f"expected {count} messages, got {len(messages)}")
        for message in messages:
            socket.send_multipart(message)
        time.sleep(delay_ms / 1000)
    elif operation == "receive":
        messages = [socket.recv_multipart() for _ in range(count)]
        print(encode_messages(messages), flush=True)
    else:
        raise SystemExit(f"unsupported operation {operation}")

    socket.close()
    context.term()


if __name__ == "__main__":
    main()
