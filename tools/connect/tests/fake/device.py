"""测试用的设备程序：一个普通的、只管一个连接的能力，由 edith-connect 的设备服务起、几个大脑共用。只用标准库。
动作：record（holds：开始录，占着设备）、stop（releases：停）、beep（exclusive：要 1.5 秒）、who（说是哪个大脑要的）、
announce（回话后过 DEVICE_ANNOUNCE_DELAY 秒主动推一句「只聊天」——测「当前大脑」）。收到的每一条记进 DEVICE_LOG。"""
import json
import os
import sys
import threading
import time

sys.stdin.reconfigure(encoding="utf-8")
sys.stdout.reconfigure(encoding="utf-8")
LOG = os.environ.get("DEVICE_LOG")
lock = threading.Lock()


def send(message):
    with lock:
        sys.stdout.write(json.dumps(message, ensure_ascii=False) + "\n")
        sys.stdout.flush()


def reply(message, text):
    send({"jsonrpc": "2.0", "id": message["id"], "result": {"text": text}})


def announce():
    time.sleep(float(os.environ.get("DEVICE_ANNOUNCE_DELAY", "2")))
    send({"jsonrpc": "2.0", "method": "sense", "params": {"text": "只聊天", "kind": "speech", "from": "麦克风"}})


def action(name, description, **flags):
    return dict({"name": name, "description": description, "parameters": {"type": "object", "properties": {}}}, **flags)


for line in sys.stdin:
    message = json.loads(line)
    if LOG:
        with open(LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(message, ensure_ascii=False) + "\n")
    method = message.get("method")
    if method == "initialize":
        send({"jsonrpc": "2.0", "id": message["id"], "result": {
            "protocolVersion": "1",
            "capability": {"title": "测试麦克风", "version": "1", "description": "假的麦克风"},
            "senses": [{"kind": "speech", "description": "听到的话"}],
            "actions": [
                action("record", "开始录音（占着麦克风）", holds=True),
                action("stop", "停止录音", releases=True),
                action("beep", "哔一声（要 1.5 秒）", exclusive=True),
                action("who", "说是谁要的"),
                action("announce", "过一会儿说一句话"),
            ],
            "receives": [],
        }})
    elif method == "act":
        params = message["params"]
        name = params["action"]
        if name == "record":
            reply(message, "开始录了")
        elif name == "stop":
            reply(message, "停了")
        elif name == "beep":
            time.sleep(1.5)
            reply(message, "哔")
        elif name == "who":
            reply(message, "是 " + params.get("brain", {}).get("workspace", "?") + " 要的，call=" + params["call"])
        elif name == "announce":
            reply(message, "一会儿说")
            threading.Thread(target=announce, daemon=True).start()
    elif method == "shutdown":
        break
