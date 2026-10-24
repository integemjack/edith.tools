"""生成 index.json：index.py <标签> <仓库> <包所在目录>。大脑的 edith tools 读它：有哪些工具、每个平台从哪下、SHA-256、大小"""
import hashlib
import json
import os
import re
import sys

DESCRIPTIONS = {
    "edith-websocket": "远程能力的中转：设备、网页、手机连它的 WebSocket，每个客户端经大脑的套接字成为一个能力",
    "edith-connect": "共享设备：麦克风、摄像头、串口这类独占设备整台机器一个设备服务，几个大脑共用",
    "edith-friday": "Friday 客户端的网关：讲 Friday 的远程协议，客户端经 SSH 连上来就用这台的 EDITH（按工作目录起大脑）",
}

tag, repository, folder = sys.argv[1], sys.argv[2], sys.argv[3]
tools = {}
for file in sorted(os.listdir(folder)):
    match = re.fullmatch(r"(.+)-" + re.escape(tag) + r"-([a-z0-9]+-[a-z0-9]+)\.(tar\.gz|zip)", file)
    if not match:
        continue
    tool, platform = match.group(1), match.group(2)
    path = os.path.join(folder, file)
    with open(path, "rb") as f:
        digest = hashlib.sha256(f.read()).hexdigest()
    entry = tools.setdefault(tool, {"description": DESCRIPTIONS.get(tool, ""), "assets": {}})
    entry["assets"][platform] = {
        "file": file,
        "url": f"https://github.com/{repository}/releases/download/{tag}/{file}",
        "sha256": digest,
        "size": os.path.getsize(path),
    }
json.dump({"version": tag, "repository": repository, "tools": tools}, sys.stdout, ensure_ascii=False, indent=2)
print()
