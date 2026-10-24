"""索引（大脑的 edith tools 读它：有哪些工具、各自的版本、每个平台从哪下、SHA-256、大小）。每个工具各自发版，所以分两步：

- index.py fragment <工具> <版本> <仓库> <包所在目录> <Cargo.toml>：这一版这个工具的清单片段（<工具>.json，跟包一起挂在它的 Release 上）
- index.py merge <仓库> <片段…>：各个工具最新的片段合成 index.json（挂在固定的 index Release 上，scripts/index.sh 调）
"""
import datetime
import hashlib
import json
import os
import re
import sys


def description(cargo_toml):
    with open(cargo_toml, encoding="utf-8") as f:
        match = re.search(r'^description = "(.*)"$', f.read(), re.M)
    return match.group(1) if match else ""


def fragment(tool, version, repository, folder, cargo_toml):
    tag = f"{tool}-{version}"
    assets = {}
    for file in sorted(os.listdir(folder)):
        match = re.fullmatch(re.escape(tag) + r"-([a-z0-9]+-[a-z0-9]+)\.(tar\.gz|zip)", file)
        if not match:
            continue
        path = os.path.join(folder, file)
        with open(path, "rb") as f:
            digest = hashlib.sha256(f.read()).hexdigest()
        assets[match.group(1)] = {
            "file": file,
            "url": f"https://github.com/{repository}/releases/download/{tag}/{file}",
            "sha256": digest,
            "size": os.path.getsize(path),
        }
    return {"name": tool, "version": version, "description": description(cargo_toml), "assets": assets}


def merge(repository, paths):
    tools = {}
    for path in paths:
        with open(path, encoding="utf-8") as f:
            part = json.load(f)
        tools[part["name"]] = {"description": part["description"], "version": part["version"], "assets": part["assets"]}
    now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    # version：这份索引什么时候生成的（每个工具自己的版本在 tools.<工具>.version；旧的大脑只认顶上这个）
    return {"version": now, "repository": repository, "tools": dict(sorted(tools.items()))}


if sys.argv[1] == "fragment":
    result = fragment(*sys.argv[2:7])
elif sys.argv[1] == "merge":
    result = merge(sys.argv[2], sys.argv[3:])
else:
    sys.exit(f"不认识 {sys.argv[1]}（fragment / merge）")
json.dump(result, sys.stdout, ensure_ascii=False, indent=2)
print()
