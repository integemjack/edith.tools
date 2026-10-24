# EDITH 工具（edith.tools）

[EDITH](https://github.com/integemjack/edith) 大脑只有一个接口：本机套接字。所有能力都从它连进去、都是一级能力，讲
[能力协议](https://github.com/integemjack/edith/blob/main/docs/CAPABILITY-PROTOCOL.md)。这个仓库放大家共用的那些能力程序：

| 工具 | 做什么 | 说明 |
|---|---|---|
| `edith-websocket` | 远程能力的中转：设备、网页、手机连它的 WebSocket，每个客户端经大脑的套接字成为一个能力 | [docs/WEBSOCKET.md](docs/WEBSOCKET.md) |
| `edith-connect` | 共享设备：麦克风、摄像头、串口这类独占设备整台机器一个设备服务，几个大脑共用 | [docs/DEVICES.md](docs/DEVICES.md) |
| `edith-mcp` | MCP 服务的桥：把一个 MCP 服务的工具当成能力的动作接进大脑 | [docs/MCP.md](docs/MCP.md) |

它们只讲能力协议、不用大脑的代码（`edith-mcp` 用 [friday.core](https://github.com/integemjack/friday.core) 的 MCP 客户端，子模块 `core/`）。

## 下载

大脑要用时自己下：`edith tools install edith-connect`，或者能力配置里写了 `edith-connect` 而本机没有时自动下（装在 `~/.edith/tools/`）。
它读 Release 里的 [`index.json`](https://github.com/integemjack/edith.tools/releases/latest/download/index.json)：

```json
{"version": "v0.1.3", "repository": "integemjack/edith.tools", "tools": {
  "edith-connect": {"description": "…", "assets": {
    "macos-arm64": {"file": "edith-connect-v0.1.3-macos-arm64.tar.gz", "url": "https://…", "sha256": "…", "size": 1234567}, "…": {}}}}}
```

平台：macOS（arm64、x64）、Windows（x64、x86、arm64）、Linux（x64、x86、arm64、armv7、armv6、riscv64、loong64、ppc64le、s390x；
musl 静态程序，不挑发行版）。每个包里只有那一个程序。

## 大脑怎么起它们

能力配置（`~/.edith/capabilities.json` 或目录的 `.edith/capabilities.json`）里写命令，大脑起它时给三个环境变量，它自己连进大脑的套接字：

| 环境变量 | 内容 |
|---|---|
| `EDITH_ACCESS` | 大脑套接字的地址（`127.0.0.1:端口`） |
| `EDITH_ACCESS_TOKEN` | 令牌：连上后第一行发 `{"jsonrpc": "2.0", "method": "auth", "params": {"token": "…"}}` |
| `EDITH_CAPABILITY` | 在大脑那里叫什么（回 `initialize` 时的 `name`） |

## 开发

```bash
git clone --recursive https://github.com/integemjack/edith.tools.git
cargo test            # 要 python3；测试自己扮大脑（tests/common）
cargo build --release
```

每次推送 GitHub Actions 编全平台全架构、跑测试；推到 `main` 的发一个 Release（`v<版本前两位>.<序号>`，只留最新一个）。

## 许可证

MIT 或 Apache-2.0 任选（[LICENSE-MIT](LICENSE-MIT)、[LICENSE-APACHE](LICENSE-APACHE)）。
