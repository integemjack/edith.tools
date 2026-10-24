# EDITH 工具（edith.tools）

[EDITH](https://github.com/integemjack/edith) 大脑只有一个接口：本机套接字。所有能力都从它连进去、都是一级能力，讲
[能力协议](https://github.com/integemjack/edith/blob/main/docs/CAPABILITY-PROTOCOL.md)。这个仓库放大家共用的那些能力程序：

| 工具 | 做什么 | 说明 |
|---|---|---|
| `edith-websocket` | 远程能力的中转：设备、网页、手机连它的 WebSocket，每个客户端经大脑的套接字成为一个能力 | [docs/WEBSOCKET.md](docs/WEBSOCKET.md) |
| `edith-connect` | 共享设备：麦克风、摄像头、串口这类独占设备整台机器一个设备服务，几个大脑共用 | [docs/DEVICES.md](docs/DEVICES.md) |
| `edith-friday` | Friday 客户端的网关：讲 Friday 的远程协议，客户端经 SSH 连上来就用这台的 EDITH（按工作目录起大脑） | [docs/FRIDAY.md](docs/FRIDAY.md) |

它们只讲能力协议、不用大脑的代码（`edith-friday` 另外读大脑的会话存档）。MCP 服务不用这里的工具：大脑直接读 `.mcp.json` 连（能力协议 §9）。

**每个工具相互隔离**：各是一个独立的 Rust 工程（`tools/<目录>/`：自己的 `Cargo.toml`、`Cargo.lock`、依赖、版本），各有各的工作流，
只在自己改了时编、测、发版，一个工具编不过不耽误别的。共用的只有测试扮大脑用的 `support/`（只是开发依赖，不进程序）。

| 目录 | 程序 | 发版标签 |
|---|---|---|
| `tools/websocket/` | `edith-websocket` | `edith-websocket-vX.Y.N` |
| `tools/connect/` | `edith-connect` | `edith-connect-vX.Y.N` |
| `tools/friday/` | `edith-friday` | `edith-friday-vX.Y.N` |

## 下载

大脑要用时自己下：`edith tools install edith-connect`，或者能力配置里写了 `edith-connect` 而本机没有时自动下（装在 `~/.edith/tools/`）。
每个工具各自的 Release（`<工具>-vX.Y.N`，只留最新一个）上是它每个平台一个包和这一版的清单 `<工具>.json`；
固定的 `index` Release 上的 [`index.json`](https://github.com/integemjack/edith.tools/releases/download/index/index.json)
把各个工具最新的清单合在一起（哪个工具发了版就重新合一次，`releases/latest/download/index.json` 也指向它），大脑读它：

```json
{"version": "2026-10-24T15:00:00Z", "repository": "integemjack/edith.tools", "tools": {
  "edith-connect": {"description": "…", "version": "v0.2.0", "assets": {
    "macos-arm64": {"file": "edith-connect-v0.2.0-macos-arm64.tar.gz", "url": "https://…", "sha256": "…", "size": 1234567}, "…": {}}}}}
```

顶上的 `version` 是这份索引生成的时间，每个工具的版本在它自己的 `version` 里。

平台：macOS（arm64、x64）、Windows（x64、x86、arm64）、Linux（x64、x86、arm64、armv7、armv6、riscv64、loong64、ppc64le、s390x；
musl 静态程序，不挑发行版）。每个包里只有那一个程序。

## 大脑怎么起它们

能力配置（`~/.edith/capabilities.json`、项目的 `.edith/capabilities.json` 或本地的 `.edith/capabilities.local.json`）里写命令，
大脑起它时给三个环境变量，它自己连进大脑的套接字：

| 环境变量 | 内容 |
|---|---|
| `EDITH_ACCESS` | 大脑套接字的地址（`127.0.0.1:端口`） |
| `EDITH_ACCESS_TOKEN` | 这个能力专用的令牌：连上后第一行发 `{"jsonrpc": "2.0", "method": "auth", "params": {"token": "…"}}`；替别人开连接时加 `"as": "<名字>"` |
| `EDITH_CAPABILITY` | 在大脑那里叫什么（令牌定的，回 `initialize` 时报别的也不管） |

令牌定了每条连接能做什么（能力配置的 `permissions`，能力协议 §8.1）；替别人开的（`as`）叫 `<能力名>-<as>`，权限按 `permissions.relay`。

## 开发

```bash
git clone https://github.com/integemjack/edith.tools.git
cd edith.tools/tools/friday       # 每个工具单独编、单独测
cargo test                        # 要 python3；测试自己扮大脑（support/）
cargo build --release
```

加一个工具：`tools/<目录>/` 放一个独立工程（`Cargo.toml` 里写 `[workspace]`，开发依赖 `edith-tools-support = { path = "../../support" }`），
再照 `.github/workflows/friday.yml` 加一个调 `tool.yml` 的工作流。

每次推送，改了的工具（`tools/<目录>/`、共用的 `support/`、`rust-toolchain.toml`、`scripts/`、流水线）在 GitHub Actions 上编全平台全架构、跑测试；
推到 `main`、而且这个工具自上一版以来改过（只改了 `support/` 的只测不发），发它的一版：版本 `v<它 Cargo.toml 版本的前两位>.<这个工具发过几次>`，
然后重新合 `index.json`（`scripts/index.sh`，手动重合：工作流 `index`）。手动发：在 Actions 里跑那个工具的工作流、勾「没改也发一版」。

## 许可证

MIT 或 Apache-2.0 任选（[LICENSE-MIT](LICENSE-MIT)、[LICENSE-APACHE](LICENSE-APACHE)）。
