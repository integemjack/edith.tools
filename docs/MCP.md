# edith-mcp：MCP 服务的桥

[MCP](https://modelcontextprotocol.io) 服务只会被调用、不会主动说话。`edith-mcp` 连进大脑的套接字当一个一级能力，
把一个 MCP 服务的工具当成自己的**动作**报上去（没有感知）；大脑调动作，它就调 MCP 的工具，结果原样交回。

```bash
edith capabilities add github edith-mcp npx -y @modelcontextprotocol/server-github          # 本机的 MCP 服务（stdio）
edith capabilities add search edith-mcp --url https://example.com/mcp --header "Authorization: Bearer …"   # HTTP 的（Streamable HTTP）
```

- 模型那边的名字是 `<能力名>__<工具名>`；工具名里不是字母、数字、`-`、`_` 的字符换成 `_`（比如 `echo.text` → `echo_text`），调的时候换回去。
- MCP 说出错了（`isError`）：动作回 `ok: false`；连不上、超时：动作回错误。
- MCP 服务退出了、或者和大脑的连接断了，它就退出；大脑过一会儿重起它（重新接上 MCP 服务）。
- MCP 客户端是 friday.core 的 `mcp_client`（协议版本 2025-06-18；stdio 的服务在新会话里起，Windows 上不弹窗、关就一起结束）。
