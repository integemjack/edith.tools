# edith-websocket：远程能力的中转

EDITH 大脑只有本机套接字一个接口。别的机器上的设备、网页、手机要当大脑的能力，就连这个工具的 WebSocket：
它替**每个客户端另开一条连接到大脑的套接字**，之后 WebSocket 文本帧和 JSON 行原样双向转。客户端自己讲完整的
[能力协议](https://github.com/integemjack/edith/blob/main/docs/CAPABILITY-PROTOCOL.md)，在大脑那里就是一个一级能力：
有自己的名字、感知、动作，能主动推内容、收事件、被调动作。

## 接上

```bash
edith capabilities add ws edith-websocket                                   # 只听本机，端口自动挑
edith capabilities add ws edith-websocket --listen 0.0.0.0:7789 --token 一串随机的令牌
edith run
edith instances                                                             # 看它开在哪个地址
```

- `--listen 地址:端口`：默认 `127.0.0.1:0`（系统挑空端口，几个目录的大脑各接一个也不会撞）。
- `--token 令牌`（或环境变量 `EDITH_WEBSOCKET_TOKEN`）：不是只听本机（127.0.0.1 / ::1 / localhost）时必须给，不给不启动。
- 它自己也用一条连接连进大脑（名字是能力配置里的名字），报对外的入口（`endpoints`），登记在 `~/.edith/instances/`。
- 自己跑（不由大脑起）：`edith-websocket --brain <目录>`，从 `~/.edith/instances/` 找那个目录正在跑的大脑。

## 客户端

```
ws://<地址>:<端口>/?token=<令牌>&name=<客户端名字>
```

- 令牌放查询参数 `token`，或请求头 `Authorization: Bearer <令牌>`；不对回 401。
- 每个文本帧是一条 JSON-RPC 2.0 消息，内容就是能力协议：连上后大脑先发 `initialize`，客户端回话报自己的感知、动作、订阅；
  回话里没写 `name` 的，中转补上 `?name=` 的（不给 `name` 就是 `remote-<序号>`）。
- 客户端断开，它在大脑那里的连接也断开（能力摘掉）；大脑那边断了，WebSocket 也关掉。

## 例子（浏览器：一个能交事、看过程的网页）

```js
const ws = new WebSocket("ws://127.0.0.1:7789/?name=browser");   // 地址见 edith instances
const send = (message) => ws.send(JSON.stringify({ jsonrpc: "2.0", ...message }));
ws.onmessage = ({ data }) => {
  const message = JSON.parse(data);
  if (message.method === "initialize")   // 大脑问：你是谁、能做什么
    send({ id: message.id, result: { protocolVersion: "1", capability: { title: "网页" }, senses: [{ kind: "message" }], actions: [], receives: ["text", "done"] } });
  if (message.method === "initialized") send({ id: 1, method: "ask", params: { text: "列一下桌面上有什么" } });
  if (message.method === "event" && message.params.type === "text") console.log(message.params.data.text);
  if (message.id === 1) console.log("回答：", message.result.answer);
};
```
