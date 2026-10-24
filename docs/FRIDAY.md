# edith-friday：让 Friday 客户端连 EDITH

[Friday](https://github.com/integemjack/Friday) 的客户端（Mac 版、Windows / Linux 版）连远程机器时经 SSH 跑那台的
`friday-server --attach`，在标准输入 / 输出上讲 Friday 的远程协议（Friday 仓库的 `qt/docs/REMOTE.md`）。
`edith-friday` 讲的就是这套协议，对内连 EDITH 大脑的套接字：**客户端不用改**，装上它，那台机器的服务端就是 EDITH。

```
Friday 客户端 ──SSH──▶ edith-friday --attach（一条连接一个）──套接字──▶ 各个工作目录的 EDITH 大脑（edith run --idle --linger）
                         会话列表直接读 ~/.edith/projects/*/sessions/
```

- **一条连接一个网关**：客户端断了网关就退出；要一直干的活在大脑里，断了照常跑。几个客户端各开一个网关，看到的是同一份。
- **连上来接着看**：hello 时连上这台全部在跑的大脑（之后每 5 秒看一次有没有新起的），问它们在干什么——客户端关了又打开、
  换一台设备，正在干的那一轮照样显示、接着流；在等的批准也会重新问到这个客户端（一直开着的大脑没人能批时等能批准的连上来，最多 10 分钟）。
- **按工作目录起大脑**：新建的会话先只记在网关这（空白会话），第一次发话时才按 EDITH 的规则算出工作目录（往上找 `.edith/`、
  到 git 仓库的根），`~/.edith/instances/` 里登记着就连上，没有就在那里起 `edith run --idle --linger 1800`
  （脱离这次 SSH；没有连接、闲半小时自己退，下次用到再起）。看会话不用起大脑。
- **翻译**：EDITH 的历史（OpenAI 格式）换成 Friday 的会话和消息：用户的一条；一轮里模型说的话、思考、调的工具合成一条助手消息
  （工具卡片带结果、成败、没批准）；交付检查追的提醒不当用户消息，记成那条回复的提示；干活时补充的话在这一轮里另起一段
  （一条用户消息 + 接着的回复），显示原话、不带给模型标的来源。消息和卡片的 id 由会话 id 和位置算出来，正在干的那一轮按大脑推来的事件拼，
  做完按存档重发整段，id 前后一样（同一条消息里同一个调用 id 用了几次也分得开）。
- **批准**：网关连大脑时说自己能批准、是全权的连接。大脑要批准的事变成 Friday 的「待确认」（卡片上的「允许执行 / 拒绝」、
  侧边栏的手、`attention`），客户端点了回给大脑。卡片是连上之前出的，就按批准请求补一张。
- **用户打的字**：交给大脑时说是 `kind: "user"`，模型看到的同终端里打的一样，不标「由能力传来」。
- **子智能体**：EDITH 推的 `agent` / `agent.event` 拼成 Friday 的样子（同 Friday 的 SubAgentRunner）：`delegate_task` 那张卡片就是那个子智能体
  （名字、运行记录、状态、交回的结果），并联 / 工作流的卡片里每个子智能体一张子卡片，往下派的照样嵌进去；在跑的进 Agent 树（`team` 事件，
  id 是它的卡片 id，上级是派它的那张卡片、主智能体派的是会话）。做完的从 EDITH 存的运行记录（`agents/<会话>/<id>.json`）读回来。
  子智能体交回给主智能体的那段话标 `fromAgent`（Friday 里不显示），用户干活时补充的标 `interjected`（「工作中补充」）。
- **会话改动**：文件卡片的「本次会话改动」、改动视图、撤销转给 EDITH 的 `changes.*`（基线同 Friday 的 SessionChanges：写之前记原文，
  命令改的到 git 里找原文）。删会话交给 EDITH 的 `sessions.remove`，基线、子智能体的运行记录一起删。

## 装上

在要被连的那台机器上（没装 Friday 桌面版的；装了的客户端会先用桌面版自带的服务端）：

```bash
mkdir -p ~/.friday-server/bin
cp edith-friday ~/.friday-server/bin/friday-server        # Windows：%USERPROFILE%\.friday-server\bin\friday-server.exe
cp edith ~/.friday-server/bin/                            # 或者 edith 在 PATH 里 / EDITH_BIN 指过去
# 配好模型：~/.edith/config.json 写 {"baseURL": "https://…/v1", "apiKey": "…", "model": "…"}
```

然后在 Friday 里照常添加这台远程机器（SSH）。网关报的服务端版本是 `0.0.0`，客户端不会想把它「升级」成 Friday 自己的服务端。
只想临时试：客户端那边设 `FRIDAY_REMOTE_SERVER=<那台机器上 edith-friday 的路径>`（路径里不能有空格），客户端就只跑它、不自动装。

环境变量：

| 变量 | 说明 |
|---|---|
| `EDITH_HOME` | EDITH 的数据目录（默认 `~/.edith`）：会话、登记、配置都在这 |
| `EDITH_BIN` | 起大脑用哪个 `edith`（默认：同目录里的 → PATH 里的） |
| `EDITH_FRIDAY_DIRECTORY` | 新会话没给工作目录时用哪（默认家目录） |
| `EDITH_FRIDAY_LINGER` | 起的大脑没有连接、闲多少秒自己退（默认 1800；`0` 一直开着；旧版不认 `--linger` 的 EDITH 自动退回一直开着） |
| `EDITH_FRIDAY_TRACE=1` | 每个请求、每个事件记一行到标准错误（排查用；出错的请求总是记） |

网关自己记的（EDITH 没有的）在 `~/.edith/friday.json`：置顶、草稿、最近的目录、每段会话在界面上选的档位、还没发过话的空白会话、
最近一轮的用量（上下文进度环）。几个网关都写它：每次改先读回最新的再改。附件传上来放在 `~/.edith/uploads/<uploadID>/`。

## 能做的

| 能做 | 方法 |
|---|---|
| 连上、看会话列表、打开会话 | `hello`、`conversation.subscribe` / `unsubscribe`、`message.get`、`conversation.search` |
| 新建（空白会话）、删、改名、置顶、草稿 | `conversation.create`、`remove`、`rename`、`setPinned`、`setDraft` |
| 选工作目录（没发过话的）、列目录、新建文件夹、最近的目录 | `runner.setWorkingDirectory`、`fs.list`、`fs.mkdir`（特性 `mkdir`）、`directories.*` |
| 发话：闲着开始一轮，忙时补充进这一轮，或排到下一轮（显示在队列里） | `runner.submit`（`whenBusy: "queue"` 排队，特性 `submitFrom`） |
| 附件、文件卡片预览 | `file.upload`（一块块传，回这台上的路径）、`fs.read` |
| 流式看回复、速度和用量、上下文进度环 | `message` / `conversation` / `runtime` / `runner` 事件 |
| 批准（主智能体的、子智能体的） | `runner.resolveApproval` |
| 停下 | `runner.stop`（排着的也不干了）、`runner.stopAll` |
| 子智能体：卡片里看它的运行记录、Agent 树、停掉、追加指示 | `team` 事件、`runner.backgroundAgents`、`agent.stop`、`agent.send` |
| 文件卡片的「本次会话改动」、改动视图、撤销 | `fs.changes`、`fs.diff`、`fs.revert`（交给 EDITH 的 `changes.*`） |

还不行的（回 `-32601` 没有，或错误 `4` 说明原因；`server.features` 只报上面那两个，客户端就不显示别的功能）：

- 分叉、重试、切分支；把排着的话拿掉（主智能体的、子智能体的）。
- 终端、浏览器（`available: false`）、本地模型、设置页、助手（助手页那一行显示「版本太旧」）。
- 在这里改模型和密钥：模型只有一个「EDITH · <模型>」，读 `~/.edith/config.json`；界面上选的权限模式、思考档位只记下来显示，
  EDITH 按自己的审批规则（[EDITH.md](https://github.com/integemjack/edith/blob/main/docs/EDITH.md#信任与审批)）。
- 图片附件模型看不到（只把路径告诉它）。
- 要批准的事问最后连上的那个客户端；没有客户端连着时大脑等能批准的连上来（最多 10 分钟，过了就不做），
  要无人值守就在 `~/.edith/config.json` 的 `permissions.allow` 里放开，或用 `edith --auto`。

## 测试

```bash
cd tools/friday && cargo test
```

一头扮 Friday 客户端，一头扮大脑（登记一个假的大脑）：存档翻译、连上来接着看在干的那一轮、发话、流式、批准、干活时补充另起一段、
排队、停下、改名、空白会话第一次发话才在 EDITH 里建、选目录、列目录、新建文件夹、附件上传、删；子智能体的卡片和运行记录、Agent 树、
子智能体要批准的、停掉 / 追加指示、交回的汇报不显示；会话改动、改动视图、撤销。
