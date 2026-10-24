# edith-friday：让 Friday 客户端连 EDITH

[Friday](https://github.com/integemjack/Friday) 的客户端（Mac 版、Windows / Linux 版）连远程机器时经 SSH 跑那台的
`friday-server --attach`，在标准输入 / 输出上讲 Friday 的远程协议（Friday 仓库的 `qt/docs/REMOTE.md`）。
`edith-friday` 讲的就是这套协议，对内连 EDITH 大脑的套接字：**客户端不用改**，装上它，那台机器的服务端就是 EDITH。

```
Friday 客户端 ──SSH──▶ edith-friday --attach（一条连接一个）──套接字──▶ 各个工作目录的 EDITH 大脑（edith run --idle）
                         会话列表直接读 ~/.edith/projects/*/sessions/
```

- **一条连接一个网关**：客户端断了网关就退出；要一直干的活在大脑里，断了照常跑。几个客户端各开一个网关，看到的是同一份。
- **按工作目录起大脑**：在某个目录新建会话、发话时，按 EDITH 的规则算出工作目录（往上找 `.edith/`、到 git 仓库的根），
  `~/.edith/instances/` 里登记着就连上，没有就在那里起 `edith run --idle`（脱离这次 SSH）。看会话不用起大脑。
- **翻译**：EDITH 的历史（OpenAI 格式）换成 Friday 的会话和消息：用户的一条；一轮里模型说的话、思考、调的工具合成一条助手消息
  （工具卡片带结果、成败、没批准）；交付检查追的提醒不当用户消息，记成那条回复的提示；补充进来的话去掉给模型标的来源。
  消息和卡片的 id 由会话 id 和位置算出来，正在干的那一轮按大脑推来的事件拼，做完按存档重发整段，id 前后一样。
- **批准**：网关连大脑时说自己能批准、是全权的连接。大脑要批准的事变成 Friday 的「待确认」（卡片上的「允许执行 / 拒绝」、
  侧边栏的手、`attention`），客户端点了回给大脑。
- **用户打的字**：交给大脑时说是 `kind: "user"`，模型看到的同终端里打的一样，不标「由能力传来」。

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
| `EDITH_FRIDAY_TRACE=1` | 每个请求、每个事件记一行到标准错误（排查用；出错的请求总是记） |

网关自己记的（EDITH 没有的）在 `~/.edith/friday.json`：置顶、草稿、最近的目录、每段会话在界面上选的档位。

## 第一阶段能做的

| 能做 | 方法 |
|---|---|
| 连上、看会话列表、打开会话 | `hello`、`conversation.subscribe` / `unsubscribe`、`message.get`、`conversation.search` |
| 新建、删、改名、置顶、草稿 | `conversation.create`（按目录起大脑、用客户端给的 id 建）、`remove`、`rename`、`setPinned`、`setDraft` |
| 发话（忙时补充进这一轮，或排到下一轮）、流式看回复 | `runner.submit`（`whenBusy: "queue"` 排队）、`message` / `conversation` / `runtime` / `runner` 事件 |
| 批准 | `runner.resolveApproval` |
| 停下 | `runner.stop`、`runner.stopAll` |
| 最近的目录 | `directories.get` / `push` / `remove` |

还不行的（回 `-32601` 没有，或错误 `4` 说明原因；`server.features` 是空的，客户端就不显示那些功能）：

- 分叉、重试、切分支；在 Friday 里管子智能体（子智能体照样派，卡片在消息里，Agent 树是空的）。
- 终端、浏览器（`available: false`）、本地模型、设置页、助手（助手页那一行显示「版本太旧」）。
- 在这里改模型和密钥：模型只有一个「EDITH · <模型>」，读 `~/.edith/config.json`；界面上选的权限模式、思考档位只记下来显示，
  EDITH 按自己的审批规则（[EDITH.md](https://github.com/integemjack/edith/blob/main/docs/EDITH.md#信任与审批)）。
- 附件不上传：路径原样附在话后面（要那台机器上有）。
- 要批准的事问最后连上的那个客户端；**没有客户端连着时不等**（同 EDITH 别的入口：没人能批就不做），
  要无人值守就在 `~/.edith/config.json` 的 `permissions.allow` 里放开，或用 `edith --auto`。

## 测试

`cargo test --test friday`：测试一头扮 Friday 客户端，一头扮大脑（登记一个假的大脑），走一遍存档翻译、发话、流式、批准、停下、改名、删。
