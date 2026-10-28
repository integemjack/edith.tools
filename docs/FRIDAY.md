# edith-friday：让 Friday 客户端连 EDITH

[Friday](https://github.com/integemjack/Friday) 的客户端（Mac 版、Windows / Linux 版）连远程机器时经 SSH 跑那台的
`friday-server --attach`，在标准输入 / 输出上讲 Friday 的远程协议（Friday 仓库的 `qt/docs/REMOTE.md`）。
`edith-friday` 讲的就是这套协议，对内连 EDITH 大脑的套接字：**客户端不用改**，装上它，那台机器的服务端就是 EDITH。

```
Friday 客户端 ──SSH──▶ edith-friday --attach（一条连接一个）──套接字──▶ 各个工作目录的 EDITH 大脑（edith run --idle --linger）
                         │ 会话列表直接读 ~/.edith/projects/*/sessions/              ▲
                         └──控制口──▶ edith-friday serve（常驻的那份，整台机器一个）──┘ 在每个大脑里是能力「friday」
```

**两份**：网关（`--attach`，一个客户端一条，客户端走了就退）只管翻译协议；Friday 专有、要一直在的放在**常驻的那份**
（`edith-friday serve`，像 edith-connect 的设备服务：整台机器一个）——终端、浏览器、智能体的浏览器工具、本地能力和模型、定时任务、钩子、插件、
能力提供方、实时语音、复盘和自我改进……
大脑只给它开两个口子：模型地址和钩子（EDITH 的能力协议 §6.6、§6.7）。

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
  命令改的到 git 里找原文）。删会话交给 EDITH 的 `sessions.remove`，基线、子智能体的运行记录、分出去的各支一起删。卡片上的「+N −M」按 `result` 事件的 `lines`。
- **重试、分支、分叉**：一轮出错（`done` 带 `error`）记在 `friday.json`（存档里不记），最后那条回复标错误、能重试——截到这一轮那句之前再发一遍，
  消息 id 不变。改一条重发：这段整个存成 EDITH 里藏起来的一支（`sessions.fork` 带 `hidden`，记在 `#branch` 名下），主干截到那条之前；
  分出去的各支按每条消息连同前面历史的指纹接进消息树（共同的开头不重复，同一处几支按分出来的先后排），切分支就是两支对换（`sessions.copy`）。
  从某条消息分叉成新会话用 `sessions.fork`（标题加「（fork）」）。
- **会话上选的**：权限模式、思考档位、模型、任务模式随每句话交给大脑（`sense` 的 `mode` / `thinking` / `engine` / `team`），附件里的图片单独列出（`images`，模型能看图时给它）。
  任务模式在 EDITH 上只有两种：**原来的**（`delegatesHeavy`，主智能体需要时自己派子智能体）和**工作流**（`ultra` → `team: "workflow"`：主智能体直接有
  `run_workflow`，先定完整流程交给子智能体并联 / 串联做，回来验收、打回）；客户端选「不派子智能体」（还有旧的「只派」）按原来的记。
  按会话记（`runner.setTeamMode`，特性 `teamModePerConversation`），同时记成新对话的默认。hello 的 `server.teamModes` 报
  `["delegatesHeavy", "ultra"]`：Friday 2.82 起的客户端任务模式菜单、`/mode` 只列这两项，选着「不派」的显示成需要时派（更早的客户端照旧三项）。
  模型列表读 EDITH 的 `config.json`（顶层那个是 `edith`，`engines` 里的照原样），设置里加改删模型、存删密钥写回 `config.json` / `secrets.json`。
- **设置页、记忆**：设置 › 智能体（系统提示、要不要批准、新对话的权限模式和模型、默认工作目录）、设置 › 工具（工具开关、技能、MCP 服务）改的是
  EDITH 的 `config.json`、`mcp.json`；工具清单问默认工作目录的大脑（`tools.list`）。设置 › 记忆直接读写 EDITH 的 `memory.json`（模型用 save_memory / search_memory）。
- **常驻的那份**：网关第一次要用时拉起（`~/.edith/friday/serve.json` 登记地址和令牌，`serve.lock` 保证只一份，日志 `serve.log`），
  它连上这台每个登记了的大脑（主令牌；网关新起的大脑先叫它马上连上，第一轮就有浏览器工具）。在大脑里叫 `friday`（网关叫 `friday-client`），
  报 `resident`（不算有人连着，大脑闲够了照样退）。没有网关连着、没有在跑的终端、没在干活的大脑，闲半小时自己退，下次用到再起。
  `EDITH_FRIDAY_HUB=off` 不用它（终端、浏览器用不了）。
- **终端**：常驻的那份开伪终端跑 shell（macOS / Linux 的 posix_openpt，Windows 的 ConPTY），客户端断了照常跑，接上来重放最近 512 KB 的输出。
- **智能体的终端**（0.12，同 Friday v2.88）：常驻的那份在大脑里开 `run_in_terminal` / `read_terminal` / `close_terminal`（模型那边叫 `friday__*`，
  批准同 run_shell：删除类自动模式也问）。shell 只跑这条命令（Unix `$SHELL -l -i -c`、Windows PowerShell `-EncodedCommand`），屏幕第一行写一行淡色的 `$ 命令`，
  客户端的终端卡片、后台任务里看得见（`terminals` 里带 `command`）。命令还在跑时**主动给开它的会话发内容**：打出带 `::friday::` 的行（送标记后面的字）、
  匹配 `notify_on` 的行、跑完（退出码、跑了多久、最后 40 行）、用户把还在跑的关了——经能力协议的 `sense`（`kind: "command"`、`urgency: "interject"`：
  大脑在干这段会话就塞进下一步，闲着就马上开一轮）送回去；一秒内的几行合成一条（`EDITH_FRIDAY_TERM_BATCH_MS`），一个终端最多 30 次
  （`EDITH_FRIDAY_TERM_MAX_PUSHES`）。大脑已经闲够了自己退了就先拉起来再送。跑完的留 90 秒（`EDITH_FRIDAY_TERM_RETAIN`）再收掉，最后的输出
  `read_terminal` 还读得到。网关把这些消息（「（…能力「friday」（command）…）」下面是「[终端 …」）标成汇报（fromAgent，Friday 里不显示），卡片标题同 Friday。
  **有后台命令在跑时不换新版**：新网关请常驻的那份退（`serve.retire`）时回 `{busy: N}`，新网关先接着用它；它等后台命令都跑完（再过 2 秒，让「跑完了」先送到）自己退。
- **浏览器**：常驻的那份起这台装着的 Chrome / Chromium / Edge 的无头模式（`EDITH_BROWSER` 指定别的），标签按会话开，记在
  `~/.edith/friday/browser/tabs.json`，它重起后按网址重新打开；画面推给看着的客户端。
- **智能体的浏览器工具**：是常驻那份的动作（`friday__browser_navigate` / `read` / `act` / `screenshot` / `resize` / `tabs` / `hand_over` / `run_js`，
  照 Friday 的 BrowserTools），操作的就是这段会话在卡片里的那些标签。批准用 PreToolUse 钩子告诉大脑（`need`）：打开新网站按网站问、本机 / 内网另说、
  点输入跑脚本问一声、自动模式下点付款登录和往密码框卡号输入也问；截图随动作的 `media` 交给能看图的模型；智能体开的标签一轮结束（`done`）关掉，
  `keep` 的留给用户；`hand_over` 推 `browser.handOver`，用户点「好了」时网关照这段会话选的档位替用户说一句。
- **本地能力**（Friday.Powers，`powers.*`）：常驻的那份取 Friday.Powers 的 index-v2.json（`FRIDAY_POWERS_INDEX` 可以换），这台能用的平台包
  （Metal / Vulkan / CUDA，共用运行库 cuda13）在这里装、更新、开关、选项、启停：断点续传、比大小和 SHA-256、系统的 tar 解开，
  进程 `serve …` 报 `listening` 才算起来，关标准输入就退，闲 15 分钟停掉。目录 `~/.edith/friday/powers`（`FRIDAY_POWERS_DIR`）。
  本地语音的用户音色导入、删（`powers.importVoice` / `deleteVoice`，先 `file.upload`）。
- **本地模型**（`models.*`）：模型库是魔搭 / Hugging Face 的缓存布局（和这台的 Friday 桌面端共用），量化档、分片、视觉投影的规则同 llama.cpp；
  从魔搭 / HF 下、强制删。`models.use` 在 EDITH 的 `config.json` 里建本地引擎（`power://llm`、「组织/仓库:量化档」）给会话换上，
  大脑每段请求前问常驻的那份 `model.endpoint`（能力协议 §6.6）：拉起 friday-llm，回地址、令牌、`x_friday`（模型文件、会话、优先级）。
  本地大模型关掉时本地引擎跟着标 `disabled`。生图 / 生视频的模型「使用」后记在 `~/.edith/friday/settings.json`。
- **定时任务**（`/loop`，`loops.*`）：会话里打 `/loop …` 不交给模型——常驻的那份建、列、停，问答记进会话（EDITH 的 `note`）；
  到点把 loop.run 提示词交给那段会话的大脑（排在手上这一轮之后，照会话选的档位；大脑没在跑就起），这一轮的回答就是这次的结果，
  自己定节奏的按 `/loop next`，智能体建的 `/loop done` 就结束，用户建的只有用户能停；错过的 12 小时内补一次。
  主智能体的 `loop` 动作（最多 5 个、一天 24 次，子 Agent 不给）。存在 `~/.edith/friday/loops.json`；有没暂停的任务时常驻的那份不退。
- **脚本工具**：`~/.edith/friday/tools/<名字>/`（格式同 Swift / Qt，存储用 friday-core 的 script_tools）；启用了的是常驻那份的动作，
  按权限加沙盒（macOS sandbox-exec，Linux 有 bwrap 时用它），声明的密钥从 EDITH 的密钥库按同名环境变量给，60 秒超时。
  智能体的 `create_tool` / `update_tool` / `delete_tool` / `list_tools` / `read_tool`（存了就自测，通过才启用）；设置 › 工具的建、存、自测、回滚、开关、删。
  工具变了经 `capability.update`（能力协议 §4.7）告诉大脑。
- **钩子**（`hooks.*`）：同 Claude Code 的格式：全局 `~/.edith/friday/hooks.json`、项目 `.friday/hooks.json`（要信任、内容变了再信任）、插件带的；
  大脑在四个时机问常驻的那份（能力协议 §6.7），这里跑命令：标准输入是 Claude Code 那个 JSON，退出码 2 拦下 / 接着做，JSON 输出照 Claude Code 认。
- **插件**（`plugins.*`）：Friday（plugin.json）和 Claude Code（.claude-plugin/）两种写法，从 GitHub 地址或客户端传来的 zip 装：先看一眼列清单，点了才装到
  `~/.edith/friday/plugins/<名字>/`。启用的钩子、脚本工具直接用；技能链进 EDITH 的 `~/.edith/skills/<插件>-<技能>`，MCP 服务并进 `~/.edith/mcp.json`
  （「插件名-服务名」，带 `fridayPlugin` 标记）——停用、卸载时撤掉。插件的脚本工具只读。
- **能力提供方**（`capabilities.*`）：装好的本地语音是「听录音、说话」的提供方，friday-diffusion 是「生图、生视频」的；智能体的 `transcribe_audio` / `speak` /
  `generate_image` / `generate_video` / `check_job` 是常驻那份的动作（有开着的提供方才给；生视频是异步任务，check_job 取回）。设置页能开关、关掉一项、改名、看任务。
  结果存在 `~/.edith/friday/media/`。在线服务接入时它的看图 / 生图 / 听录音这些也是提供方（照 Friday 的在线提供方发请求）；提供方包（`.fridayprovider`，
  脚本提供方）导入、导出、重测、删，脚本照 Friday 的格式跑。
- **在线服务商**（`services.*`，设置 › 服务与密钥）：服务目录同 Friday，一个密钥一家服务；接入时对话模型进 EDITH 的 `config.json`、别的进能力提供方，
  同一家第二个账号是「id#n」、密钥名 X_n，不在目录里的 OpenAI 兼容服务认成 custom-…；拉模型列表（`/models`），余额和套餐用量按接口地址认
  （DeepSeek、Kimi、硅基流动、OpenRouter、智谱 / Z.ai、MiniMax、one-api / new-api 中转）。
- **命令**（`commands.*`，输入框里的「/名字」）：同 Claude Code 的 slash commands（会话目录的 `.friday/commands/`、`.claude/commands/`，`~/.claude/commands/`，
  `~/.edith/friday/commands/`，这台 Friday 的 Commands，插件的）；发给模型的是展开后的内容（$ARGUMENTS、!`命令`、model、allowed-tools 那一轮免确认），
  对话里照旧显示用户打的。模型能用 `use_command` 自己调（`disable-model-invocation` 的除外）；现成的子智能体是大脑的（`subagent_type`）。
- **提示词文件**（`prompts.*`）：EDITH 的 `~/.edith/Prompts` 是 git 仓库，文件一变停 1.5 秒自动提交；看历史、回滚、恢复成自带的（`.upstream/`）、
  自动回滚的开关；评测现在的和上一版（见下面「复盘」）。
- **文件存放位置**（`storage.*`）：整个数据根（默认 `~/.edith`）换位置，默认位置留 `storage.json` 指过去（EDITH、网关、edith-connect、edith-websocket 都按它找）；
  「重启服务」时才搬（停掉常驻那份和闲着的大脑，复制、核对、改指向、删旧的）。模型文件夹单独选，默认缓存里的能「搬过来」。
- **实时语音**（`voice.*`）：客户端只管麦克风和喇叭（`voice.feed` 进、`voice.audio` 出，16 位 PCM），听写、交给大脑、边写边念、插话都在常驻那份做——
  连本地语音的两条实时 WebSocket（Friday.Powers 的 voice-protocol.md）。说的话交给这台的**助手**（见下面「助手」，助手关着时不能用语音），
  记进助手的日志（来源 `voice`），助手的回复边到边念（「（不说）」不念）。按住说话（`voice.held`）松开前攒着；
  回话时开口压低音量（`voice.duck`），说完一句算插话：念的停下、客户端丢掉没放的（`voice.clearAudio`），这一步剩下的字不念，看到插话的下一步照常念；
  「停」（`voice.interrupt`）停下大脑这一轮；打断之后、合成回「已打断」之前到的音频丢掉。一台同时一路：后开的接过来，原来那一路 `active` 变假、说明被谁接过去。
- **复盘、内置钩子、自我改进**（同 Friday 的 MemoryLearner / BuiltinHooks / PromptEval / PromptImprover）：常驻那份收着全部会话的事件，每一轮记一条
  （用户说的、提示词版本、停止 / 改了重发 / 重试 / 出错这些信号、读过的技能、查到的记忆、调了哪些工具、回答），存在 `~/.edith/friday/learn.json`。
  - 内置钩子（`hooks.state` 的 `builtin` / `builtinModels`、`hooks.setBuiltin`，默认都开）：「会话第一条消息」附上记忆库和技能的目录（UserPromptSubmit 补的上下文，
    界面上不显示）；「一轮结束」这段会话全部做完后，用选的模型（自动：快的）整理——记下 / 改掉 / 忘掉 EDITH 的记忆（`memory.json`，`source: auto:turn`）、
    记经验、建 / 改技能（`~/.edith/skills/<名字>/`，标 `source: auto`、`status: 试用`），没整理成的试 3 次。
  - 整理、判定、评测、改进「自动」挑模型时不挑本机的大模型（`power://`，后台活不该把它拉起来占内存），要用它在设置里选。
  - 判定：一段对话交付后 12 小时没再说话判一次——几件事、追问几次（就是各提示词版本的一次交付率、追问率），用过的记忆、技能记成败，对不上的改掉、忘掉，
    试用的技能帮上了转正、带偏了移到回收站，写了「怎样算做好」的收成评测题；判完开着自动回滚、现在的提示词比上一版明显差时回滚（friday-core 的规则）。
    自动学的记忆、技能追问比平时多或 90 天没用上的归档 / 移到回收站。
  - 评测（`prompts.evaluate`）：照那个版本的段拼主智能体的系统提示、不带工具作答，再对照「怎样算做好」打分。
  - 自我改进（`improve.*`，默认关）：每天一次（或「现在找一次」）看最近没做好的段和题，诊断、改几段提示词（受保护的、工具参数的说明不给改），
    评测不比现在的差才交给用户，采用才写进提示词文件、提交成一个版本；要改代码的在设好的源码目录开工作树（`~/.edith/friday/Improve/<id>`，分支
    `improve/<id>`）交给一段会话做，做完核对改了哪些文件（碰了受保护的拦下），等用户推送或丢掉；改进者自己成绩差时改它自己的几段，采用后采用率掉了就退回。
- **升级后换新的常驻那份**：常驻的那份在 `serve.json` 里记着自己的版本、程序文件和它当时的修改时间、大小（`exe`、`build`）。网关连它之前先看：
  同一个程序文件后来换过（升级了 edith-friday——同一版的补丁号程序里的工程版本不变，所以看文件），或者它的版本比网关旧，就请它退出（`serve.retire`；
  0.9 及以前的不认，按 pid 结束它）、起新的。别的拷贝起的、版本不比网关旧的不动（免得两份来回踢）。开着助手时它不会闲退，不这样就一直是旧的。
- **助手**（`assistant.*`，同 Friday 的 AssistantRuntime / WorldBoard / Triage / TurnBrief，Friday 仓库 qt/docs/ASSISTANT.md）：在常驻那份里，
  默认关，设置里打开（`assistant.settings.update {enabled}`）。客户端的助手页看这台时看的就是它。
  - **脑子是 EDITH**：一段藏起来的「助手」会话（默认工作目录的大脑里，`sessions.create` 带 `owner: "#assistant"`、会话列表不列，带助手的人设当专用系统提示
    `instructions`，id 记在 `~/.edith/friday/assistant/state.json`）。用户说的话交给它，后面附上「[现在]」和状态板；助手在回时用户又说话就打断它带上这句重来
    （`urgency: interrupt`）；它的回复边写边进日志，回「（不说）」的标 silent。以「#短号」开头的话原样发给那段会话、不经过大脑。
  - **管会话的工具**是常驻那份的动作，只给助手会话（动作的 `sessions`）：`list_sessions`、`create_session`（在某个目录开会话、交任务）、`send_to_session`
    （默认排到下一轮，`interject` 补进正在做的这一轮）、`read_session`、`manage_session`（停下、关注、改名）、`post_detail`（给看的内容）；记事用 EDITH 的
    `save_memory` / `search_memory`，定时用 `loop`。
  - **状态板**：这台全部会话现在的样子（在跑 / 等确认 / 在问你 / 出错 / 空闲，在跑哪个工具、最后一句、短号、助手派的、用户在等的），代码维护，
    80 毫秒合并推一次（`assistant.board`）。「等确认」靠 EDITH 的 `approval` 事件；回执上的「批准 / 拒绝」（`assistant.resolveApproval`）由客户端连着的网关
    认出手上那件待确认来答。
  - **回执和分诊**：一轮做完按代码模板写（`完成｜机器｜标题 #短号`、挑出来的一句、改了几个文件 · 跑了几条命令 · 用时），不过模型；出错、在问你、等确认、
    卡住（在跑 5 分钟没动静，有工具在跑时 15 分钟）立即，助手派的、关注的、30 分钟内提到过的「用户在等」及时——这两级叫醒助手（每小时最多 30 次，
    客户端前台正看着那段会话时不叫），带上交给它的和它最后的回答让它核对；别的只上状态板、记日志。推送（`assistant.push`）按推送规则（默认要紧的）。
  - **聊天工具渠道**（`assistant.channels.*`，同 Friday 的 ChannelHub）：SChat 机器人（`/bot/v1` 长轮询；服务器是 HTTPS 自签证书时按 SPKI 指纹认，
    第一次连上记下 `options.pin`，之后对不上不连）、Telegram（Bot API 长轮询）、脚本渠道（`options.script`，stdin / stdout 一行一个 JSON：出
    `{type: "send", chatKey, text, replyTo, buttons}`，入 `{type: "message", chatKey, senderID, senderName, text, group, replyTo}`）。配置在
    `assistant/channels.json`，token 存 EDITH 的密钥库（`SCHAT_BOT_TOKEN`、`TELEGRAM_BOT_TOKEN`，第二个起带序号）。绑定码（`bindCode`，6 位、30 分钟）：
    在聊天工具里给机器人发「/bind 绑定码」成为主人，在群里发就是推送的去处；只听主人的私聊。主人的话交给助手（来源是渠道 id），回复回到最近
    说话的那个聊天、在想时「正在输入」；/help /status /sessions /stop /new 直接答；图片、视频、文件下载好当附件，语音先听写；回执和助手主动说的
    推到开了「通知」的渠道的主人和群（回执带「接着做…」按钮、记着 #短号：引用回复它 = 直接发给那段会话）。都按纯文本渲染、超长切条。助手关着时渠道都停。
  - **通话**（同 Friday 的 AssistantCalls / AssistantTextSession，Friday 仓库 qt/docs/ASSISTANT.md §8；`calls` 特性，见下面「通话内核」）：SChat 渠道
    （`options.calls`，默认开）连上后 `setCallSettings` 声明能打电话，另开一条通话连接（`ws(s)://服务器/ws`，子协议 `schat-ws.v1`，hello 带机器人 token，
    回 self ack 才算连上，渠道列表的 `callsReady`；每 25 秒 ping，断了 3 秒起翻倍重连），`p2p_*` 信令原样进出。只接主人打来的（设置「接电话」`answerCalls`，
    默认开；还要装着本地语音），先拿服务器给的 TURN（`getIceServers`）再自动接；别人打来的回 `user_rejected`，在别的渠道通话时 `busy`，两边同时打按 `p2p_glare` 让。
    接通后先应一声，之后就是实时语音那一路接过来：对方的声音换成 16 kHz 交给听写，说完的话交给助手（来源 `call:<渠道>`，口语、短），回复边写边念、
    48 kHz 每 20 毫秒一帧送进通话（用户开口时压低、插话打断照语音那样），`post_detail` 的长内容发到那个聊天里；视频通话时助手的画面是一个跟着音量起伏的光环
    （360×640）。挂断记通话时长。助手的 `call_user` 打给最近跟它说话的主人（按设置「主动打电话」`callPolicy`：never / asked / important——没人让打的一刻钟最多一次；
    免打扰 `quietStart` / `quietEnd`，`set_quiet_hours` 设），接通后先念打电话的理由，30 秒没人接发 `request_cancel`。
    视频通话时说的每句话带上对方摄像头最新的一帧（PNG，存在 `assistant/attachments/`，用户那条的 `data.attachments`），随这句话交给 EDITH 的 `images`
    （模型标了 `supportsVision` 才看得到）。
    P2P 文字（`callType: "text"`）：同一道闸，建一条只有 DataChannel 的连接（手机出 offer），打的字交给助手（来源 `p2ptext:<渠道>`），回复转纯文本发回通道
    （没开时攒着，会话结束了发到普通聊天里）；图片、文件回一句只看得懂文字，要升级成语音回 `upgrade_reject`。
  - **只听你的声音**（同 Friday 的 Voiceprint，ASSISTANT.md §16.2–16.3；语音模式和通话都管）：本地语音能算声纹（清单里有 `speakerEmbedding`）时实时听写带上
    每句的声纹（`speaker: true`），声纹存在 `~/.edith/friday/voiceprint.json`（同 Friday 的格式：登记的 + 最近的你 + 临时的）。设置「只听你的声音」
    （`voiceOnlyEnrolled`，默认开）、登记过声纹时：不像你的不交、不打断，记一条说明「没交给助手（不像你的声音：相似度 0.12，要到 0.35）：…」（`data` 带
    `voiceprint`、`voiceprintThreshold`、`durationMs`、`voiceprintVector`、`heard`），点「是我说的」（`assistant.voice.confirm`）记成临时的（14 天、最多 5 个）；
    交了的那句带 `data.voiceprint`（相似度），够长（≥ 3 秒）、很像的往「最近的你」挪一小步。门槛按严格程度（`voiceprintStrictness` 0–2：0.35 / 0.45 / 0.55，
    不到 1.5 秒的放宽 0.15），通话里松一档。助手在说 / 在想时不到两个实在的字（附和、杂音）不交；只听出一个字母的不交。说完一句等多久（`voicePatience` 0–2）
    是听写的断句静音（0.3 / 0.45 / 0.8 秒）。这三项客户端用 `settings.update` 推过来（网关转给这台的助手）。登记：助手的 `voiceprint` 工具（`enroll`）——
    正在语音或电话里时，接下来说的三句（每句 1.5 秒以上）不交给助手，取平均存起来，几句不像同一个人就不存；`status` / `reset`（去掉自动更新和临时的）/ `clear`。
  - **远程工作通道**（`callType: "work"`，Friday 仓库 qt/docs/REMOTE.md §2「WebRTC 工作通道」；SChat 桌面客户端扫码登录后这样连这台）：只认建这个机器人的人
    （`/bot/v1/me` 的 `botOwnerId`，还得是绑定的主人），别的回 `user_rejected`；每个 requestId 一条，同一个人的几台可以同时连。先拿 TURN、建好连接再回 accepted，
    客户端建 DataChannel `friday-work`、出 offer；通道开了这边起一个网关子进程（`edith-friday --attach`，环境变量 `EDITH_FRIDAY_CHANNEL=work`），通道上的消息
    一条一行进它的标准输入、它的输出一行一条发回去——协议、方法和 SSH 上的完全一样，只是只认 SSH 的回错误 `4`（`server.*` 除 `server.status`、`mcp.update` /
    `saveServer` / `saveConfigText` / `importText`、`tools.saveScript` / `testScript` / `rollback` / `createScript`、`capabilities.import` / `retest`、
    `plugins.install` / `update`、`hooks.saveGlobal` / `trust`、`sync.*`、`assistant.export.*` / `import.*`）。一条 JSON-RPC 消息大于 200 KB 时切成 ≤ 128 KB 的
    二进制片（`'F''W''1'` + 8 字节大端 sid + 4 字节 seq + 4 字节 total + 分片，同 Friday 的 WorkFramer，单条上限 64 MB，协议错就断开）；通道上积着 4 MB 以上时先攒着。
    对端 hangup、通道关了、ICE 断了、30 秒没连上、网关退出都收尾（发 hangup，关网关）。
- **客户端设备**（同 Friday 的 ClientDevices，Friday 仓库 qt/docs/REMOTE.md §6「客户端设备」；`server.features` 报 `clientDevices`，`server.clientDevices.power`
  说 devices 能力装好开着没有）：硬件插在客户端那台电脑上（ESP / Arduino / STM32 开发板的串口、摄像头、安卓 / iOS 手机、局域网设备）。
  - 客户端在 hello 的 `features` 里声明 `clientDevices`（`clientDevices.kinds`）：它的网关登记给常驻那份；之后这边能反过来向它发 `device.*` 请求——网关编 id
    `"s:N"` 发给客户端，回复、超时（错误 `23`）、客户端断开（`21`）各回一次；客户端发来的 `device.*` 通知转过去。SSH 进来的也行（没有专用通道）。
  - 工作通道上串口会话、TCP 隧道各走同一个连接上的一条专用通道（客户端建的 `friday-serial:<id>` / `friday-tcp:<id>`，带内协商），字节原样转；
    别的 label 不认（关掉）。
  - 业务都在 devices 能力（Friday.Powers，设置 › 能力里装，清单声明 `clientDevices`，不用显卡的 `<系统>-<架构>` 包）：有登记的客户端时拉起它，经
    `/v1/bridge`（WebSocket：文本帧 JSON——`hello`、`clients`、`notify`、`channel.open` / `closed` / `pause` / `resume`、`response`，它发 `request`、
    `channel.close`；二进制帧是通道数据 `[1 字节头长 H][H 字节「客户端\nlabel」][数据]`）中转。它的 MCP 服务（`/mcp`）以名字 `client` 写进 `~/.edith/mcp.json`
    （带 `fridayDevices` 标记、`autoApprove`：动硬件前客户端那头会问拿着设备的人），智能体用 `client__…` 的工具；能力重启换了地址、令牌就换上新的
    （EDITH 已连着的 MCP 服务配置改了，调用时按新配置重连）。通道积压过 4 MB 叫它先别读那头，发完了再接着；桥连着时不让它当闲着停掉。
    客户端都走了 1 分钟后断桥、去掉 MCP 服务；桥断了 3 秒后重连，起不来 30 秒后再试。
  - 日志在 `~/.edith/friday/assistant/journal/<日期>.jsonl`（只追加，同一个 id 后写的算），设置在 `settings.json`；`assistant.history` 按页取，`assistant.clear`
    清空并换一段新的助手会话。开着助手时常驻那份不闲退；`activity.subscribe` 的快照报 `assistant: true`（客户端的助手对这台的会话只上状态板）。

## 装上

**最省事**：Friday 客户端 2.83 起添加远程机器时「服务端」选 EDITH（默认是 Friday 服务端），连接时客户端自动把 EDITH 和 edith-friday
装到那台的 `~/.edith/bin/`（Windows：`%USERPROFILE%\.edith\bin\`），之后有新版自动升级（Friday 仓库 qt/docs/REMOTE.md §9「EDITH 当服务端」）；
连上后在 设置 › 模型 里给那台配模型。

也可以在要被连的那台机器上自己装（给选了 Friday 服务端、但那台没装 Friday 桌面版的；装了的客户端会先用桌面版自带的服务端）：装好 EDITH，配好模型，一条命令：

```bash
edith friday install        # 下载 edith-friday、装成 ~/.friday-server/bin/friday-server，edith 自己拷一份到旁边
edith friday status         # 看装了没有；edith friday uninstall 去掉（原来是 Friday 服务端的换回去）
```

已经装着 Friday 自己的服务端时要加 `--force`（原来的改名成 `friday-server.friday` 留着）。手动装也行：

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
| 新建（空白会话）、删、改名、置顶、草稿、分叉 | `conversation.create`、`remove`、`rename`、`setPinned`、`setDraft`、`fork` |
| 选工作目录（没发过话的）、列目录、新建文件夹、最近的目录 | `runner.setWorkingDirectory`、`fs.list`、`fs.mkdir`（特性 `mkdir`）、`directories.*` |
| 发话：闲着开始一轮，忙时补充进这一轮，或排到下一轮；拿掉排着的 | `runner.submit`（`whenBusy: "queue"` 排队，特性 `submitFrom`）、`runner.removeQueued` |
| 改一条重发、切分支、出错了重试 | `runner.submit` 带 `replacing`、`runner.selectBranch`、`runner.retry`（`runtime` 的 `failedMessageID` / `canRetry`） |
| 权限模式、思考档位、模型、任务模式（特性 `teamModePerConversation`） | `runner.setPermissionMode` / `setThinking` / `setEngine` / `setTeamMode`；`engines.update` / `remove`、`secrets.set` / `remove` |
| 附件、文件卡片预览 | `file.upload`（一块块传，回这台上的路径）、`fs.read` |
| 流式看回复、速度和用量、上下文进度环 | `message` / `conversation` / `runtime` / `runner` 事件 |
| 批准（主智能体的、子智能体的；几个客户端都问，先答的算） | `runner.resolveApproval` |
| 停下 | `runner.stop`（排着的也不干了）、`runner.stopAll` |
| 子智能体：卡片里看它的运行记录、Agent 树、停掉、追加指示、拿掉没看的指示 | `team` 事件、`runner.backgroundAgents`、`agent.stop`、`agent.send`、`agent.removeQueued` |
| 文件卡片的「本次会话改动」、改动视图、撤销 | `fs.changes`、`fs.diff`、`fs.revert`（交给 EDITH 的 `changes.*`） |
| 设置页（特性 `config`） | `config.subscribe`、`settings.get` / `update`、`tools.setEnabled` / `skill` / `deleteSkill` / `refreshSkills`、`mcp.*` |
| 记住的事（特性 `memory`、`memorySearch`） | `memory.get` / `search` / `add` / `setText` / `setPinned` / `archive` / `restore` / `remove`、`memory` 事件 |
| 终端 | `term.open` / `attach` / `detach` / `write` / `resize` / `close`、`terminals`、`term.output` 事件 |
| 浏览器 | `browser.open` / `select` / `close` / `navigate` / `watch` / `container` / `fit` / `input` / `dialog` / `handOverDone`、`browser`、`browser.frame`、`browser.handOver` 事件 |
| 本地能力（特性 `powers`、`userVoices`） | `powers.subscribe` / `unsubscribe` / `refresh` / `install` / `cancel` / `uninstall` / `setEnabled` / `setOption` / `start` / `stop` / `importVoice` / `deleteVoice`、`powers` 事件 |
| 本地模型（特性 `models`） | `models.subscribe` / `unsubscribe` / `refresh` / `use` / `download` / `cancel` / `retry` / `dismiss` / `release` / `delete`、`models`、`modelsFailed` 事件 |
| 定时任务（特性 `loops`） | 会话里的 `/loop …`、`loops.list` / `setPaused` / `runNow` / `remove`、`loops` 事件 |
| 脚本工具 | `tools.scriptVersions` / `saveScript` / `testScript` / `rollback` / `createScript` / `deleteScript`、`tools.setEnabled` |
| 钩子（特性 `hooks`）、内置钩子（特性 `builtinHooks`） | `hooks.state` / `saveGlobal` / `trust` / `example` / `setBuiltin` |
| 插件（特性 `plugins`） | `plugins.list` / `inspectGithub` / `inspectData` / `update` / `install` / `discard` / `setEnabled` / `uninstall` |
| 能力提供方、提供方包 | `capabilities.setEnabled` / `setKindAllowed` / `setTitle` / `setOption` / `deleteJob` / `refreshAvailability` / `retest` / `import` / `export` / `deletePackage` |
| 在线服务商（特性 `services`、`serviceOps`、`serviceUsage`） | `services.install` / `installCustom` / `apply` / `setEnabled` / `uninstall` / `fetchModels` / `usage`（百炼 Token Plan / Coding Plan 用 EDITH 密钥库里的控制台 token `DASHSCOPE_CONSOLE_TOKEN` 或阿里云 AccessKey 查，都没有 / 过期了回 `consoleLogin: missing / expired`，客户端在自己的浏览器里登录后经 `secrets.set` 存过来；同 Friday v2.85） |
| 命令（特性 `commands`） | `commands.list`，输入框里的「/名字 参数」 |
| 提示词文件（特性 `prompts`） | `prompts.get` / `recheck` / `rollback` / `restoreBundled` / `setAutoRollback` / `evaluate`、`prompts` 事件 |
| 文件存放位置（特性 `storage`） | `storage.get` / `measure` / `move` / `cancel` / `setModels` / `adoptModels` / `restart`、`storage` 事件 |
| 实时语音（特性 `voice`） | `voice.start` / `stop` / `state` / `feed` / `held` / `interrupt` / `playing` / `played`、`voice.state` / `voice.audio` / `voice.clearAudio` / `voice.duck` 事件 |
| 自我改进（特性 `improve`） | `improve.get` / `run` / `runSelf` / `setEnabled` / `setEngine` / `setRepository` / `accept` / `dismiss` / `push` / `discard`、`improve` 事件 |
| 客户端的助手看这台的会话（特性 `activity`） | `activity.subscribe` / `unsubscribe`、`activity` / `activity.assistant` 事件 |
| 助手（特性 `assistant`） | `assistant.status` / `subscribe` / `unsubscribe` / `history` / `say` / `stop` / `settings.update` / `presence` / `resolveApproval` / `clear` / `memory.list`、`assistant.entry` / `board` / `state` / `push` / `reset` 事件 |
| 助手的聊天工具 | `assistant.channels.list` / `add` / `update` / `remove` / `removeOwner` / `removeGroup` / `bindCode`、`assistant.channels` 事件 |
| 助手的通话（SChat 渠道的语音 / 视频电话、P2P 文字、远程工作通道） | 渠道列表的 `calls` / `callsReady`、`options.calls`；设置的 `answerCalls` / `callPolicy` / `quietStart` / `quietEnd` |
| 客户端设备（特性 `clientDevices`） | hello 的 `features` / `clientDevices.kinds`、`device.*` 反向请求（id `"s:N"`）和通知、工作通道上的 `friday-serial:` / `friday-tcp:` 通道 |
| 只听你的声音（声纹） | `settings.update` 的 `voiceOnlyEnrolled` / `voiceprintStrictness` / `voicePatience`、`assistant.voice.confirm`、`voice.state` 的 `filtering` |

还不行的（回 `-32601` 没有，或错误 `4` 说明原因；`server.features` 不报，客户端就不显示）：

- 渠道里批准工具调用（`approveFromChannels`）、流式回复（SChat streams、Telegram 草稿）、发文件和语音回复、问题卡片还没有；
  助手回答会话的 `ask_user` 问题、附件读取还没有。
  不带 `calls` 特性的包（Linux 的 x86、armv7、armv6、riscv64、loong64、ppc64le、s390x）不能打电话、不能连工作通道（渠道列表 `calls: false`；
  客户端设备只有 SSH 进来的反向请求）。
- 任务模式「不派子智能体」：EDITH 上没有这一档，按原来的（需要时派）做。
- 要批准的事：没有客户端连着时大脑等能批准的连上来（最多 10 分钟，过了就不做）；要无人值守就选「完全」权限模式，
  或在 `~/.edith/config.json` 的 `permissions.allow` 里放开、用 `edith --auto`。

## 通话内核

`tools/friday/call/`：Secret-Chat 桌面端的通话内核（P2PCallManager + DcMediaEngine，Friday 也用这一套）、C 接口和依赖的 CMake（libdatachannel、
Mbed TLS、Opus、openh264，第一次配置时从 GitHub 拉源码），见那里的 README.md。`cargo build --features calls` 时 `build.rs` 用 CMake（Release）编好、
静态链进 edith-friday；不开特性就不要 CMake、不带通话。发布的包在 macOS（arm64、x64）、Windows（x64、x86、arm64）、Linux（x64、arm64）上开着。

```bash
cd tools/friday && cargo build --release --features calls     # 要 CMake 3.24+、C++17 编译器；FRIDAY_CALL_DEPS_DIR 指到现成的依赖源码就不下载
```

## 测试

```bash
cd tools/friday && cargo test
```

一头扮 Friday 客户端，一头扮大脑（登记一个假的大脑）：存档翻译、连上来接着看在干的那一轮、发话、流式、批准、干活时补充另起一段、
排队、停下、改名、空白会话第一次发话才在 EDITH 里建、选目录、列目录、新建文件夹、附件上传、删；子智能体的卡片和运行记录、Agent 树、
子智能体要批准的、停掉 / 追加指示、交回的汇报不显示；会话改动、改动视图、撤销；出错重试、改一条重发、切分支、分叉；模型、密钥、
会话上选的档位交给大脑；设置页、MCP、记忆；会话的动静。常驻的那份：网关拉起它、只一份、闲了自己退；终端真跑 shell（开、输出、重放、关）；
浏览器真开标签（这台装着 Chrome 时）；连上大脑报的浏览器工具、钩子、`resident`，PreToolUse 替自己的动作说怎么批准，做动作。
单元测试另有定时任务的写法（`/loop 每天9:00 …` 这些）、钩子的退出码 / JSON 认法。

和真的 EDITH 一起跑的端到端（假模型；本地能力用本地索引里的假包，不在测试机上加载真的大模型——16 GB 的机器按显存开满上下文会把系统卡死）：
本地能力装 / 停 / 关 / 卸载和 `model.endpoint`（`x_friday`、令牌）、真索引和魔搭的文件列表；定时任务的问答、到点、结果、`/loop done`；
脚本工具的建 / 自测 / 回滚 / 开关 / 沙盒；钩子的四个时机和项目信任、插件的装 / 投到 EDITH / 停用 / 卸载；能力提供方的听写 / 合成 / 生图 / 生视频；
智能体的浏览器工具（真 Chrome）；命令的展开 / allowed-tools / use_command；提示词文件的历史 / 回滚 / 恢复；文件存放位置的搬；在线服务商的接入、
模型列表、余额（假的服务）；实时语音（假的本地语音：听写把 PCM 当字、合成把字当 PCM）的说一句、按住说话、插话、叫停、接过去；
复盘的第一条消息附目录、一轮结束整理、判定、评测、改进建议（评测比过才交、采用写进提示词、改代码开工作树交给会话、丢掉）；
助手的开关、说话（藏起来的助手会话、人设、附状态板）、回执、派活（create_session）、做完叫醒助手、直达会话、不说、历史、等确认和在助手页拒绝；
渠道（假的 SChat、Telegram 服务器、脚本渠道）的绑定、只听主人、说话回到那个聊天、命令、回执推送和引用回复直达、图片附件、自签证书的指纹、关助手渠道停；
通话（假的 SChat 服务器转信令，假手机 `edith-friday __call-phone <ws 地址> <用户 token>` 是另一部通话内核、媒体真走本机 WebRTC；假的本地语音按音量断句、
合成回一段音调，带 speaker 时按音高给一个假声纹）的通话连接、不是主人直接拒、打进来自动接和先应一声、说一句交给助手、回复念回去、视频通话收到光环画面、
视频里说的话带上摄像头的画面（模型收到图）、挂断记时长、电话里登记声纹、不像你的不交和「是我说的」、设置关掉过滤、call_user 打给主人、
关掉主动打电话不打、P2P 文字收发和媒体回一句、工作通道（非创建者拒、通道上 hello、只认 SSH 的回 4、400 KB 回复和 300 KB 请求分片）；
客户端设备（假的 devices 能力：/v1/bridge + /mcp）的登记、反向请求经「s:N」到客户端和回来、超时 23、断开 21、通知、专用通道的字节进出桥、不认识的通道不转、
mcp.json 的 client、智能体调 client__devices 一路到客户端、SSH 进来的客户端接反向请求、能力重启后大脑重连 MCP、都走了断桥去掉 MCP 服务。
