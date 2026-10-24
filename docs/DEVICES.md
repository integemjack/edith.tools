# 共享设备（edith-connect）

麦克风、摄像头、串口、机器人、打印机……同一时间只能被一个进程打开。按目录工作时几个目录的大脑各起一份会互相抢，
所以这类设备**整台机器只起一个设备服务**持有它，设备服务连到每个要用它的大脑的套接字（大脑唯一的接口），
在每个大脑那里都是一个一级能力（[能力协议](https://github.com/integemjack/edith/blob/main/docs/CAPABILITY-PROTOCOL.md)）。

```
目录 a 的大脑 ◀──套接字──┐
                        ├── 设备服务（edith-connect --hub mic）──stdio── 设备程序（普通能力）── 麦克风
目录 b 的大脑 ◀──套接字──┘        仲裁：当前大脑、设备忙
        ▲ 大脑起启动器 edith-connect mic：确认服务在跑、让它连过来，然后退出
```

## 用法

1. 在 `~/.edith/devices.json` 定义设备：

```json
{"devices": [
  {"name": "mic", "command": "python3", "args": ["-u", "/opt/mic/mic.py"], "env": {"MIC_INDEX": "0"}, "cwd": "/opt/mic", "idleSeconds": 900}
]}
```

| 字段 | 说明 |
|---|---|
| `name` | 设备名（中转和设备服务用它找） |
| `command` / `args` / `env` / `cwd` | 怎么起设备程序（一个普通的能力，见下） |
| `idleSeconds` | 没有大脑连着多少秒后设备服务退出（连设备程序一起），默认 900 |

2. 让大脑接它（全局：所有目录；或只在某个目录，见能力协议 §8）：

```bash
edith capabilities add --global mic edith-connect mic
```

之后每个大脑的模型都看得到 `mic__<动作>`，外加一个 `mic__claim`。

## 设备程序怎么写

**就写一个普通的能力**（能力协议），只是它经 **stdio** 和设备服务讲（设备服务起它、当它的「大脑」），只管一条连接。几个大脑共用的事都由设备服务做，它只多知道三件事：

- `act` 的参数里多一个 `brain`：`{"id": "…", "workspace": "/Users/me/project-a"}`——是哪个大脑要它做的；
- `act` 的 `call` 变成 `b<编号>:<原来的>`（几个大脑的编号不撞）；`act.progress` 照样回这个 `call`，设备服务还给对的大脑；
- 它的 `initialize` 是设备服务发的（`brain.name` 是「EDITH 设备服务」）。

动作可以带三个标记（`initialize` 回话的 `actions` 里）：

| 标记 | 意思 |
|---|---|
| `"exclusive": true` | 做的时候别的大脑调这个设备的任何动作都回「设备忙」（比如「拍一张」要几秒） |
| `"holds": true` | 成功后调它的那个大脑**一直占着设备**，直到它调带 `releases` 的动作、或者它断开（比如「开始录音」） |
| `"releases": true` | 放开 `holds` 占着的（比如「停止录音」） |

例子见 [`tools/connect/tests/fake/device.py`](../tools/connect/tests/fake/device.py)。

## 设备服务做什么（约定）

自己写一个支持几个大脑的设备服务（不用 edith-connect）也照这些来：

### 每个大脑一条连接、各握一次手

设备服务连到每个大脑的套接字（报令牌），各走一遍完整的能力协议握手：大脑发 `initialize`（`brain.id`、`brain.workspace` 说明是谁），服务回设备的感知、动作
（外加 `claim`）、订阅、入口。之后这条连接上的消息只和这个大脑有关。

### 当前大脑

设备**主动**的东西——`sense`、`ask`、`sessions.*` 请求——只交给**当前大脑**，不会几个大脑一起回答同一句话：

- 第一个连上（握完手）的是当前大脑；
- 调了设备动作的大脑成为当前大脑（最近和它打交道的）；
- 调 `claim`（服务自动加的动作，没有参数）的成为当前大脑：「让麦克风之后听到的都交给这个目录」；
- 当前大脑断开了，换最近连上的那个；一个都没有时设备的请求回 `-32603`「没有大脑连着这个设备」，通知丢掉。

设备 `receives` 订的事件只转**当前大脑**的（比如音箱只念当前大脑的回答）。

### 设备忙（`-32005`）

别的大脑占着设备时（`holds` 没放、`exclusive` 动作在做），再来调动作的大脑收到：

```json
{"jsonrpc": "2.0", "id": 12, "error": {"code": -32005, "message": "设备「mic」正被 /Users/me/project-a 的大脑占着",
  "data": {"holder": {"id": "3f9a0c1b2d4e5f60", "workspace": "/Users/me/project-a"}}}}
```

EDITH 把它交给模型时说清被谁占着、建议等它用完或换个办法。占着的大脑断开时自动放开。

### 生命周期

- 启动器（`edith-connect <设备名>`）由大脑起（拿到 `EDITH_ACCESS` / `EDITH_ACCESS_TOKEN` / `EDITH_CAPABILITY`）：
  连到 `~/.edith/devices/<设备名>.json` 里登记的服务（127.0.0.1 随机端口 + 令牌，只有本用户能读；连上先发 `auth`），
  服务没起就在后台拉起（`edith-connect --hub <设备名>`）；发 `attach {address, token, name}` 让服务去连这个大脑，然后退出。
  同一个大脑已经连着就不再连。
- 一个设备只起一个服务：服务拿 `~/.edith/devices/<设备名>.lock`，几个启动器同时拉起时后来的拿不到锁就退出。
- 没有大脑连着 `idleSeconds` 秒，服务结束设备程序、删登记、退出。
- 设备程序退出了，服务也退出：各个大脑那条连接断开，大脑过一会儿重起启动器，启动器再拉起服务。
- 日志：`~/.edith/devices/<设备名>.log`（服务的、设备程序标准错误的）。
