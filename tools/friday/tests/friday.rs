//! edith-friday：测试一头扮 Friday 客户端（标准输入 / 输出讲 Friday 远程协议），一头扮 EDITH 大脑（support 的假套接字，登记在 instances/ 里）

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};

use edith_tools_support::{FakeBrain, Link, scratch};
use serde_json::{Value, json};

const SESSION: &str = "5d0c1a2b-3c4d-4e5f-8a9b-0c1d2e3f4a5b";

/// 扮 Friday 客户端：起网关、发请求、等事件
struct Client {
    child: Child,
    stdin: Option<ChildStdin>,
    messages: Receiver<Value>,
    backlog: Vec<Value>,
    next: u64,
}

impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.child.kill();
    }
}

impl Client {
    fn start(home: &Path, work: &Path) -> Client {
        let mut child = Command::new(env!("CARGO_BIN_EXE_edith-friday"))
            .arg("--attach")
            .env("EDITH_HOME", home)
            .env("EDITH_FRIDAY_DIRECTORY", work)
            // 登记里没有时别真去起一个大脑
            .env("EDITH_BIN", home.join("no-such-edith"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (sender, messages) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if let Ok(message) = serde_json::from_str::<Value>(&line) {
                    let _ = sender.send(message);
                }
            }
        });
        Client { child, stdin: Some(stdin), messages, backlog: Vec::new(), next: 0 }
    }

    fn send(&mut self, method: &str, params: Value) -> u64 {
        self.next += 1;
        let stdin = self.stdin.as_mut().unwrap();
        writeln!(stdin, "{}", json!({ "jsonrpc": "2.0", "id": self.next, "method": method, "params": params })).unwrap();
        stdin.flush().unwrap();
        self.next
    }

    fn wait_for(&mut self, seconds: u64, what: &str, mut wanted: impl FnMut(&Value) -> bool) -> Value {
        if let Some(index) = self.backlog.iter().position(&mut wanted) {
            return self.backlog.remove(index);
        }
        let deadline = Instant::now() + Duration::from_secs(seconds);
        while let Some(left) = deadline.checked_duration_since(Instant::now()) {
            match self.messages.recv_timeout(left) {
                Ok(message) if wanted(&message) => return message,
                Ok(message) => self.backlog.push(message),
                Err(_) => break,
            }
        }
        panic!("等不到 {what}；收到过：{:#?}", self.backlog.iter().map(|m| m["method"].clone()).collect::<Vec<_>>());
    }

    fn reply(&mut self, id: u64) -> Value {
        self.wait_for(30, &format!("请求 {id} 的回话"), |message| message["id"] == id && message.get("method").is_none())
    }

    fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self.send(method, params);
        let reply = self.reply(id);
        assert!(reply.get("error").is_none(), "{method}：{reply}");
        reply["result"].clone()
    }

    /// 之前收到的都不要了（接着等的是这之后的）
    fn drain(&mut self) {
        std::thread::sleep(Duration::from_millis(300));
        while self.messages.try_recv().is_ok() {}
        self.backlog.clear();
    }

    fn event(&mut self, method: &str, mut wanted: impl FnMut(&Value) -> bool) -> Value {
        self.wait_for(15, method, |message| message["method"] == method && wanted(&message["params"]))
    }
}

/// 一段存好的会话：问了一句、调了两个工具（一个没批准）、交付检查追了一条
fn write_session(home: &Path, work: &Path) {
    let dir = home.join("projects").join("work-12345678").join("sessions");
    std::fs::create_dir_all(&dir).unwrap();
    let call = |id: &str, name: &str, arguments: Value| json!({ "id": id, "type": "function", "function": { "name": name, "arguments": arguments.to_string() } });
    let lines = [
        json!({ "type": "meta", "id": SESSION, "cwd": work.to_string_lossy(), "created": "2026-10-24T10:00:00+08:00" }),
        json!({ "type": "title", "title": "写个文件" }),
        json!({ "type": "message", "message": { "role": "user", "content": "帮我写个文件" } }),
        json!({ "type": "message", "message": { "role": "assistant", "content": "先写。", "reasoning_content": "想一下", "tool_calls": [call("call_0", "write_file", json!({ "path": "a.txt", "content": "x" }))] } }),
        json!({ "type": "message", "message": { "role": "tool", "tool_call_id": "call_0", "content": "已写入 1 字节" } }),
        json!({ "type": "message", "message": { "role": "assistant", "content": "写好了 a.txt" } }),
        json!({ "type": "message", "message": { "role": "user", "content": "（Friday 检查：这一轮写了 a.txt，但写完之后没有运行或读回确认。）" } }),
        json!({ "type": "message", "message": { "role": "assistant", "content": "", "tool_calls": [call("call_0", "run_shell", json!({ "command": "cat a.txt" }))] } }),
        json!({ "type": "message", "message": { "role": "tool", "tool_call_id": "call_0", "content": "用户没批准：跑命令：cat a.txt" } }),
        json!({ "type": "message", "message": { "role": "assistant", "content": "没能验证" } }),
        // 补充进来的话：存档里带着给模型的来源说明，界面上只显示原话
        json!({ "type": "message", "message": { "role": "user", "content": "（用户在你工作时补充了下面这段：和手上的事一起考虑）\n顺便看看 b.txt" } }),
        json!({ "type": "message", "message": { "role": "assistant", "content": "好" } }),
    ];
    let text: String = lines.iter().map(|line| format!("{line}\n")).collect();
    std::fs::write(dir.join(format!("{SESSION}.jsonl")), text).unwrap();
}

/// 登记一个在跑的大脑（假的）：网关按工作目录找到它
fn register(home: &Path, brain: &FakeBrain, work: &Path) {
    let dir = home.join("instances");
    std::fs::create_dir_all(&dir).unwrap();
    let entry = json!({
        "address": brain.address, "token": brain.token, "pid": std::process::id(),
        "workspace": common_path(work), "started": "2026-10-24T10:00:00+08:00",
    });
    std::fs::write(dir.join("work-1.json"), entry.to_string()).unwrap();
}

/// 同网关（和 EDITH）认的路径：真实路径，Windows 去掉 \\?\
fn common_path(path: &Path) -> String {
    let text = std::fs::canonicalize(path).unwrap().to_string_lossy().into_owned();
    text.strip_prefix(r"\\?\").map(str::to_string).unwrap_or(text)
}

fn tools(message: &Value) -> Vec<Value> {
    message["blocks"].as_array().unwrap().iter().filter_map(|block| block["kind"].get("tool").map(|tool| tool["_0"].clone())).collect()
}

fn kinds(message: &Value) -> Vec<String> {
    message["blocks"].as_array().unwrap().iter().map(|block| block["kind"].as_object().unwrap().keys().next().unwrap().clone()).collect()
}

/// 网关连上大脑：auth → initialize（它说自己能批准、全权）→ initialized
fn accept(brain: &FakeBrain) -> Link {
    let mut link = brain.accept(15).expect("网关没连大脑");
    let reply = link.handshake(json!({ "name": "EDITH", "version": "test" }));
    assert_eq!(reply["approves"], true, "{reply}");
    assert_eq!(reply["sessions"], "all", "{reply}");
    link
}

fn sessions_dir(home: &Path) -> std::path::PathBuf {
    home.join("projects").join("work-12345678").join("sessions")
}

/// 替大脑往存档里追加（一轮做完时大脑这样存）
fn append(home: &Path, session: &str, messages: &[Value]) {
    let mut file = std::fs::OpenOptions::new().append(true).open(sessions_dir(home).join(format!("{session}.jsonl"))).unwrap();
    for message in messages {
        writeln!(file, "{}", json!({ "type": "message", "message": message })).unwrap();
    }
}

fn push(link: &mut Link, session: &str, turn: &str, kind: &str, data: Value) {
    link.send(json!({ "jsonrpc": "2.0", "method": "event", "params": { "session": session, "turn": turn, "type": kind, "data": data } }));
}

/// 连上来时大脑被问浏览器开着哪些标签：没有
fn answer_browser(link: &mut Link) {
    let browser = link.wait_for(10, |message| message["method"] == "browser.state").expect("连上来没问浏览器");
    reply(link, &browser, json!({ "available": true, "tabs": [] }));
}

/// 连上来时大脑被问浏览器、开着哪些终端：都没有
fn answer_terminals(link: &mut Link) {
    answer_browser(link);
    let terminals = link.wait_for(10, |message| message["method"] == "terminals.list").expect("连上来没问终端");
    reply(link, &terminals, json!({ "terminals": [] }));
}

fn reply(link: &mut Link, request: &Value, result: Value) {
    link.send(json!({ "jsonrpc": "2.0", "id": request["id"], "result": result }));
}

fn tool_call(id: &str, name: &str, arguments: Value) -> Value {
    json!({ "id": id, "type": "function", "function": { "name": name, "arguments": arguments.to_string() } })
}

fn nodes(conversation: &Value) -> Vec<Value> {
    conversation["params"]["conversation"]["nodes"].as_array().unwrap().clone()
}

fn has(list: &Value, id: &str) -> bool {
    list.as_array().unwrap().iter().any(|item| item == &json!(id.to_uppercase()))
}

#[test]
fn friday_client_through_gateway() {
    let home = scratch("friday");
    let work = home.join("work");
    // 工作目录是个 git 仓库：下面子目录里的会话也归这个大脑
    std::fs::create_dir_all(work.join(".git")).unwrap();
    write_session(&home, &work);
    let brain = FakeBrain::new();
    register(&home, &brain, &work);
    let mut client = Client::start(&home, &work);
    let upper = SESSION.to_uppercase();

    // 没 hello 不行；协议版本不对不行
    let id = client.send("conversation.subscribe", json!({ "id": SESSION }));
    assert_eq!(client.reply(id)["error"]["code"], 2);
    let id = client.send("hello", json!({ "protocol": 99 }));
    assert_eq!(client.reply(id)["error"]["code"], 1);

    // hello：先连上登记着的大脑，问它在干什么——这段会话有一轮在连上之前就开始了（客户端关了又打开）
    let hello = client.send("hello", json!({ "protocol": 1, "client": { "name": "test" } }));
    let mut link = accept(&brain);
    let list = link.wait_for(10, |message| message["method"] == "sessions.list").expect("连上来没问大脑在干什么");
    reply(
        &mut link,
        &list,
        json!([{ "id": SESSION, "cwd": work.to_string_lossy(), "working": true, "turn": { "id": "t0", "text": "之前发的" } }]),
    );
    answer_terminals(&mut link);
    let agents = link.wait_for(10, |message| message["method"] == "agents.list").expect("连上来没问子智能体");
    reply(&mut link, &agents, json!([]));
    let hello = client.reply(hello)["result"].clone();
    assert_eq!(hello["protocol"], 1);
    assert_eq!(hello["server"]["version"], "0.0.0", "不让客户端自动升级");
    assert!(hello["server"]["features"].as_array().unwrap().contains(&json!("mkdir")));
    assert!(has(&hello["state"]["runner"]["generating"], SESSION), "{}", hello["state"]["runner"]);
    let summaries = hello["state"]["summaries"].as_array().unwrap();
    assert_eq!(summaries.len(), 1);
    assert_eq!((summaries[0]["id"].as_str(), summaries[0]["title"].as_str()), (Some(upper.as_str()), Some("写个文件")));
    assert_eq!(hello["state"]["engines"]["defaultEngineID"], "edith");

    // 存档 → Friday 的消息：用户 / 助手 / 用户（补充，去掉来源说明）/ 助手；交付检查的提醒记成提示；后面接着在干的那一轮
    let opened = client.request("conversation.subscribe", json!({ "id": upper }));
    let shown = opened["conversation"]["nodes"].as_array().unwrap().clone();
    let roles: Vec<&str> = shown.iter().map(|node| node["role"].as_str().unwrap()).collect();
    assert_eq!(roles, ["user", "assistant", "user", "assistant", "user", "assistant"]);
    assert_eq!(shown[2]["blocks"][0]["kind"]["text"]["_0"], "顺便看看 b.txt");
    assert_eq!(shown[4]["blocks"][0]["kind"]["text"]["_0"], "之前发的");
    assert_eq!(shown[5]["stats"]["live"], true);
    assert_eq!(kinds(&shown[1]), ["thinking", "text", "tool", "text", "tool", "text"]);
    let cards = tools(&shown[1]);
    assert_eq!((cards[0]["status"].as_str(), cards[0]["title"].as_str()), (Some("succeeded"), Some("写入文件")));
    assert_eq!(cards[1]["status"], "denied");
    assert_ne!(cards[0]["id"], cards[1]["id"], "两轮都叫 call_0 也不能撞");
    assert!(shown[1]["notice"].as_str().unwrap().contains("交付检查"));
    assert_eq!(shown[1]["parentID"], shown[0]["id"]);
    assert_eq!(opened["conversation"]["selectedChildren"]["00000000-0000-0000-0000-000000000000"], shown[0]["id"]);
    assert_eq!(opened["runtime"]["generating"], true);
    // 那一轮做完：存档后重发整段（同一个 id），带上这一轮的用量（进度环）
    push(
        &mut link,
        SESSION,
        "t0",
        "stats",
        json!({ "promptTokens": 1200, "generatedTokens": 30, "generateTime": 1.5, "contextParts": { "system": 800, "tools": 300, "user": 100 }, "contextLimit": 200000 }),
    );
    append(&home, SESSION, &[json!({ "role": "user", "content": "之前发的" }), json!({ "role": "assistant", "content": "好的" })]);
    push(&mut link, SESSION, "t0", "done", json!({ "answer": "好的", "stopped": false, "error": null }));
    let done = client.event("conversation", |params| {
        params["conversation"]["nodes"]
            .as_array()
            .unwrap()
            .last()
            .is_some_and(|node| node["stats"].get("live").is_none() && node["stats"].is_object())
    });
    let stored = nodes(&done);
    assert_eq!(stored.len(), 6);
    assert_eq!(stored[5]["id"], shown[5]["id"]);
    assert_eq!(stored[5]["stats"]["contextTokenCount"], 1200);
    assert_eq!(stored[5]["stats"]["contextLimit"], 200000);
    client.event("runner", |params| !has(&params["generating"], SESSION));

    // 发话：交给连着的大脑（用户自己打的、忙时补充）
    let submit = client.send("runner.submit", json!({ "conversationID": upper, "text": "再写一个", "attachments": [] }));
    let sense = link.wait_for(10, |message| message["method"] == "sense").expect("没交给大脑");
    assert_eq!((sense["params"]["session"].as_str(), sense["params"]["text"].as_str()), (Some(SESSION), Some("再写一个")));
    assert_eq!((sense["params"]["kind"].as_str(), sense["params"]["urgency"].as_str()), (Some("user"), Some("interject")));
    reply(&mut link, &sense, json!({ "session": SESSION, "disposition": "started", "turn": "t1" }));
    assert!(client.reply(submit).get("error").is_none());

    // 这一轮：开始 → 字 → 工具 → 要批准
    push(&mut link, SESSION, "t1", "turn.started", json!({ "text": "再写一个", "origin": { "capability": "friday", "kind": "user" } }));
    let live = client.event("conversation", |params| params["conversation"]["nodes"].as_array().unwrap().len() == 8);
    let live = nodes(&live);
    assert_eq!(live[6]["blocks"][0]["kind"]["text"]["_0"], "再写一个");
    push(&mut link, SESSION, "t1", "text", json!({ "text": "好的，" }));
    push(
        &mut link,
        SESSION,
        "t1",
        "tool",
        json!({ "id": "call_0", "name": "write_file", "arguments": { "path": "b.txt", "content": "y" } }),
    );
    let approve = link
        .start("approve", json!({ "session": SESSION, "turn": "t1", "call": "call_0", "tool": "write_file", "summary": "写文件：b.txt" }));
    client.event("attention", |_| true);
    let waiting = client.event("message", |params| tools(&params["message"]).iter().any(|tool| tool["status"] == "awaitingApproval"));
    assert_eq!(waiting["params"]["message"]["id"], live[7]["id"], "正在写的那条和结构里的是同一条");
    let card = tools(&waiting["params"]["message"])[0].clone();
    assert_eq!(card["arguments"], "{\n  \"content\" : \"y\",\n  \"path\" : \"b.txt\"\n}");
    client.event("runner", |params| has(&params["pendingApproval"], SESSION));
    // 客户端点「允许」：回给大脑；批过的不能再批
    client.request("runner.resolveApproval", json!({ "eventID": card["id"], "approved": true }));
    assert_eq!(link.reply_to(approve, 10)["result"]["decision"], "allow");
    let id = client.send("runner.resolveApproval", json!({ "eventID": card["id"], "approved": true }));
    assert_eq!(client.reply(id)["error"]["code"], 3);
    // 写完：卡片上显示加了几行、删了几行
    push(
        &mut link,
        SESSION,
        "t1",
        "result",
        json!({ "id": "call_0", "name": "write_file", "ok": true, "text": "已写入", "lines": { "added": 3, "removed": 1 } }),
    );
    let counted = client.event("message", |params| tools(&params["message"]).iter().any(|tool| tool["linesAdded"] == 3));
    assert_eq!(tools(&counted["params"]["message"])[0]["linesRemoved"], 1);

    // 干活时补充的话塞进来了：另起一段（一条用户消息 + 接着的回复），又一个 call_0
    push(&mut link, SESSION, "t1", "input", json!({ "text": "顺便也写 c.txt", "origin": { "capability": "friday", "kind": "user" } }));
    let split = client.event("conversation", |params| params["conversation"]["nodes"].as_array().unwrap().len() == 10);
    let split = nodes(&split);
    assert_eq!(split[8]["blocks"][0]["kind"]["text"]["_0"], "顺便也写 c.txt");
    push(
        &mut link,
        SESSION,
        "t1",
        "tool",
        json!({ "id": "call_0", "name": "write_file", "arguments": { "path": "c.txt", "content": "z" } }),
    );
    let second = client.event("message", |params| params["message"]["id"] == split[9]["id"] && !tools(&params["message"]).is_empty());
    let second_card = tools(&second["params"]["message"])[0]["id"].clone();
    assert_ne!(second_card, card["id"]);

    // 忙时排队的话：交给大脑排着，界面上显示在队列里
    let queued = client.send("runner.submit", json!({ "conversationID": upper, "text": "排着的", "attachments": [], "whenBusy": "queue" }));
    let sense = link.wait_for(10, |message| message["method"] == "sense").expect("排队的没交给大脑");
    assert_eq!(sense["params"]["urgency"], "queue");
    reply(&mut link, &sense, json!({ "session": SESSION, "disposition": "queued", "turn": "q1" }));
    assert!(client.reply(queued).get("error").is_none());
    client.event("runtime", |params| params["runtime"]["queue"].as_array().unwrap().iter().any(|item| item["text"] == "排着的"));
    // 再排一句、拿掉它：交给大脑（按排队时回的轮）
    let second = client.send("runner.submit", json!({ "conversationID": upper, "text": "不要了", "attachments": [], "whenBusy": "queue" }));
    let sense = link.wait_for(10, |message| message["method"] == "sense").unwrap();
    reply(&mut link, &sense, json!({ "session": SESSION, "disposition": "queued", "turn": "q2" }));
    assert!(client.reply(second).get("error").is_none());
    let runtime = client.event("runtime", |params| params["runtime"]["queue"].as_array().unwrap().len() == 2);
    let item =
        runtime["params"]["runtime"]["queue"].as_array().unwrap().iter().find(|item| item["text"] == "不要了").unwrap()["id"].clone();
    let remove = client.send("runner.removeQueued", json!({ "conversationID": upper, "itemID": item }));
    let request = link.wait_for(10, |message| message["method"] == "sessions.dequeue").expect("没让大脑拿掉");
    assert_eq!((request["params"]["id"].as_str(), request["params"]["turn"].as_str()), (Some(SESSION), Some("q2")));
    reply(&mut link, &request, json!({ "removed": true }));
    assert!(client.reply(remove).get("error").is_none());
    client.event("runtime", |params| {
        let queue = params["runtime"]["queue"].as_array().unwrap();
        queue.len() == 1 && queue[0]["text"] == "排着的"
    });

    // 做完：大脑存档（这里替它追加），网关按存档重发整段；消息、卡片 id 前后一致
    push(&mut link, SESSION, "t1", "result", json!({ "id": "call_0", "name": "write_file", "ok": true, "text": "已写入" }));
    append(
        &home,
        SESSION,
        &[
            json!({ "role": "user", "content": "再写一个" }),
            json!({ "role": "assistant", "content": "好的，", "tool_calls": [tool_call("call_0", "write_file", json!({ "path": "b.txt", "content": "y" }))] }),
            json!({ "role": "tool", "tool_call_id": "call_0", "content": "已写入" }),
            json!({ "role": "user", "content": "（用户在你工作时补充了下面这段：和手上的事一起考虑）\n顺便也写 c.txt" }),
            json!({ "role": "assistant", "content": "", "tool_calls": [tool_call("call_0", "write_file", json!({ "path": "c.txt", "content": "z" }))] }),
            json!({ "role": "tool", "tool_call_id": "call_0", "content": "已写入" }),
            json!({ "role": "assistant", "content": "都写好了" }),
        ],
    );
    push(&mut link, SESSION, "t1", "done", json!({ "answer": "都写好了", "stopped": false, "error": null }));
    let done = client.event("conversation", |params| {
        let nodes = params["conversation"]["nodes"].as_array().unwrap();
        nodes.len() == 10 && nodes[9]["stats"].get("live").is_none()
    });
    let finished = nodes(&done);
    assert_eq!((finished[7]["id"].clone(), finished[9]["id"].clone()), (live[7]["id"].clone(), split[9]["id"].clone()));
    assert_eq!(tools(&finished[7])[0]["id"], card["id"]);
    assert_eq!(tools(&finished[9])[0]["id"], second_card);
    assert_eq!(finished[8]["blocks"][0]["kind"]["text"]["_0"], "顺便也写 c.txt");
    // 排着的那句开始了：从队列里拿掉
    push(&mut link, SESSION, "t2", "turn.started", json!({ "text": "排着的", "origin": { "capability": "friday", "kind": "user" } }));
    client.event("runtime", |params| params["runtime"]["queue"].as_array().unwrap().is_empty() && params["runtime"]["generating"] == true);

    // 停下：交给大脑
    let stop = client.send("runner.stop", json!({ "conversationID": upper }));
    let request = link.wait_for(10, |message| message["method"] == "sessions.stop").expect("没让大脑停下");
    assert_eq!(request["params"]["id"], SESSION);
    reply(&mut link, &request, json!({ "stopped": true }));
    assert!(client.reply(stop).get("error").is_none());
    push(&mut link, SESSION, "t2", "done", json!({ "answer": "", "stopped": true, "error": null }));
    client.event("runner", |params| !has(&params["generating"], SESSION));

    // 改名、置顶、草稿：记下、推会话列表
    client.request("conversation.rename", json!({ "id": upper, "title": "新名字" }));
    client.request("conversation.setPinned", json!({ "id": upper, "pinned": true }));
    client.request("conversation.setDraft", json!({ "id": upper, "draft": "没发完" }));
    let listed = client.event("summaries", |params| params["summaries"][0]["hasDraft"] == true);
    let summary = &listed["params"]["summaries"][0];
    assert_eq!((summary["title"].as_str(), summary["pinned"].as_bool()), (Some("新名字"), Some(true)));

    // 新建：只记下（空白会话，还不惊动大脑）；换工作目录；第一次发话才在 EDITH 里建、用客户端给的 id
    let fresh = "0c3d9e1f-2a4b-4c5d-9e6f-7a8b9c0d1e2f";
    let sub = work.join("sub");
    std::fs::create_dir_all(&sub).unwrap();
    client.request(
        "conversation.create",
        json!({ "id": fresh.to_uppercase(), "workingDirectory": work.to_string_lossy(), "title": "新的那段" }),
    );
    let listed = client
        .event("summaries", |params| params["summaries"].as_array().unwrap().iter().any(|item| item["id"] == json!(fresh.to_uppercase())));
    let item =
        listed["params"]["summaries"].as_array().unwrap().iter().find(|item| item["id"] == json!(fresh.to_uppercase())).unwrap().clone();
    assert_eq!((item["isEmpty"].as_bool(), item["title"].as_str()), (Some(true), Some("新的那段")));
    client.request("runner.setWorkingDirectory", json!({ "conversationID": fresh.to_uppercase(), "path": sub.to_string_lossy() }));
    let submit = client.send("runner.submit", json!({ "conversationID": fresh.to_uppercase(), "text": "第一句", "attachments": [] }));
    let create = link.wait_for(10, |message| message["method"] == "sessions.create").expect("第一次发话没在 EDITH 里建会话");
    assert_eq!((create["params"]["id"].as_str(), create["params"]["cwd"].as_str()), (Some(fresh), sub.to_str()));
    let meta = json!({ "type": "meta", "id": fresh, "cwd": sub.to_string_lossy(), "created": "2026-10-24T12:00:00+08:00" });
    std::fs::write(sessions_dir(&home).join(format!("{fresh}.jsonl")), format!("{meta}\n")).unwrap();
    reply(&mut link, &create, json!({ "id": fresh, "cwd": sub.to_string_lossy() }));
    let sense = link.wait_for(10, |message| message["method"] == "sense").expect("建完没交话");
    assert_eq!(sense["params"]["session"], fresh);
    reply(&mut link, &sense, json!({ "session": fresh, "disposition": "started", "turn": "t9" }));
    assert!(client.reply(submit).get("error").is_none());
    let saved = std::fs::read_to_string(sessions_dir(&home).join(format!("{fresh}.jsonl"))).unwrap();
    assert!(saved.contains("新的那段"), "空白会话的标题没带进存档：{saved}");

    // 有内容的会话不能换工作目录；没有的方法
    let id = client.send("runner.setWorkingDirectory", json!({ "conversationID": upper, "path": sub.to_string_lossy() }));
    assert_eq!(client.reply(id)["error"]["code"], 4);
    let id = client.send("terminal.open", json!({}));
    assert_eq!(client.reply(id)["error"]["code"], -32601);

    // 删：交给大脑删（这里替它删存档）
    let remove = client.send("conversation.remove", json!({ "id": upper }));
    let request = link.wait_for(10, |message| message["method"] == "sessions.remove").expect("没让大脑删");
    assert_eq!(request["params"]["id"], SESSION);
    std::fs::remove_file(sessions_dir(&home).join(format!("{SESSION}.jsonl"))).unwrap();
    reply(&mut link, &request, json!({ "removed": true }));
    assert!(client.reply(remove).get("error").is_none());
    assert!(!sessions_dir(&home).join(format!("{SESSION}.jsonl")).exists());
    client.event("summaries", |params| !params["summaries"].as_array().unwrap().iter().any(|item| item["id"] == json!(upper)));

    // 客户端走了：网关退出
    drop(client.stdin.take());
    let deadline = Instant::now() + Duration::from_secs(10);
    while client.child.try_wait().unwrap().is_none() {
        assert!(Instant::now() < deadline, "客户端走了网关还不退");
        std::thread::sleep(Duration::from_millis(100));
    }
    let _ = std::fs::remove_dir_all(&home);
}

#[test]
fn directories_files_and_uploads() {
    let home = scratch("friday-files");
    let work = home.join("work");
    std::fs::create_dir_all(work.join("项目")).unwrap();
    std::fs::create_dir_all(work.join(".hidden")).unwrap();
    std::fs::write(work.join("说明.txt"), "你好").unwrap();
    let mut client = Client::start(&home, &work);
    client.request("hello", json!({ "protocol": 1 }));

    // 选工作目录：只列子目录、不列隐藏的；files 连文件一起（目录在前）
    let listed = client.request("fs.list", json!({ "path": work.to_string_lossy() }));
    let names: Vec<&str> = listed["entries"].as_array().unwrap().iter().map(|entry| entry["name"].as_str().unwrap()).collect();
    assert_eq!(names, ["项目"]);
    assert!(listed["roots"].as_array().is_some_and(|roots| !roots.is_empty()));
    assert!(listed["parent"].is_string());
    let listed = client.request("fs.list", json!({ "path": work.to_string_lossy(), "files": true }));
    let entries = listed["entries"].as_array().unwrap();
    assert_eq!((entries[0]["name"].as_str(), entries[0]["dir"].as_bool()), (Some("项目"), Some(true)));
    assert_eq!((entries[1]["name"].as_str(), entries[1]["size"].as_u64()), (Some("说明.txt"), Some(6)));

    // 新建文件夹；同名的不行、名字带斜杠的不行
    let made = client.request("fs.mkdir", json!({ "path": work.to_string_lossy(), "name": "新文件夹" }));
    assert!(work.join("新文件夹").is_dir(), "{made}");
    let id = client.send("fs.mkdir", json!({ "path": work.to_string_lossy(), "name": "新文件夹" }));
    assert_eq!(client.reply(id)["error"]["code"], 4);
    let id = client.send("fs.mkdir", json!({ "path": work.to_string_lossy(), "name": "a/b" }));
    assert_eq!(client.reply(id)["error"]["code"], -32602);

    // 附件分两块传上来，最后一块回这台上的路径；fs.read 读回来
    use base64::Engine;
    let encode = |bytes: &[u8]| base64::engine::general_purpose::STANDARD.encode(bytes);
    client.request("file.upload", json!({ "uploadID": "u1", "name": "图.png", "offset": 0, "data": encode(b"hello "), "last": false }));
    let done =
        client.request("file.upload", json!({ "uploadID": "u1", "name": "图.png", "offset": 6, "data": encode(b"world"), "last": true }));
    let path = done["path"].as_str().unwrap().to_string();
    assert_eq!(std::fs::read(&path).unwrap(), b"hello world");
    let read = client.request("fs.read", json!({ "path": path, "offset": 6, "length": 100 }));
    assert_eq!(
        (read["data"].as_str(), read["size"].as_u64(), read["eof"].as_bool()),
        (Some(encode(b"world").as_str()), Some(11), Some(true))
    );
    let id = client.send("file.upload", json!({ "uploadID": "../x", "name": "a", "offset": 0, "data": "", "last": true }));
    assert_eq!(client.reply(id)["error"]["code"], -32602);
    let _ = std::fs::remove_dir_all(&home);
}

/// 子智能体：卡片里装它的运行记录、Agent 树、停掉 / 追加指示、它要批准的；会话改动交给大脑
#[test]
fn sub_agents_and_file_changes() {
    let home = scratch("friday-agents");
    let work = home.join("work");
    std::fs::create_dir_all(work.join(".git")).unwrap();
    // 主智能体派了一个（后台）、并联派了两个；第一个交回了一段汇报
    let dir = sessions_dir(&home);
    std::fs::create_dir_all(&dir).unwrap();
    let lines = [
        json!({ "type": "meta", "id": SESSION, "cwd": work.to_string_lossy(), "created": "2026-10-24T10:00:00+08:00" }),
        json!({ "type": "message", "message": { "role": "user", "content": "派人去查" } }),
        json!({ "type": "message", "message": { "role": "assistant", "content": "", "tool_calls": [
            tool_call("call_d", "delegate_task", json!({ "task": "查一下", "name": "小助手" })),
            tool_call("call_p", "run_parallel_tasks", json!({ "tasks": [{ "task": "甲" }, { "task": "乙" }] })),
        ] } }),
        json!({ "type": "message", "message": { "role": "tool", "tool_call_id": "call_d", "content": "已在后台派出子 Agent「小助手」" } }),
        json!({ "type": "message", "message": { "role": "tool", "tool_call_id": "call_p", "content": "已在后台派出 2 个子 Agent" } }),
        json!({ "type": "message", "message": { "role": "assistant", "content": "派出去了" } }),
        json!({ "type": "message", "message": { "role": "user", "content": "子 Agent「甲」交回结果（运行记录 id：b1）：\n甲做完了" } }),
        json!({ "type": "message", "message": { "role": "assistant", "content": "甲收到了" } }),
    ];
    std::fs::write(dir.join(format!("{SESSION}.jsonl")), lines.iter().map(|line| format!("{line}\n")).collect::<String>()).unwrap();
    let brain = FakeBrain::new();
    register(&home, &brain, &work);
    let mut client = Client::start(&home, &work);
    let upper = SESSION.to_uppercase();
    let agent = |id: &str, name: &str, call: &str, slot: u64, kind: &str, state: &str, transcript: Value| {
        json!({ "id": id, "name": name, "task": format!("{name}的任务"), "session": SESSION, "parent": null, "call": call, "slot": slot,
                "kind": kind, "depth": 1, "state": state, "started": "2026-10-24T10:01:00Z", "steps": 1,
                "activity": { "kind": "tool", "title": "run_shell" }, "reply": if state == "done" { "甲做完了" } else { "" }, "transcript": transcript })
    };

    let hello = client.send("hello", json!({ "protocol": 1 }));
    let mut link = accept(&brain);
    let list = link.wait_for(10, |message| message["method"] == "sessions.list").unwrap();
    reply(&mut link, &list, json!([]));
    answer_terminals(&mut link);
    let agents = link.wait_for(10, |message| message["method"] == "agents.list").unwrap();
    reply(
        &mut link,
        &agents,
        json!([
            agent(
                "a1",
                "小助手",
                "call_d",
                0,
                "delegate",
                "running",
                json!([
                    { "type": "text", "text": "在查" },
                    { "type": "tool", "call": "c1", "name": "run_shell", "arguments": { "command": "ls" }, "status": "running", "result": "" },
                ])
            ),
            agent("b1", "甲", "call_p", 0, "parallel", "done", json!([{ "type": "text", "text": "甲做完了" }])),
            agent("b2", "乙", "call_p", 1, "parallel", "running", json!([])),
        ]),
    );
    let hello = client.reply(hello)["result"].clone();
    // Agent 树：在跑的两个；id 是它的卡片 id，上级是这段会话
    let runs = hello["state"]["team"]["runs"].as_array().unwrap().clone();
    assert_eq!(runs.len(), 2, "{runs:?}");
    let small = runs.iter().find(|run| run["name"] == "小助手").unwrap().clone();
    assert_eq!((small["parentKey"].as_str(), small["conversationID"].as_str()), (Some(upper.as_str()), Some(upper.as_str())));
    assert_eq!((small["activity"]["kind"].as_str(), small["activity"]["title"].as_str()), (Some("tool"), Some("执行命令")));
    assert!(has(&hello["state"]["runner"]["backgroundAgents"], SESSION));

    // 卡片：delegate_task 就是那个子智能体；并联的卡片里一个子智能体一张子卡片；交回的汇报标 fromAgent
    let opened = client.request("conversation.subscribe", json!({ "id": upper }));
    let nodes = opened["conversation"]["nodes"].as_array().unwrap().clone();
    let cards = tools(&nodes[1]);
    let delegate = &cards[0];
    assert_eq!((delegate["agentName"].as_str(), delegate["status"].as_str()), (Some("小助手"), Some("running")));
    assert_eq!(delegate["id"], small["id"], "卡片 id == Agent 树里的 id");
    let transcript = delegate["transcript"].as_array().unwrap();
    assert_eq!(transcript[0]["kind"]["text"]["_0"], "在查");
    assert_eq!(transcript[1]["kind"]["tool"]["_0"]["name"], "run_shell");
    let parallel = &cards[1];
    assert_eq!((parallel["agentName"].as_str(), parallel["status"].as_str()), (Some("后台 ×2"), Some("running")));
    let children: Vec<Value> = parallel["transcript"].as_array().unwrap().iter().map(|block| block["kind"]["tool"]["_0"].clone()).collect();
    assert_eq!(children.iter().map(|child| child["agentName"].as_str().unwrap()).collect::<Vec<_>>(), ["甲", "乙"]);
    assert_eq!((children[0]["status"].as_str(), children[0]["result"].as_str()), (Some("succeeded"), Some("甲做完了")));
    assert_eq!(nodes[2]["fromAgent"], "甲", "交回的汇报不当用户消息显示");

    // 它接着干：运行记录跟着变（推装着它的那条消息）
    push(
        &mut link,
        SESSION,
        "t0",
        "agent.event",
        json!({ "agent": "a1", "type": "result", "data": { "id": "c1", "name": "run_shell", "ok": true, "text": "a.txt" } }),
    );
    push(&mut link, SESSION, "t0", "agent.event", json!({ "agent": "a1", "type": "text", "data": { "text": "查到了" } }));
    let updated = client.event("message", |params| {
        tools(&params["message"]).first().is_some_and(|card| card["transcript"].as_array().is_some_and(|blocks| blocks.len() == 3))
    });
    let blocks = tools(&updated["params"]["message"])[0]["transcript"].as_array().unwrap().clone();
    assert_eq!(blocks[1]["kind"]["tool"]["_0"]["status"], "succeeded");
    assert_eq!(blocks[2]["kind"]["text"]["_0"], "查到了");

    // 子智能体要批准：卡片在它的运行记录里
    push(
        &mut link,
        SESSION,
        "t0",
        "agent.event",
        json!({ "agent": "a1", "type": "tool", "data": { "id": "c2", "name": "write_file", "arguments": { "path": "x" } } }),
    );
    let approve = link.start("approve", json!({ "session": SESSION, "turn": "t0", "call": "c2", "tool": "write_file", "agent": "a1" }));
    client.event("attention", |_| true);
    let pending = |message: &Value| {
        tools(message)
            .first()
            .and_then(|card| card["transcript"].as_array().cloned())
            .unwrap_or_default()
            .into_iter()
            .map(|block| block["kind"]["tool"]["_0"].clone())
            .find(|tool| tool["status"] == "awaitingApproval")
    };
    let waiting = client.event("message", |params| pending(&params["message"]).is_some());
    let inner = pending(&waiting["params"]["message"]).unwrap();
    client.event("team", |params| params["runs"].as_array().unwrap().iter().any(|run| run["activity"]["kind"] == "waitingApproval"));
    client.request("runner.resolveApproval", json!({ "eventID": inner["id"], "approved": true }));
    assert_eq!(link.reply_to(approve, 10)["result"]["decision"], "allow");
    // 又要批一件，但别的客户端先批了：大脑推 approve.cancel，这边收掉待确认
    push(
        &mut link,
        SESSION,
        "t0",
        "agent.event",
        json!({ "agent": "a1", "type": "tool", "data": { "id": "c3", "name": "run_shell", "arguments": { "command": "ls" } } }),
    );
    let _other = link.start("approve", json!({ "session": SESSION, "turn": "t0", "call": "c3", "tool": "run_shell", "agent": "a1" }));
    client.event("runner", |params| has(&params["pendingApproval"], SESSION));
    link.send(json!({ "jsonrpc": "2.0", "method": "approve.cancel", "params": { "session": SESSION, "call": "c3", "agent": "a1" } }));
    client.event("runner", |params| !has(&params["pendingApproval"], SESSION));
    // 它还没看的追加指示进 Agent 树的队列，能拿掉
    let mut queued = agent("a1", "小助手", "call_d", 0, "delegate", "running", Value::Null);
    queued.as_object_mut().unwrap().remove("transcript");
    queued["inbox"] = json!([{ "id": "m1", "text": "顺便看看 c" }]);
    push(&mut link, SESSION, "t0", "agent", queued);
    let team =
        client.event("team", |params| params["runs"].as_array().unwrap().iter().any(|run| !run["queue"].as_array().unwrap().is_empty()));
    let run = team["params"]["runs"].as_array().unwrap().iter().find(|run| run["name"] == "小助手").unwrap().clone();
    assert_eq!(run["queue"][0]["text"], "顺便看看 c");
    let remove = client.send("agent.removeQueued", json!({ "agentID": run["id"], "itemID": run["queue"][0]["id"] }));
    let request = link.wait_for(10, |message| message["method"] == "agents.dequeue").expect("没让大脑拿掉");
    assert_eq!((request["params"]["id"].as_str(), request["params"]["item"].as_str()), (Some("a1"), Some("m1")));
    reply(&mut link, &request, json!({ "removed": true }));
    assert!(client.reply(remove).get("error").is_none());

    // Agent 树里追加指示、停掉：交给大脑
    let card = small["id"].clone();
    let send = client.send("agent.send", json!({ "agentID": card, "text": "顺便看看 b", "attachments": [] }));
    let request = link.wait_for(10, |message| message["method"] == "agents.send").expect("没交给大脑");
    assert_eq!((request["params"]["id"].as_str(), request["params"]["text"].as_str()), (Some("a1"), Some("顺便看看 b")));
    reply(&mut link, &request, json!({ "name": "小助手" }));
    assert!(client.reply(send).get("error").is_none());
    let stop = client.send("agent.stop", json!({ "agentID": card }));
    let request = link.wait_for(10, |message| message["method"] == "agents.stop").expect("没让大脑停");
    assert_eq!(request["params"]["id"], "a1");
    reply(&mut link, &request, json!({ "name": "小助手" }));
    assert!(client.reply(stop).get("error").is_none());
    // 停了：卡片标已停止、Agent 树里没有它了
    let mut stopped = agent("a1", "小助手", "call_d", 0, "delegate", "stopped", Value::Null);
    stopped.as_object_mut().unwrap().remove("transcript");
    push(&mut link, SESSION, "t0", "agent", stopped);
    client.event("team", |params| !params["runs"].as_array().unwrap().iter().any(|run| run["name"] == "小助手"));
    client.event("message", |params| {
        tools(&params["message"]).first().is_some_and(|card| card["stopped"] == true && card["status"] == "failed")
    });

    // 会话改动：交给大脑
    let changes = client.send("fs.changes", json!({ "conversationID": upper }));
    let request = link.wait_for(10, |message| message["method"] == "changes.list").expect("没问大脑改动");
    assert_eq!(request["params"]["session"], SESSION);
    let files = json!({ "files": [{ "path": "/w/a.txt", "status": "M", "known": true, "text": true, "added": 1, "removed": 0 }] });
    reply(&mut link, &request, files.clone());
    assert_eq!(client.reply(changes)["result"], files);
    let diff = client.send("fs.diff", json!({ "conversationID": upper, "path": "/w/a.txt" }));
    let request = link.wait_for(10, |message| message["method"] == "changes.diff").unwrap();
    assert_eq!(request["params"]["path"], "/w/a.txt");
    reply(
        &mut link,
        &request,
        json!({ "path": "/w/a.txt", "status": "M", "known": true, "binary": false, "tooLarge": false, "old": "1\n", "new": "1\n2\n" }),
    );
    assert_eq!(client.reply(diff)["result"]["new"], "1\n2\n");
    let revert = client.send("fs.revert", json!({ "conversationID": upper }));
    let request = link.wait_for(10, |message| message["method"] == "changes.revert").unwrap();
    assert!(request["params"].get("path").is_none());
    reply(&mut link, &request, json!({ "failed": [] }));
    assert_eq!(client.reply(revert)["result"]["failed"], json!([]));
    let _ = std::fs::remove_dir_all(&home);
}

/// 存档里的历史（同 EDITH 的只追加格式：reset 之前的作废）
fn read_history(home: &Path, session: &str) -> Vec<Value> {
    let text = std::fs::read_to_string(sessions_dir(home).join(format!("{session}.jsonl"))).unwrap();
    let mut history = Vec::new();
    for line in text.lines() {
        let entry: Value = serde_json::from_str(line).unwrap();
        match entry["type"].as_str() {
            Some("message") => history.push(entry["message"].clone()),
            Some("reset") => history.clear(),
            _ => {}
        }
    }
    history
}

/// 替大脑写一整段会话（分叉、藏起来的另一支）
fn write_history(home: &Path, work: &Path, session: &str, owner: &str, history: &[Value]) {
    let mut lines = vec![
        json!({ "type": "meta", "id": session, "cwd": work.to_string_lossy(), "created": "2026-10-24T10:00:00+08:00", "owner": owner }),
        json!({ "type": "title", "title": "写个文件" }),
    ];
    lines.extend(history.iter().map(|message| json!({ "type": "message", "message": message })));
    let text: String = lines.iter().map(|line| format!("{line}\n")).collect();
    std::fs::write(sessions_dir(home).join(format!("{session}.jsonl")), text).unwrap();
}

/// 大脑做完一轮：开始 → 存档追加 stored → 做完，等整段重发（最后一条是 last）
fn finish_turn(client: &mut Client, link: &mut Link, home: &Path, turn: &str, stored: &[Value], end: Value, last: &str) -> Value {
    client.drain();
    // 这一轮交来的那句：存档里这一轮的第一条
    push(link, SESSION, turn, "turn.started", json!({ "text": stored[0]["content"] }));
    client.event("runner", |params| has(&params["generating"], SESSION));
    append(home, SESSION, stored);
    push(link, SESSION, turn, "done", end);
    client.event("conversation", |params| {
        params["conversation"]["nodes"].as_array().unwrap().last().is_some_and(|node| {
            node["stats"].get("live").is_none() && (node["blocks"][0]["kind"]["text"]["_0"] == last || node["error"] == last)
        })
    })
}

fn texts(conversation: &Value) -> Vec<String> {
    // 选中的那条路上用户说的
    let nodes = conversation["nodes"].as_array().unwrap();
    let mut out = Vec::new();
    let mut key = "00000000-0000-0000-0000-000000000000".to_string();
    while let Some(child) = conversation["selectedChildren"][&key].as_str() {
        let node = nodes.iter().find(|node| node["id"] == child).unwrap();
        if node["role"] == "user" {
            out.push(node["blocks"][0]["kind"]["text"]["_0"].as_str().unwrap().to_string());
        }
        key = child.to_string();
    }
    out
}

#[test]
fn errors_retry_branches_and_fork() {
    let home = scratch("friday-branches");
    let work = home.join("work");
    std::fs::create_dir_all(work.join(".git")).unwrap();
    write_session(&home, &work);
    let brain = FakeBrain::new();
    register(&home, &brain, &work);
    let mut client = Client::start(&home, &work);
    let hello = client.send("hello", json!({ "protocol": 1, "client": { "name": "test" } }));
    let mut link = accept(&brain);
    let list = link.wait_for(10, |message| message["method"] == "sessions.list").unwrap();
    reply(&mut link, &list, json!([]));
    answer_terminals(&mut link);
    let agents = link.wait_for(10, |message| message["method"] == "agents.list").unwrap();
    reply(&mut link, &agents, json!([]));
    client.reply(hello);
    client.request("conversation.subscribe", json!({ "id": SESSION }));

    // 一轮出错：最后一条回复带错误，能重试（failedMessageID 是它）
    let stored = [json!({ "role": "user", "content": "再来" })];
    let end = json!({ "answer": "", "stopped": false, "error": "HTTP 400：坏了" });
    let done = finish_turn(&mut client, &mut link, &home, "t1", &stored, end, "HTTP 400：坏了");
    let failed = nodes(&done).last().unwrap()["id"].clone();
    let opened = client.request("conversation.subscribe", json!({ "id": SESSION }));
    assert_eq!((opened["runtime"]["failedMessageID"].clone(), opened["runtime"]["canRetry"].clone()), (failed.clone(), json!(true)));

    // 重试：截到这一轮那句之前、再发一遍
    let retry = client.send("runner.retry", json!({ "conversationID": SESSION }));
    let truncate = link.wait_for(10, |message| message["method"] == "sessions.truncate").expect("重试没截短");
    assert_eq!(truncate["params"]["keep"], 10, "{truncate}");
    let history = read_history(&home, SESSION);
    write_history(&home, &work, SESSION, "", &history[..10]);
    reply(&mut link, &truncate, json!({}));
    let sense = link.wait_for(10, |message| message["method"] == "sense").expect("重试没再发");
    assert_eq!(sense["params"]["text"], "再来");
    reply(&mut link, &sense, json!({ "disposition": "started", "turn": "t2" }));
    client.reply(retry);
    let stored = [json!({ "role": "user", "content": "再来" }), json!({ "role": "assistant", "content": "这次好了" })];
    let end = json!({ "answer": "这次好了", "stopped": false, "error": null });
    let done = finish_turn(&mut client, &mut link, &home, "t2", &stored, end, "这次好了");
    assert_eq!(nodes(&done).last().unwrap()["id"], failed, "重做的回复用出错那条的 id");
    assert!(nodes(&done).last().unwrap().get("error").is_none());

    // 改第二句重发：整段藏成另一支，主干截到那句之前
    let second = nodes(&done)[2]["id"].clone();
    let submit =
        client.send("runner.submit", json!({ "conversationID": SESSION, "text": "换个说法", "attachments": [], "replacing": second }));
    let fork = link.wait_for(10, |message| message["method"] == "sessions.fork").expect("没存另一支");
    assert_eq!(fork["params"]["hidden"], true);
    let archive = "9a8b7c6d-5e4f-4a3b-8c2d-1e0f9a8b7c6d";
    let full = read_history(&home, SESSION);
    write_history(&home, &work, archive, "#branch", &full);
    reply(&mut link, &fork, json!({ "id": archive }));
    let truncate = link.wait_for(10, |message| message["method"] == "sessions.truncate").unwrap();
    assert_eq!(truncate["params"]["keep"], 8, "截到第二句之前：{truncate}");
    write_history(&home, &work, SESSION, "", &full[..8]);
    reply(&mut link, &truncate, json!({}));
    let sense = link.wait_for(10, |message| message["method"] == "sense").unwrap();
    reply(&mut link, &sense, json!({ "disposition": "started", "turn": "t3" }));
    client.reply(submit);
    let stored = [json!({ "role": "user", "content": "换个说法" }), json!({ "role": "assistant", "content": "收到" })];
    let end = json!({ "answer": "收到", "stopped": false, "error": null });
    finish_turn(&mut client, &mut link, &home, "t3", &stored, end, "收到");
    let opened = client.request("conversation.subscribe", json!({ "id": SESSION }));
    let conversation = &opened["conversation"];
    assert_eq!(texts(conversation), ["帮我写个文件", "换个说法"]);
    let all = conversation["nodes"].as_array().unwrap();
    let parent = all[1]["id"].clone();
    let siblings: Vec<&Value> = all.iter().filter(|node| node["parentID"] == parent).collect();
    assert_eq!(siblings.len(), 2, "第二句那里两支");
    assert_eq!(siblings[0]["blocks"][0]["kind"]["text"]["_0"], "顺便看看 b.txt", "旧的在前");
    // 藏起来的那支不在会话列表里
    let summaries = client.request("hello", json!({ "protocol": 1 }))["state"]["summaries"].clone();
    assert_eq!(summaries.as_array().unwrap().len(), 1);

    // 切回旧的那支：主干存成另一支，旧的换上来
    let old = siblings[0]["id"].clone();
    let select = client.send("runner.selectBranch", json!({ "conversationID": SESSION, "child": old, "parentKey": parent }));
    let fork = link.wait_for(10, |message| message["method"] == "sessions.fork").unwrap();
    let swapped = "1b2c3d4e-5f6a-4b7c-8d9e-0f1a2b3c4d5e";
    write_history(&home, &work, swapped, "#branch", &read_history(&home, SESSION));
    reply(&mut link, &fork, json!({ "id": swapped }));
    let copy = link.wait_for(10, |message| message["method"] == "sessions.copy").unwrap();
    assert_eq!(copy["params"]["from"], archive);
    write_history(&home, &work, SESSION, "", &full);
    reply(&mut link, &copy, json!({}));
    let remove = link.wait_for(10, |message| message["method"] == "sessions.remove").unwrap();
    assert_eq!(remove["params"]["id"], archive);
    std::fs::remove_file(sessions_dir(&home).join(format!("{archive}.jsonl"))).unwrap();
    reply(&mut link, &remove, json!({ "removed": true }));
    client.reply(select);
    let opened = client.request("conversation.subscribe", json!({ "id": SESSION }));
    assert_eq!(texts(&opened["conversation"]), ["帮我写个文件", "顺便看看 b.txt", "再来"]);
    let siblings: Vec<String> = opened["conversation"]["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|node| node["parentID"] == parent)
        .map(|node| node["blocks"][0]["kind"]["text"]["_0"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(siblings, ["顺便看看 b.txt", "换个说法"], "切了顺序不变");

    // 从第一条回复分叉
    let first = opened["conversation"]["nodes"][1]["id"].clone();
    let new = "2c3d4e5f-6a7b-4c8d-9e0f-1a2b3c4d5e6f";
    let request = client.send("conversation.fork", json!({ "id": SESSION, "messageID": first, "newID": new.to_uppercase() }));
    let fork = link.wait_for(10, |message| message["method"] == "sessions.fork").unwrap();
    assert_eq!((fork["params"]["keep"].clone(), fork["params"]["newID"].clone()), (json!(8), json!(new)));
    write_history(&home, &work, new, "", &full[..8]);
    reply(&mut link, &fork, json!({ "id": new }));
    assert_eq!(client.reply(request)["result"]["id"], new.to_uppercase());
    let listed = client.event("summaries", |params| params["summaries"].as_array().unwrap().len() == 2);
    let forked = listed["params"]["summaries"].as_array().unwrap().iter().find(|item| item["id"] == new.to_uppercase()).unwrap().clone();
    assert_eq!(forked["title"], "写个文件（fork）");
}

#[test]
fn models_secrets_and_turn_settings() {
    let home = scratch("friday-models");
    let work = home.join("work");
    std::fs::create_dir_all(work.join(".git")).unwrap();
    write_session(&home, &work);
    let second = json!({ "id": "second", "baseURL": "https://second.example/v1", "model": "m2", "apiKeySecret": "SECOND_KEY", "thinking": "effort" });
    std::fs::write(home.join("config.json"), json!({ "model": "m1", "baseURL": "http://127.0.0.1:1/v1", "engines": [second] }).to_string())
        .unwrap();
    let brain = FakeBrain::new();
    register(&home, &brain, &work);
    let mut client = Client::start(&home, &work);
    let hello = client.send("hello", json!({ "protocol": 1 }));
    let mut link = accept(&brain);
    let list = link.wait_for(10, |message| message["method"] == "sessions.list").unwrap();
    reply(&mut link, &list, json!([]));
    answer_terminals(&mut link);
    let agents = link.wait_for(10, |message| message["method"] == "agents.list").unwrap();
    reply(&mut link, &agents, json!([]));
    let hello = client.reply(hello)["result"].clone();

    // 模型：顶层那个（edith，本机的不分档）+ engines 里的；默认第一个
    let engines = &hello["state"]["engines"];
    let ids: Vec<&str> = engines["engines"].as_array().unwrap().iter().map(|engine| engine["id"].as_str().unwrap()).collect();
    assert_eq!((ids, engines["defaultEngineID"].as_str()), (vec!["edith", "second"], Some("edith")));
    assert_eq!(engines["engines"][0]["thinking"], "none");
    // 密钥只记名字、值写进 secrets.json；加、删模型改 config.json（别的键不动）
    client.request("secrets.set", json!({ "name": "SECOND_KEY", "value": "k" }));
    assert_eq!(client.event("secrets", |_| true)["params"]["names"], json!(["SECOND_KEY"]));
    let secrets: Value = serde_json::from_slice(&std::fs::read(home.join("secrets.json")).unwrap()).unwrap();
    assert_eq!(secrets["SECOND_KEY"], "k");
    client.request("engines.update", json!({ "engine": { "id": "third", "baseURL": "https://third.example/v1", "model": "m3" } }));
    client.event("engines", |params| params["engines"].as_array().unwrap().len() == 3);
    client.request("engines.remove", json!({ "id": "third" }));
    client.event("engines", |params| params["engines"].as_array().unwrap().len() == 2);
    let config: Value = serde_json::from_slice(&std::fs::read(home.join("config.json")).unwrap()).unwrap();
    assert_eq!((config["model"].as_str(), config["engines"].as_array().unwrap().len()), (Some("m1"), 1));
    let id = client.send("engines.remove", json!({ "id": "edith" }));
    assert_eq!(client.reply(id)["error"]["code"], 4);

    // 会话上选的权限模式、模型、思考档位随话交给大脑；附件里的图片单独列出（模型能看图时给它）
    client.request("runner.setPermissionMode", json!({ "conversationID": SESSION, "mode": "full" }));
    client.request("runner.setEngine", json!({ "conversationID": SESSION, "engineID": "second" }));
    client.request("runner.setThinking", json!({ "conversationID": SESSION, "level": "high" }));
    let opened = client.request("conversation.subscribe", json!({ "id": SESSION }));
    assert_eq!(opened["runtime"]["engine"]["id"], "second");
    assert_eq!(opened["runtime"]["thinkingLevels"], json!(["off", "low", "medium", "high"]));
    let shot = work.join("图.png").to_string_lossy().into_owned();
    let notes = work.join("说明.txt").to_string_lossy().into_owned();
    let submit = client.send("runner.submit", json!({ "conversationID": SESSION, "text": "看看", "attachments": [shot, notes] }));
    let sense = link.wait_for(10, |message| message["method"] == "sense").unwrap();
    let params = &sense["params"];
    assert_eq!(
        (params["mode"].as_str(), params["engine"].as_str(), params["thinking"].as_str()),
        (Some("full"), Some("second"), Some("high"))
    );
    assert_eq!(params["images"], json!([shot]));
    reply(&mut link, &sense, json!({ "disposition": "started", "turn": "t1" }));
    client.reply(submit);
}

#[test]
fn activity_for_the_assistant() {
    let home = scratch("friday-activity");
    let work = home.join("work");
    std::fs::create_dir_all(work.join(".git")).unwrap();
    write_session(&home, &work);
    let brain = FakeBrain::new();
    register(&home, &brain, &work);
    let mut client = Client::start(&home, &work);
    let hello = client.send("hello", json!({ "protocol": 1 }));
    let mut link = accept(&brain);
    let list = link.wait_for(10, |message| message["method"] == "sessions.list").unwrap();
    reply(&mut link, &list, json!([]));
    answer_terminals(&mut link);
    let agents = link.wait_for(10, |message| message["method"] == "agents.list").unwrap();
    reply(&mut link, &agents, json!([]));
    let hello = client.reply(hello)["result"].clone();
    assert!(hello["server"]["features"].as_array().unwrap().contains(&json!("activity")), "助手那一行不再说版本太旧");
    assert_eq!(client.request("assistant.status", json!({}))["enabled"], false);

    // 快照：有内容的会话现在的样子
    let snapshot = client.request("activity.subscribe", json!({}));
    let sessions = snapshot["sessions"].as_array().unwrap();
    assert_eq!((sessions.len(), sessions[0]["status"].as_str(), sessions[0]["title"].as_str()), (1, Some("idle"), Some("写个文件")));
    assert_eq!(snapshot["assistant"], false);

    // 一轮：开始 → 跑命令（值得一提的工具）→ 要批准 → 批了 → 做完带简报
    let activity = |client: &mut Client, kind: &str| {
        let kind = kind.to_string();
        client.event("activity", move |params| params["kind"] == json!(kind))["params"].clone()
    };
    push(&mut link, SESSION, "t1", "turn.started", json!({ "text": "跑一下测试", "origin": { "kind": "user" } }));
    let started = activity(&mut client, "started");
    assert_eq!((started["by"].as_str(), started["request"].as_str()), (Some("user"), Some("跑一下测试")));
    assert_eq!(started["conversationID"], SESSION.to_uppercase());
    push(&mut link, SESSION, "t1", "tool", json!({ "id": "c1", "name": "run_shell", "arguments": { "command": "cargo test" } }));
    let tool = activity(&mut client, "tool");
    assert_eq!((tool["tool"]["tool"].as_str(), tool["tool"]["summary"].as_str()), (Some("run_shell"), Some("cargo test")));
    let approve = link
        .start("approve", json!({ "session": SESSION, "turn": "t1", "call": "c1", "tool": "run_shell", "summary": "跑命令：cargo test" }));
    let asked = activity(&mut client, "approval");
    let pending = asked["approvals"][0].clone();
    assert_eq!((pending["tool"].as_str(), pending["summary"].as_str()), (Some("run_shell"), Some("跑命令：cargo test")));
    assert_eq!(pending["eventID"], tool["tool"]["eventID"], "待确认的就是那张卡片");
    client.request("runner.resolveApproval", json!({ "eventID": pending["eventID"], "approved": true }));
    let answered = activity(&mut client, "approval");
    assert_eq!((answered["approvals"].clone(), answered["resolved"][0]["outcome"].clone()), (json!([]), json!("approved")));
    assert_eq!(link.reply_to(approve, 10)["result"]["decision"], "allow");
    push(&mut link, SESSION, "t1", "result", json!({ "id": "c1", "name": "run_shell", "ok": false, "text": "1 个没过" }));
    append(
        &home,
        SESSION,
        &[
            json!({ "role": "user", "content": "跑一下测试" }),
            json!({ "role": "assistant", "content": "**跑了**测试。有一个没过，要我修吗？" }),
        ],
    );
    push(&mut link, SESSION, "t1", "done", json!({ "answer": "**跑了**测试。有一个没过，要我修吗？", "stopped": false, "error": null }));
    let finished = activity(&mut client, "finished");
    let brief = &finished["brief"];
    assert_eq!(
        (brief["status"].as_str(), brief["commands"].as_i64(), brief["commandsFailed"].as_i64()),
        (Some("question"), Some(1), Some(1))
    );
    assert_eq!(
        (brief["question"].as_str(), brief["answer"].as_str()),
        (Some("有一个没过，要我修吗？"), Some("跑了测试。有一个没过，要我修吗？"))
    );
    let snapshot = client.request("activity.subscribe", json!({}));
    assert_eq!(snapshot["sessions"][0]["status"], "question");
    assert_eq!(snapshot["sessions"][0]["lastBrief"]["status"], "question");
}

#[test]
fn settings_mcp_and_memory() {
    let home = scratch("friday-settings");
    let work = home.join("work");
    std::fs::create_dir_all(&work).unwrap();
    let mut client = Client::start(&home, &work);
    let hello = client.request("hello", json!({ "protocol": 1 }));
    let features = hello["server"]["features"].as_array().unwrap().clone();
    assert!(["config", "memory", "memorySearch"].iter().all(|feature| features.contains(&json!(feature))), "{features:?}");

    // 设置 › 智能体：改 EDITH 的 config.json（别的键留着）
    std::fs::write(home.join("config.json"), json!({ "model": "m1", "apiKey": "k" }).to_string()).unwrap();
    let opened = client.request("config.subscribe", json!({}));
    assert_eq!(
        (opened["settings"]["requireApproval"].as_bool(), opened["settings"]["defaultPermissionMode"].as_str()),
        (Some(true), Some("auto"))
    );
    let updated =
        client.request("settings.update", json!({ "systemPrompt": "多用表格", "requireApproval": false, "defaultPermissionMode": "full" }));
    assert_eq!(updated["requireApproval"], false);
    let config: Value = serde_json::from_slice(&std::fs::read(home.join("config.json")).unwrap()).unwrap();
    assert_eq!(
        (config["systemPrompt"].as_str(), config["permissions"]["mode"].as_str(), config["apiKey"].as_str()),
        (Some("多用表格"), Some("auto"), Some("k"))
    );
    client.event("config", |params| params["settings"]["systemPrompt"] == "多用表格");
    client.request("tools.setEnabled", json!({ "name": "web_search", "enabled": false }));
    let config: Value = serde_json::from_slice(&std::fs::read(home.join("config.json")).unwrap()).unwrap();
    assert_eq!(config["disabledTools"], json!(["web_search"]));

    // MCP：存、改名、贴、删，都在 EDITH 全局那层的 mcp.json
    let saved = client.request("mcp.saveServer", json!({ "name": "a", "config": { "command": "echo" } }));
    assert_eq!(saved["error"], "");
    client.request("mcp.saveServer", json!({ "name": "b", "oldName": "a", "config": { "command": "echo" } }));
    let pasted = client.request("mcp.importText", json!({ "text": "{\"mcpServers\": {\"c\": {\"url\": \"https://x.example/mcp\"}}}" }));
    assert_eq!((pasted["ok"].as_bool(), pasted["names"].clone()), (Some(true), json!(["c"])));
    let servers: Vec<String> =
        pasted["mcp"]["servers"].as_array().unwrap().iter().map(|server| server["name"].as_str().unwrap().to_string()).collect();
    assert_eq!(servers, ["b", "c"]);
    let bad = client.request("mcp.importText", json!({ "text": "不是 JSON" }));
    assert_eq!(bad["ok"], false);

    // 记住的事：加、改、常驻、查、归档、恢复、删（EDITH 的 memory.json）
    let state = client.request("memory.get", json!({ "recent": 20 }));
    assert_eq!(state["count"], 0);
    let state = client.request("memory.add", json!({ "text": "回复用中文" }));
    let id = state["items"][0]["id"].as_str().unwrap().to_string();
    assert_eq!((state["items"][0]["kind"].as_str(), state["items"][0]["kindLabel"].as_str()), (Some("preference"), Some("偏好")));
    client.event("memory", |params| params["count"] == 1);
    client.request("memory.setText", json!({ "id": id, "text": "回复一律用中文" }));
    client.request("memory.setPinned", json!({ "id": id, "pinned": true }));
    let found = client.request("memory.search", json!({ "query": "中文" }));
    assert_eq!((found["items"][0]["text"].as_str(), found["items"][0]["pinned"].as_bool()), (Some("回复一律用中文"), Some(true)));
    let state = client.request("memory.archive", json!({ "id": id }));
    assert_eq!((state["count"].as_u64(), state["archivedCount"].as_u64()), (Some(0), Some(1)));
    client.request("memory.restore", json!({ "id": id }));
    let state = client.request("memory.remove", json!({ "id": id }));
    assert_eq!(state["count"], 0);
    let missing = client.send("memory.remove", json!({ "id": id }));
    assert_eq!(client.reply(missing)["error"]["code"], 3);
}

#[test]
fn terminals_live_in_the_brain() {
    let home = scratch("friday-terminals");
    let work = home.join("work");
    std::fs::create_dir_all(work.join(".git")).unwrap();
    write_session(&home, &work);
    let brain = FakeBrain::new();
    register(&home, &brain, &work);
    let mut client = Client::start(&home, &work);
    let hello = client.send("hello", json!({ "protocol": 1 }));
    let mut link = accept(&brain);
    let list = link.wait_for(10, |message| message["method"] == "sessions.list").unwrap();
    reply(&mut link, &list, json!([]));
    // 连上来问它开着哪些终端（之前开的、客户端断了还在跑的）
    answer_browser(&mut link);
    let terminals = link.wait_for(10, |message| message["method"] == "terminals.list").unwrap();
    let running = json!({ "id": "T0", "conversationID": SESSION, "title": "zsh", "shell": "zsh", "running": true, "cols": 80, "rows": 24 });
    reply(&mut link, &terminals, json!({ "terminals": [running] }));
    let agents = link.wait_for(10, |message| message["method"] == "agents.list").unwrap();
    reply(&mut link, &agents, json!([]));
    let hello = client.reply(hello)["result"].clone();
    let listed = &hello["state"]["terminals"];
    assert_eq!(
        (listed["available"].as_bool(), listed["terminals"][0]["conversationID"].clone()),
        (Some(true), json!(SESSION.to_uppercase()))
    );

    // 开一个：交给会话所在目录的大脑
    let open = client.send("term.open", json!({ "id": "T1", "conversationID": SESSION, "cols": 90, "rows": 20, "command": "ls" }));
    let asked = link.wait_for(10, |message| message["method"] == "terminals.open").unwrap();
    assert_eq!(
        (asked["params"]["session"].as_str(), asked["params"]["command"].as_str(), asked["params"]["cols"].as_u64()),
        (Some(SESSION), Some("ls"), Some(90))
    );
    let info = json!({ "id": "T1", "conversationID": SESSION, "title": "zsh", "shell": "zsh", "running": true, "cols": 90, "rows": 20 });
    reply(&mut link, &asked, info);
    client.reply(open);
    client.event("terminals", |params| params["terminals"].as_array().unwrap().len() == 2);
    // 输出原样转；接上、写、改大小、关都转给那个大脑
    link.send(json!({ "jsonrpc": "2.0", "method": "terminal.output", "params": { "id": "T1", "data": "aGk=" } }));
    assert_eq!(client.event("term.output", |params| params["id"] == "T1")["params"]["data"], "aGk=");
    let attach = client.send("term.attach", json!({ "id": "T0" }));
    let asked = link.wait_for(10, |message| message["method"] == "terminals.attach").unwrap();
    reply(&mut link, &asked, json!({ "data": "b2xk", "cols": 80, "rows": 24 }));
    assert_eq!(client.reply(attach)["result"]["data"], "b2xk");
    let write = client.send("term.write", json!({ "id": "T1", "data": "bHMK" }));
    let asked = link.wait_for(10, |message| message["method"] == "terminals.write").unwrap();
    assert_eq!(asked["params"]["data"], "bHMK");
    reply(&mut link, &asked, json!({}));
    client.reply(write);
    // 大脑说关了：列表跟着变；没有的终端回 3
    link.send(json!({ "jsonrpc": "2.0", "method": "terminals", "params": { "terminals": [running] } }));
    client.event("terminals", |params| params["terminals"].as_array().unwrap().len() == 1);
    let missing = client.send("term.write", json!({ "id": "T1", "data": "" }));
    assert_eq!(client.reply(missing)["error"]["code"], 3);
}

#[test]
fn browser_tabs_live_in_the_brain() {
    let home = scratch("friday-browser");
    let work = home.join("work");
    std::fs::create_dir_all(work.join(".git")).unwrap();
    write_session(&home, &work);
    let brain = FakeBrain::new();
    register(&home, &brain, &work);
    let mut client = Client::start(&home, &work);
    let hello = client.send("hello", json!({ "protocol": 1 }));
    let mut link = accept(&brain);
    let list = link.wait_for(10, |message| message["method"] == "sessions.list").unwrap();
    reply(&mut link, &list, json!([]));
    // 连上来问它开着哪些标签（客户端断了还在的）
    let browser = link.wait_for(10, |message| message["method"] == "browser.state").unwrap();
    let old = json!({ "id": "B0", "conversationID": SESSION, "title": "旧的", "url": "https://a.example/", "loading": false, "current": true, "viewport": [1280, 800], "fixed": false });
    reply(&mut link, &browser, json!({ "available": true, "tabs": [old] }));
    let terminals = link.wait_for(10, |message| message["method"] == "terminals.list").unwrap();
    reply(&mut link, &terminals, json!({ "terminals": [] }));
    let agents = link.wait_for(10, |message| message["method"] == "agents.list").unwrap();
    reply(&mut link, &agents, json!([]));
    let hello = client.reply(hello)["result"].clone();
    let state = &hello["state"]["browser"];
    assert_eq!((state["available"].as_bool(), state["tabs"][0]["conversationID"].clone()), (Some(true), json!(SESSION.to_uppercase())));

    // 开一个：交给会话所在目录的大脑；之后的请求按标签转给它（刚开的还没等到 browser 通知也行）
    let open = client.send("browser.open", json!({ "conversationID": SESSION, "url": "example.com" }));
    let asked = link.wait_for(10, |message| message["method"] == "browser.open").unwrap();
    assert_eq!((asked["params"]["session"].as_str(), asked["params"]["url"].as_str()), (Some(SESSION), Some("example.com")));
    reply(&mut link, &asked, json!({ "tab": "B1" }));
    assert_eq!(client.reply(open)["result"]["tab"], "B1");
    let watch = client.send("browser.watch", json!({ "tab": "B1", "width": 800, "height": 600 }));
    let asked = link.wait_for(10, |message| message["method"] == "browser.watch").unwrap();
    assert_eq!((asked["params"]["tab"].as_str(), asked["params"]["width"].as_u64()), (Some("B1"), Some(800)));
    reply(&mut link, &asked, json!({}));
    client.reply(watch);
    // 画面原样转；标签变了推 browser
    link.send(json!({ "jsonrpc": "2.0", "method": "browser.frame", "params": { "tab": "B1", "data": "/9j/", "viewport": [800, 600] } }));
    assert_eq!(client.event("browser.frame", |params| params["tab"] == "B1")["params"]["data"], "/9j/");
    let new = json!({ "id": "B1", "conversationID": SESSION, "title": "Example", "url": "https://example.com/", "loading": false, "current": true, "viewport": [800, 600], "fixed": false });
    link.send(json!({ "jsonrpc": "2.0", "method": "browser", "params": { "available": true, "tabs": [old, new] } }));
    client.event("browser", |params| params["tabs"].as_array().unwrap().len() == 2);
    let input = client.send("browser.input", json!({ "tab": "B0", "kind": "text", "text": "你好" }));
    let asked = link.wait_for(10, |message| message["method"] == "browser.input").unwrap();
    assert_eq!(asked["params"]["text"], "你好");
    reply(&mut link, &asked, json!({}));
    client.reply(input);
    // 没有的标签不理
    let unknown = client.send("browser.select", json!({ "tab": "nope" }));
    assert!(client.reply(unknown).get("error").is_none());
}
