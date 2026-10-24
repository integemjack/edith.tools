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
    push(&mut link, SESSION, "t1", "result", json!({ "id": "call_0", "name": "write_file", "ok": true, "text": "已写入" }));

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
    reply(&mut link, &sense, json!({ "session": SESSION, "disposition": "queued", "turn": null }));
    assert!(client.reply(queued).get("error").is_none());
    client.event("runtime", |params| params["runtime"]["queue"].as_array().unwrap().iter().any(|item| item["text"] == "排着的"));

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

    // 删：停下、删存档
    let remove = client.send("conversation.remove", json!({ "id": upper }));
    if let Some(request) = link.wait_for(10, |message| message["method"] == "sessions.stop") {
        reply(&mut link, &request, json!({ "stopped": false }));
    }
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
