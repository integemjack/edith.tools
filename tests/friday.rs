//! edith-friday：测试一头扮 Friday 客户端（标准输入 / 输出讲 Friday 远程协议），一头扮 EDITH 大脑（tests/common 的假套接字，登记在 instances/ 里）

mod common;

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};

use common::{FakeBrain, Link, scratch};
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

#[test]
fn friday_client_through_gateway() {
    let home = scratch("friday");
    let work = home.join("work");
    std::fs::create_dir_all(&work).unwrap();
    write_session(&home, &work);
    let brain = FakeBrain::new();
    register(&home, &brain, &work);
    let mut client = Client::start(&home, &work);

    // 没 hello 不行；协议版本不对不行
    let id = client.send("conversation.subscribe", json!({ "id": SESSION }));
    assert_eq!(client.reply(id)["error"]["code"], 2);
    let id = client.send("hello", json!({ "protocol": 99 }));
    assert_eq!(client.reply(id)["error"]["code"], 1);

    let hello = client.request("hello", json!({ "protocol": 1, "client": { "name": "test" } }));
    assert_eq!(hello["protocol"], 1);
    assert_eq!(hello["server"]["version"], "0.0.0", "不让客户端自动升级");
    let summaries = hello["state"]["summaries"].as_array().unwrap();
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0]["id"], SESSION.to_uppercase());
    assert_eq!(summaries[0]["title"], "写个文件");
    assert_eq!(summaries[0]["isEmpty"], false);
    assert_eq!(hello["state"]["engines"]["defaultEngineID"], "edith");

    // 存档 → Friday 的消息：用户 / 助手 / 用户（补充，去掉来源说明）/ 助手；交付检查的提醒记成提示、不当用户消息
    let opened = client.request("conversation.subscribe", json!({ "id": SESSION.to_uppercase() }));
    let nodes = opened["conversation"]["nodes"].as_array().unwrap().clone();
    let roles: Vec<&str> = nodes.iter().map(|node| node["role"].as_str().unwrap()).collect();
    assert_eq!(roles, ["user", "assistant", "user", "assistant"]);
    assert_eq!(nodes[2]["blocks"][0]["kind"]["text"]["_0"], "顺便看看 b.txt");
    assert_eq!(kinds(&nodes[1]), ["thinking", "text", "tool", "text", "tool", "text"]);
    let cards = tools(&nodes[1]);
    assert_eq!(cards[0]["status"], "succeeded");
    assert_eq!(cards[0]["title"], "写入文件");
    assert_eq!(cards[1]["status"], "denied");
    assert_ne!(cards[0]["id"], cards[1]["id"], "两轮都叫 call_0 也不能撞");
    assert!(nodes[1]["notice"].as_str().unwrap().contains("交付检查"));
    assert_eq!(nodes[1]["parentID"], nodes[0]["id"]);
    assert_eq!(opened["conversation"]["selectedChildren"]["00000000-0000-0000-0000-000000000000"], nodes[0]["id"]);
    assert_eq!(opened["runtime"]["generating"], false);

    // 发话：网关连上登记着的大脑，交过去（用户自己打的、忙时补充）
    let submit = client.send("runner.submit", json!({ "conversationID": SESSION.to_uppercase(), "text": "再写一个", "attachments": [] }));
    let mut link = accept(&brain);
    let sense = link.wait_for(10, |message| message["method"] == "sense").expect("没交给大脑");
    assert_eq!(sense["params"]["session"], SESSION);
    assert_eq!(sense["params"]["text"], "再写一个");
    assert_eq!(sense["params"]["kind"], "user");
    assert_eq!(sense["params"]["urgency"], "interject");
    link.send(json!({ "jsonrpc": "2.0", "id": sense["id"], "result": { "session": SESSION, "disposition": "started", "turn": "t1" } }));
    assert!(client.reply(submit).get("error").is_none());

    // 大脑推来这一轮：开始 → 字 → 工具 → 要批准
    let push = |link: &mut Link, kind: &str, data: Value| {
        link.send(
            json!({ "jsonrpc": "2.0", "method": "event", "params": { "session": SESSION, "turn": "t1", "type": kind, "data": data } }),
        );
    };
    push(&mut link, "turn.started", json!({ "text": "再写一个", "origin": { "capability": "friday", "kind": "user" } }));
    client.event("runner", |params| params["generating"].as_array().unwrap().iter().any(|id| id == &json!(SESSION.to_uppercase())));
    let live = client.event("conversation", |params| params["conversation"]["nodes"].as_array().unwrap().len() == 6);
    let live_nodes = live["params"]["conversation"]["nodes"].as_array().unwrap().clone();
    assert_eq!(live_nodes[4]["blocks"][0]["kind"]["text"]["_0"], "再写一个");
    assert_eq!(live_nodes[5]["stats"]["live"], true);
    push(&mut link, "text", json!({ "text": "好的，" }));
    push(&mut link, "tool", json!({ "id": "call_0", "name": "write_file", "arguments": { "path": "b.txt", "content": "y" } }));
    let approve = link
        .start("approve", json!({ "session": SESSION, "turn": "t1", "call": "call_0", "tool": "write_file", "summary": "写文件：b.txt" }));
    client.event("attention", |_| true);
    let waiting = client.event("message", |params| tools(&params["message"]).iter().any(|tool| tool["status"] == "awaitingApproval"));
    assert_eq!(waiting["params"]["message"]["id"], live_nodes[5]["id"], "正在写的那条和结构里的是同一条");
    let card = tools(&waiting["params"]["message"])[0].clone();
    assert_eq!(card["arguments"], "{\n  \"content\" : \"y\",\n  \"path\" : \"b.txt\"\n}");
    client.event("runner", |params| params["pendingApproval"].as_array().unwrap().iter().any(|id| id == &json!(SESSION.to_uppercase())));

    // 客户端点「允许」：回给大脑
    client.request("runner.resolveApproval", json!({ "eventID": card["id"], "approved": true }));
    let decision = link.reply_to(approve, 10);
    assert_eq!(decision["result"]["decision"], "allow");
    let id = client.send("runner.resolveApproval", json!({ "eventID": card["id"], "approved": true }));
    assert_eq!(client.reply(id)["error"]["code"], 3, "批过的不能再批");

    // 做完：大脑存档（这里替它追加），网关按存档重发整段；卡片 id 前后一致
    let dir = home.join("projects").join("work-12345678").join("sessions");
    let mut file = std::fs::OpenOptions::new().append(true).open(dir.join(format!("{SESSION}.jsonl"))).unwrap();
    for message in [
        json!({ "role": "user", "content": "再写一个" }),
        json!({ "role": "assistant", "content": "好的，", "tool_calls": [{ "id": "call_0", "type": "function", "function": { "name": "write_file", "arguments": "{\"path\":\"b.txt\",\"content\":\"y\"}" } }] }),
        json!({ "role": "tool", "tool_call_id": "call_0", "content": "已写入" }),
        json!({ "role": "assistant", "content": "写好了 b.txt" }),
    ] {
        writeln!(file, "{}", json!({ "type": "message", "message": message })).unwrap();
    }
    drop(file);
    push(&mut link, "result", json!({ "id": "call_0", "name": "write_file", "ok": true, "text": "已写入" }));
    push(&mut link, "done", json!({ "answer": "写好了 b.txt", "stopped": false, "error": null }));
    let done = client.event("conversation", |params| {
        let nodes = params["conversation"]["nodes"].as_array().unwrap();
        nodes.len() == 6 && nodes[5].get("stats").is_none()
    });
    let final_nodes = done["params"]["conversation"]["nodes"].as_array().unwrap().clone();
    assert_eq!(final_nodes[5]["id"], live_nodes[5]["id"]);
    assert_eq!(tools(&final_nodes[5])[0]["id"], card["id"]);
    assert_eq!(tools(&final_nodes[5])[0]["status"], "succeeded");
    client.event("runner", |params| params["generating"].as_array().unwrap().is_empty());

    // 停下：交给大脑
    let stop = client.send("runner.stop", json!({ "conversationID": SESSION.to_uppercase() }));
    let request = link.wait_for(10, |message| message["method"] == "sessions.stop").expect("没让大脑停下");
    assert_eq!(request["params"]["id"], SESSION);
    link.send(json!({ "jsonrpc": "2.0", "id": request["id"], "result": { "stopped": true } }));
    assert!(client.reply(stop).get("error").is_none());

    // 改名、置顶、草稿：记下、推会话列表
    client.request("conversation.rename", json!({ "id": SESSION.to_uppercase(), "title": "新名字" }));
    client.request("conversation.setPinned", json!({ "id": SESSION.to_uppercase(), "pinned": true }));
    client.request("conversation.setDraft", json!({ "id": SESSION.to_uppercase(), "draft": "没发完" }));
    let listed = client.event("summaries", |params| params["summaries"][0]["hasDraft"] == true);
    let summary = &listed["params"]["summaries"][0];
    assert_eq!((summary["title"].as_str(), summary["pinned"].as_bool()), (Some("新名字"), Some(true)));

    // 没有的方法
    let id = client.send("terminal.open", json!({}));
    assert_eq!(client.reply(id)["error"]["code"], -32601);

    // 删：停下、删存档
    let remove = client.send("conversation.remove", json!({ "id": SESSION.to_uppercase() }));
    if let Some(request) = link.wait_for(10, |message| message["method"] == "sessions.stop") {
        link.send(json!({ "jsonrpc": "2.0", "id": request["id"], "result": { "stopped": false } }));
    }
    assert!(client.reply(remove).get("error").is_none());
    assert!(!dir.join(format!("{SESSION}.jsonl")).exists());
    client.event("summaries", |params| params["summaries"].as_array().unwrap().is_empty());

    // 客户端走了：网关退出
    drop(client.stdin.take());
    let deadline = Instant::now() + Duration::from_secs(10);
    while client.child.try_wait().unwrap().is_none() {
        assert!(Instant::now() < deadline, "客户端走了网关还不退");
        std::thread::sleep(Duration::from_millis(100));
    }
    let _ = std::fs::remove_dir_all(&home);
}
