//! 测试自己扮演 EDITH 大脑：开一个假的套接字（大脑唯一的接口：127.0.0.1 + 令牌），起工具、等它连进来、讲能力协议
#![allow(dead_code)]

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

pub fn python() -> &'static str {
    if cfg!(windows) { "python" } else { "python3" }
}

pub fn fake(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests").join("fake").join(name)
}

/// 一次测试的目录（当 EDITH_HOME）
pub fn scratch(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!("edith-tools-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    root
}

pub fn wait_until(seconds: u64, mut condition: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(seconds);
    while Instant::now() < deadline {
        if condition() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    false
}

/// 假大脑的套接字：连进来、令牌对的连接一条条交出来
pub struct FakeBrain {
    pub address: String,
    pub token: String,
    links: Receiver<Link>,
}

impl FakeBrain {
    pub fn new() -> FakeBrain {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let token = format!("token-{}", std::process::id());
        let (sender, links) = mpsc::channel();
        let wanted = token.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let Ok(reading) = stream.try_clone() else { continue };
                let mut reader = BufReader::new(reading);
                let mut line = String::new();
                let auth = (reader.read_line(&mut line).is_ok())
                    .then(|| serde_json::from_str::<Value>(&line).ok())
                    .flatten()
                    .filter(|message| message["method"] == "auth" && message["params"]["token"] == wanted);
                let Some(auth) = auth else { continue };
                let mut link = Link::new(stream, reader);
                link.relay_as = auth["params"]["as"].as_str().map(str::to_string);
                if sender.send(link).is_err() {
                    break;
                }
            }
        });
        FakeBrain { address, token, links }
    }

    /// 等下一条连进来的连接
    pub fn accept(&self, seconds: u64) -> Option<Link> {
        self.links.recv_timeout(Duration::from_secs(seconds)).ok()
    }

    /// 同 spawn，但它的标准错误接到一根马上关掉的管子上（像大脑被强行结束后那样：日志写不出去）
    pub fn spawn_with_broken_stderr(&self, program: &str, args: &[&str], home: &Path, name: &str) -> Child {
        let mut child = Command::new(program)
            .args(args)
            .env("EDITH_HOME", home)
            .env("EDITH_ACCESS", &self.address)
            .env("EDITH_ACCESS_TOKEN", &self.token)
            .env("EDITH_CAPABILITY", name)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        drop(child.stderr.take());
        child
    }

    /// 像大脑那样起一个工具：给它接入地址、令牌、能力名
    pub fn spawn(&self, program: &str, args: &[&str], home: &Path, name: &str) -> Child {
        Command::new(program)
            .args(args)
            .env("EDITH_HOME", home)
            .env("EDITH_ACCESS", &self.address)
            .env("EDITH_ACCESS_TOKEN", &self.token)
            .env("EDITH_CAPABILITY", name)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap()
    }
}

/// 大脑这边的一条连接（一个能力）
pub struct Link {
    /// auth 里的 as（中转替谁连的）
    pub relay_as: Option<String>,
    writer: TcpStream,
    messages: Receiver<Value>,
    backlog: Vec<Value>,
    next: u64,
}

impl Drop for Link {
    /// 丢掉就关连接（读的线程手里还有一份，不关的话对面看不到断开）
    fn drop(&mut self) {
        let _ = self.writer.shutdown(std::net::Shutdown::Both);
    }
}

impl Link {
    fn new(writer: TcpStream, reader: BufReader<TcpStream>) -> Link {
        let (sender, messages) = mpsc::channel();
        std::thread::spawn(move || {
            for line in reader.lines().map_while(Result::ok) {
                if let Ok(message) = serde_json::from_str::<Value>(&line) {
                    let _ = sender.send(message);
                }
            }
        });
        Link { relay_as: None, writer, messages, backlog: Vec::new(), next: 100 }
    }

    pub fn send(&mut self, message: Value) {
        writeln!(self.writer, "{message}").unwrap();
        self.writer.flush().unwrap();
    }

    /// 等一条满足条件的消息（最多 seconds 秒）；连接断了也是 None
    pub fn wait_for(&mut self, seconds: u64, mut wanted: impl FnMut(&Value) -> bool) -> Option<Value> {
        if let Some(index) = self.backlog.iter().position(&mut wanted) {
            return Some(self.backlog.remove(index));
        }
        let deadline = Instant::now() + Duration::from_secs(seconds);
        while let Some(left) = deadline.checked_duration_since(Instant::now()) {
            match self.messages.recv_timeout(left) {
                Ok(message) if wanted(&message) => return Some(message),
                Ok(message) => self.backlog.push(message),
                Err(_) => return None,
            }
        }
        None
    }

    /// 连接断了（对面关了）
    pub fn closed(&mut self, seconds: u64) -> bool {
        let deadline = Instant::now() + Duration::from_secs(seconds);
        while let Some(left) = deadline.checked_duration_since(Instant::now()) {
            match self.messages.recv_timeout(left) {
                Ok(message) => self.backlog.push(message),
                Err(mpsc::RecvTimeoutError::Disconnected) => return true,
                Err(mpsc::RecvTimeoutError::Timeout) => return false,
            }
        }
        false
    }

    /// 发请求（不等）；返回编号
    pub fn start(&mut self, method: &str, params: Value) -> u64 {
        self.next += 1;
        let id = self.next;
        self.send(json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }));
        id
    }

    pub fn reply_to(&mut self, id: u64, seconds: u64) -> Value {
        self.wait_for(seconds, |message| message["id"] == id && message.get("method").is_none()).unwrap_or_else(|| panic!("{id} 没回话"))
    }

    /// 发请求、等回话
    pub fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self.start(method, params);
        self.reply_to(id, 30)
    }

    /// 握手：initialize（大脑是 brain）→ 回话 → initialized；返回能力报的
    pub fn handshake(&mut self, brain: Value) -> Value {
        let reply = self.request("initialize", json!({ "protocolVersion": "1", "brain": brain, "name": "", "focus": "s1" }));
        self.send(json!({ "jsonrpc": "2.0", "method": "initialized", "params": {} }));
        reply["result"].clone()
    }

    /// 让它做一个动作，返回回话
    pub fn act(&mut self, action: &str, arguments: Value) -> Value {
        let call = format!("c{}", self.next + 1);
        self.request("act", json!({ "call": call, "action": action, "arguments": arguments, "session": "s1", "turn": "t1" }))
    }
}
