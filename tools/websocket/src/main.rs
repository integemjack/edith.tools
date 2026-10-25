//! edith-websocket：远程能力的中转（docs/WEBSOCKET.md）。大脑只有一个接口——本机套接字（edith 仓库的 docs/CAPABILITY-PROTOCOL.md §1）；
//! 远程的设备、网页、手机连这个工具的 WebSocket，它替**每个客户端另开一条连接到大脑的套接字**，之后 WebSocket 文本帧和 JSON 行原样双向转：
//! 远程客户端自己讲完整的能力协议（回 initialize、报自己的感知和动作、推 sense、收 event、做 act），在大脑那里就是一个一级能力。
//! 它不懂协议内容，只在客户端回 initialize 没写 name 时补上连接时给的 ?name=。
//! 替客户端开的连接用 auth 的 `as` 报客户端名：大脑把它叫 `<这个能力的名字>-<客户端名>`，权限按能力配置的 permissions.relay
//! （默认只读的工具、只看会话）——远程客户端拿不到本机全部的权力。
//!
//! 它自己也用一条连接连进大脑（名字是 EDITH_CAPABILITY），报对外的入口（endpoints），`edith instances` 看得到。
//! 大脑起它时给 EDITH_ACCESS / EDITH_ACCESS_TOKEN / EDITH_CAPABILITY；自己跑时用 `--brain <目录>` 从 ~/.edith/instances/ 找那个大脑。
//!
//! `--listen` 默认 127.0.0.1 上系统挑的空端口；不是只听本机（127.0.0.1 / ::1 / localhost）时必须给令牌（--token 或 EDITH_WEBSOCKET_TOKEN）。

use std::io::{BufRead, BufReader, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::Duration;

use serde_json::{Value, json};
use tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tungstenite::{Message, WebSocket};

const VERSION: &str = env!("CARGO_PKG_VERSION");

static NEXT_CLIENT: AtomicU64 = AtomicU64::new(1);

/// 写不出去（起它的大脑已经没了、管子断了）就算了：eprintln! 写失败会 panic，那条线程一死进程就挂在那里
fn log(text: &str) {
    use std::io::Write as _;
    let _ = writeln!(std::io::stderr(), "{text}");
}

/// 大脑的套接字：地址、令牌
#[derive(Clone)]
struct Access {
    address: String,
    token: String,
}

impl Access {
    /// 连上去、报令牌；relay_as：替哪个客户端连（中转的子连接）
    fn connect(&self, relay_as: Option<&str>) -> Result<TcpStream, String> {
        let mut stream = TcpStream::connect(&self.address).map_err(|error| format!("连不上大脑 {}：{error}", self.address))?;
        let mut params = json!({ "token": self.token });
        if let Some(name) = relay_as {
            params["as"] = json!(name);
        }
        writeln!(stream, "{}", json!({ "jsonrpc": "2.0", "method": "auth", "params": params })).map_err(|error| error.to_string())?;
        Ok(stream)
    }
}

struct Settings {
    listen: String,
    token: Option<String>,
    brain: Option<PathBuf>,
}

fn settings() -> Result<Settings, String> {
    let mut settings = Settings {
        listen: "127.0.0.1:0".into(),
        token: std::env::var("EDITH_WEBSOCKET_TOKEN").ok().filter(|token| !token.is_empty()),
        brain: None,
    };
    let mut arguments = std::env::args().skip(1);
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--listen" => settings.listen = arguments.next().ok_or("--listen 后面要跟地址")?,
            "--token" => settings.token = Some(arguments.next().ok_or("--token 后面要跟令牌")?),
            "--brain" => settings.brain = Some(PathBuf::from(arguments.next().ok_or("--brain 后面要跟目录")?)),
            "-V" | "--version" => {
                println!("edith-websocket {VERSION}");
                std::process::exit(0);
            }
            other => return Err(format!("不认识 {other}（--listen 地址:端口、--token 令牌、--brain 目录）")),
        }
    }
    let host = settings.listen.rsplit_once(':').map(|(host, _)| host.trim_matches(['[', ']'])).unwrap_or("");
    if !matches!(host, "127.0.0.1" | "::1" | "localhost") && settings.token.is_none() {
        return Err(format!("{} 不只是本机能连：要给 --token（或环境变量 EDITH_WEBSOCKET_TOKEN）", settings.listen));
    }
    Ok(settings)
}

/// EDITH 的数据根：EDITH_HOME；没指时 ~/.edith（它的 storage.json 里换过位置就用换过的，同 EDITH）
fn home() -> PathBuf {
    match std::env::var_os("EDITH_HOME") {
        Some(dir) if !dir.is_empty() => PathBuf::from(dir),
        _ => {
            let default = std::env::var_os("HOME")
                .or_else(|| std::env::var_os("USERPROFILE"))
                .map(PathBuf::from)
                .unwrap_or_else(|| ".".into())
                .join(".edith");
            let pointer: serde_json::Value =
                std::fs::read(default.join("storage.json")).ok().and_then(|data| serde_json::from_slice(&data).ok()).unwrap_or_default();
            match pointer["root"].as_str().map(str::trim).filter(|root| !root.is_empty()) {
                Some(root) => PathBuf::from(root),
                None => default,
            }
        }
    }
}

/// 自己跑时：~/.edith/instances/ 里找这个目录正在跑的大脑（新起的优先）
fn find_brain(directory: &std::path::Path) -> Result<Access, String> {
    let wanted = std::fs::canonicalize(directory).map_err(|_| format!("目录不存在：{}", directory.display()))?;
    let mut found: Vec<Value> = std::fs::read_dir(home().join("instances"))
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| serde_json::from_slice::<Value>(&std::fs::read(entry.path()).ok()?).ok())
        .filter(|entry| entry["workspace"].as_str().map(PathBuf::from).as_deref() == Some(wanted.as_path()))
        .collect();
    found.sort_by(|a, b| b["started"].as_str().cmp(&a["started"].as_str()));
    let entry = found.first().ok_or(format!("{} 没有在跑的大脑", wanted.display()))?;
    Ok(Access { address: entry["address"].as_str().unwrap_or("").to_string(), token: entry["token"].as_str().unwrap_or("").to_string() })
}

fn main() {
    if let Err(message) = run() {
        log(&message);
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let settings = settings()?;
    let access = match &settings.brain {
        Some(directory) => find_brain(directory)?,
        None => match (std::env::var("EDITH_ACCESS"), std::env::var("EDITH_ACCESS_TOKEN")) {
            (Ok(address), Ok(token)) if !address.is_empty() => Access { address, token },
            _ => return Err("不知道连哪个大脑：由大脑起它（给 EDITH_ACCESS），或者用 --brain <目录>".into()),
        },
    };
    let name = std::env::var("EDITH_CAPABILITY").ok().filter(|name| !name.is_empty()).unwrap_or_else(|| "websocket".into());
    let listener = TcpListener::bind(&settings.listen).map_err(|error| format!("开不了 {}：{error}", settings.listen))?;
    let url = format!("ws://{}", listener.local_addr().map(|address| address.to_string()).unwrap_or(settings.listen.clone()));
    // 自己的那条连接：握手、报入口；它断了（大脑退出）就退出
    let control = access.connect(None)?;
    {
        let url = url.clone();
        std::thread::spawn(move || control_loop(control, &name, &url));
    }
    log(&format!("在 {url} 接远程能力，转到大脑 {}", access.address));
    let token = Arc::new(settings.token);
    for stream in listener.incoming().flatten() {
        let token = token.clone();
        let access = access.clone();
        std::thread::spawn(move || serve_client(stream, &token, &access));
    }
    Ok(())
}

/// 自己在大脑那里的连接：回 initialize（名字、入口），回 ping；连接断了或收到 shutdown 就整个退出
fn control_loop(stream: TcpStream, name: &str, url: &str) {
    let Ok(reading) = stream.try_clone() else { std::process::exit(1) };
    let mut writer = stream;
    for line in BufReader::new(reading).lines() {
        let Ok(line) = line else { break };
        let Ok(message) = serde_json::from_str::<Value>(&line) else { continue };
        let id = message.get("id").filter(|id| !id.is_null()).cloned();
        let reply = match (message["method"].as_str(), id) {
            (Some("initialize"), Some(id)) => Some(json!({ "jsonrpc": "2.0", "id": id, "result": {
                "protocolVersion": "1",
                "name": name,
                "capability": { "title": "WebSocket", "version": VERSION, "description": format!("远程能力的中转：在 {url} 接设备、网页、手机，每个连上来的是一个能力") },
                "senses": [], "actions": [], "receives": [],
                "endpoints": [{ "kind": "websocket", "url": url }],
            }})),
            (Some("ping"), Some(id)) => Some(json!({ "jsonrpc": "2.0", "id": id, "result": {} })),
            (Some(other), Some(id)) => {
                Some(json!({ "jsonrpc": "2.0", "id": id, "error": { "code": -32601, "message": format!("WebSocket 中转没有 {other}") } }))
            }
            (Some("shutdown"), None) => std::process::exit(0),
            _ => None,
        };
        if let Some(reply) = reply
            && writeln!(writer, "{reply}").and_then(|_| writer.flush()).is_err()
        {
            break;
        }
    }
    log("大脑断开了，退出");
    std::process::exit(0);
}

fn query(uri: &str, key: &str) -> Option<String> {
    let query = uri.split_once('?')?.1;
    query.split('&').filter_map(|pair| pair.split_once('=')).find(|(name, _)| *name == key).map(|(_, value)| percent_decode(value))
}

fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte == b'%'
            && index + 2 < bytes.len()
            && let Ok(decoded) = u8::from_str_radix(std::str::from_utf8(&bytes[index + 1..index + 3]).unwrap_or("zz"), 16)
        {
            out.push(decoded);
            index += 3;
            continue;
        }
        out.push(if byte == b'+' { b' ' } else { byte });
        index += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

// 握手回调的返回类型是 tungstenite 定的
#[allow(clippy::result_large_err)]
fn serve_client(stream: TcpStream, token: &Option<String>, access: &Access) {
    let peer = stream.peer_addr().map(|address| address.to_string()).unwrap_or_default();
    let mut name = String::new();
    let check = |request: &Request, response: Response| -> Result<Response, ErrorResponse> {
        let uri = request.uri().to_string();
        name = query(&uri, "name").unwrap_or_default();
        if let Some(token) = token {
            let header = request
                .headers()
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.strip_prefix("Bearer "));
            let given = header.map(str::to_string).or_else(|| query(&uri, "token"));
            if given.as_deref().map(str::trim) != Some(token.as_str()) {
                let mut refused = ErrorResponse::new(Some("要令牌：?token=… 或 Authorization: Bearer …".into()));
                *refused.status_mut() = tungstenite::http::StatusCode::UNAUTHORIZED;
                return Err(refused);
            }
        }
        Ok(response)
    };
    let mut socket = match tungstenite::accept_hdr(stream, check) {
        Ok(socket) => socket,
        Err(error) => {
            log(&format!("{peer} 没连上：{error}"));
            return;
        }
    };
    if name.is_empty() {
        name = format!("remote-{}", NEXT_CLIENT.fetch_add(1, Ordering::Relaxed));
    }
    // 替它另开一条连接到大脑（as：大脑据此定名字、收窄权限）
    let brain = match access.connect(Some(&name)) {
        Ok(brain) => brain,
        Err(error) => {
            log(&format!("{name}：{error}"));
            let _ = socket.close(None);
            return;
        }
    };
    log(&format!("{name}（{peer}）连上了"));
    relay(&mut socket, brain, &name);
    log(&format!("{name} 断开了"));
}

/// 原样双向转：大脑的一行 → 一个文本帧，一个文本帧 → 大脑的一行。客户端回 initialize 没写 name 的补上
fn relay(socket: &mut WebSocket<TcpStream>, brain: TcpStream, name: &str) {
    let _ = socket.get_ref().set_read_timeout(Some(Duration::from_millis(30)));
    let (from_brain, lines) = mpsc::channel::<String>();
    let Ok(reading) = brain.try_clone() else { return };
    std::thread::spawn(move || {
        for line in BufReader::new(reading).lines().map_while(Result::ok) {
            if from_brain.send(line).is_err() {
                break;
            }
        }
    });
    let mut brain = brain;
    // 大脑发的 initialize 的编号（客户端回它时补 name）
    let mut initialize: Option<Value> = None;
    'relay: loop {
        match socket.read() {
            Ok(Message::Text(text)) => {
                let mut line = text.as_str().replace('\n', " ");
                if let Some(id) = &initialize
                    && let Ok(mut message) = serde_json::from_str::<Value>(&line)
                    && message["id"] == *id
                    && message.get("method").is_none()
                {
                    if message["result"].is_object() && message["result"].get("name").is_none() {
                        message["result"]["name"] = json!(name);
                    }
                    line = message.to_string();
                    initialize = None;
                }
                if writeln!(brain, "{line}").and_then(|_| brain.flush()).is_err() {
                    break;
                }
            }
            Ok(Message::Close(_)) => break,
            Ok(_) => {}
            Err(tungstenite::Error::Io(error)) if matches!(error.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut) => {
            }
            Err(_) => break,
        }
        loop {
            match lines.try_recv() {
                Ok(line) => {
                    if initialize.is_none()
                        && let Ok(message) = serde_json::from_str::<Value>(&line)
                        && message["method"] == "initialize"
                    {
                        initialize = Some(message["id"].clone());
                    }
                    if socket.send(Message::text(line)).is_err() {
                        break 'relay;
                    }
                }
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => break 'relay,
            }
        }
        let _ = socket.flush();
    }
    let _ = socket.close(None);
    let _ = socket.flush();
    let _ = brain.shutdown(Shutdown::Both);
}
