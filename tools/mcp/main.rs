//! edith-mcp：MCP 服务的桥（docs/MCP.md）。大脑只有一个接口——本机套接字（edith 仓库的 docs/CAPABILITY-PROTOCOL.md §1）；
//! 它连进大脑的套接字当一个一级能力，把一个 MCP 服务（stdio 或 Streamable HTTP）的工具当成自己的动作报上去，大脑调动作它就调 MCP 的工具。
//! MCP 只能被调用、不会主动说话，所以它只有动作、没有感知。MCP 客户端是 friday.core 的 mcp_client（core/ 子模块）。
//!
//! - 本机的 MCP 服务：`edith capabilities add github edith-mcp npx -y @modelcontextprotocol/server-github`
//! - HTTP 的：`edith capabilities add search edith-mcp --url https://example.com/mcp [--header "Authorization: Bearer …"]`
//!
//! 大脑起它时给 EDITH_ACCESS / EDITH_ACCESS_TOKEN / EDITH_CAPABILITY。和大脑的连接断了它就退出（大脑过一会儿再起它）。

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use friday_core::{events, mcp_client};
use serde_json::{Value, json};

const VERSION: &str = env!("CARGO_PKG_VERSION");
const CONNECT_TIMEOUT_MS: u64 = 60_000;
const CALL_TIMEOUT_MS: u64 = 10 * 60_000;

/// 写不出去（起它的大脑已经没了、管子断了）就算了：eprintln! 写失败会 panic，那条线程一死进程就挂在那里
fn log(text: &str) {
    use std::io::Write as _;
    let _ = writeln!(std::io::stderr(), "{text}");
}

fn main() {
    if let Err(message) = run() {
        log(&message);
        std::process::exit(1);
    }
}

enum Server {
    Stdio { command: String, arguments: Vec<String> },
    Http { url: String, headers: Vec<(String, String)> },
}

fn parse() -> Result<Server, String> {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    match arguments.first().map(String::as_str) {
        Some("-V" | "--version") => {
            println!("edith-mcp {VERSION}");
            std::process::exit(0);
        }
        Some("--url") => {
            let url = arguments.get(1).ok_or("--url 后面要跟地址")?.clone();
            let mut headers = Vec::new();
            let mut rest = arguments[2..].iter();
            while let Some(argument) = rest.next() {
                if argument != "--header" {
                    return Err(format!("不认识 {argument}（--url 地址 [--header \"名字: 值\"]…）"));
                }
                let header = rest.next().ok_or("--header 后面要跟「名字: 值」")?;
                let (key, value) = header.split_once(':').ok_or("--header 要写成「名字: 值」")?;
                headers.push((key.trim().to_string(), value.trim().to_string()));
            }
            Ok(Server::Http { url, headers })
        }
        Some(command) => Ok(Server::Stdio { command: command.to_string(), arguments: arguments[1..].to_vec() }),
        None => Err("用法：edith-mcp <命令> [参数…]（本机的 MCP 服务）；edith-mcp --url <地址> [--header \"名字: 值\"]…".into()),
    }
}

/// 动作名只留字母、数字、-、_（大脑的规矩）；MCP 的工具名可能有点号之类
fn action_name(tool: &str) -> String {
    let cleaned: String = tool.chars().map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' }).collect();
    let cleaned = cleaned.trim_matches('_').to_string();
    if cleaned.is_empty() { "tool".into() } else { cleaned }
}

fn run() -> Result<(), String> {
    let server = parse()?;
    let address = std::env::var("EDITH_ACCESS").ok().filter(|address| !address.is_empty()).ok_or("没有 EDITH_ACCESS：要由大脑起它")?;
    let token = std::env::var("EDITH_ACCESS_TOKEN").unwrap_or_default();
    let name = std::env::var("EDITH_CAPABILITY").ok().filter(|name| !name.is_empty()).unwrap_or_else(|| "mcp".into());
    // 先接上 MCP 服务、拿到工具
    install_http_host();
    let client_id = mcp_client::open("EDITH", VERSION);
    let client = mcp_client::get(client_id).ok_or("开不了 MCP 客户端")?;
    let title = match &server {
        Server::Stdio { command, arguments } => {
            let environment: HashMap<String, String> = std::env::vars().filter(|(key, _)| !key.starts_with("EDITH_ACCESS")).collect();
            client.connect_stdio(command, arguments, &environment, "", CONNECT_TIMEOUT_MS, None).map_err(|failure| failure.message)?;
            format!("{command} {}", arguments.join(" ")).trim().to_string()
        }
        Server::Http { url, headers } => {
            client.connect_http(url, headers.clone(), CONNECT_TIMEOUT_MS, None).map_err(|failure| failure.message)?;
            url.clone()
        }
    };
    let mut tools = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let page = client.list_tools(cursor.as_deref(), CONNECT_TIMEOUT_MS, None).map_err(|failure| failure.message)?;
        tools.extend(page["tools"].as_array().into_iter().flatten().map(mcp_client::normalize_tool));
        cursor = page["nextCursor"].as_str().filter(|next| !next.is_empty()).map(str::to_string);
        if cursor.is_none() {
            break;
        }
    }
    // 动作名 → MCP 的工具名
    let mut names: HashMap<String, String> = HashMap::new();
    let actions: Vec<Value> = tools
        .iter()
        .map(|tool| {
            let original = tool["name"].as_str().unwrap_or("").to_string();
            let mut action = action_name(&original);
            while names.contains_key(&action) {
                action.push('_');
            }
            names.insert(action.clone(), original);
            json!({
                "name": action,
                "description": tool["description"].as_str().unwrap_or(""),
                "parameters": tool["inputSchema"],
                "timeoutSeconds": CALL_TIMEOUT_MS / 1000,
            })
        })
        .collect();
    log(&format!("MCP 服务 {title} 接上了：{} 个工具", actions.len()));
    // 再连进大脑
    let mut stream = TcpStream::connect(&address).map_err(|error| format!("连不上大脑 {address}：{error}"))?;
    writeln!(stream, "{}", json!({ "jsonrpc": "2.0", "method": "auth", "params": { "token": token } }))
        .map_err(|error| error.to_string())?;
    let reading = stream.try_clone().map_err(|error| error.to_string())?;
    let writer = Arc::new(Mutex::new(stream));
    let send = {
        let writer = writer.clone();
        move |message: Value| {
            let mut writer = writer.lock().unwrap();
            let _ = writeln!(writer, "{message}");
            let _ = writer.flush();
        }
    };
    let info = json!({
        "protocolVersion": "1",
        "name": name,
        "capability": { "title": format!("MCP：{title}"), "version": VERSION, "description": "MCP 服务的工具（只能被调用，不会主动说话）" },
        "senses": [],
        "actions": actions,
        "receives": [],
    });
    let names = Arc::new(names);
    for line in BufReader::new(reading).lines() {
        let Ok(line) = line else { break };
        let Ok(message) = serde_json::from_str::<Value>(&line) else { continue };
        let id = message.get("id").filter(|id| !id.is_null()).cloned();
        match (message["method"].as_str(), id) {
            (Some("initialize"), Some(id)) => send(json!({ "jsonrpc": "2.0", "id": id, "result": info })),
            (Some("ping"), Some(id)) => send(json!({ "jsonrpc": "2.0", "id": id, "result": {} })),
            (Some("act"), Some(id)) => {
                // 一个动作一个线程：几个工具调用可以一起跑
                let client = client.clone();
                let names = names.clone();
                let send = send.clone();
                let params = message["params"].clone();
                std::thread::spawn(move || send(call(&client, &names, id, &params)));
            }
            (Some(other), Some(id)) => {
                send(json!({ "jsonrpc": "2.0", "id": id, "error": { "code": -32601, "message": format!("MCP 桥没有 {other}") } }))
            }
            (Some("shutdown"), None) => break,
            _ => {}
        }
    }
    mcp_client::close(client_id);
    Ok(())
}

fn call(client: &Arc<mcp_client::Client>, names: &HashMap<String, String>, id: Value, params: &Value) -> Value {
    let action = params["action"].as_str().unwrap_or("");
    let Some(tool) = names.get(action) else {
        return json!({ "jsonrpc": "2.0", "id": id, "error": { "code": -32602, "message": format!("没有这个工具：{action}") } });
    };
    let arguments = if params["arguments"].is_object() { params["arguments"].clone() } else { json!({}) };
    match client.call_tool(tool, arguments, CALL_TIMEOUT_MS, None) {
        Ok(result) => {
            let failed = result["isError"].as_bool().unwrap_or(false);
            let text = content_text(&result);
            json!({ "jsonrpc": "2.0", "id": id, "result": { "ok": !failed, "text": if text.is_empty() { "（没有内容）".to_string() } else { text } } })
        }
        Err(failure) => json!({ "jsonrpc": "2.0", "id": id, "error": { "code": -32603, "message": failure.message } }),
    }
}

/// 结果里的内容拼成文字（friday-core 解码过的：{kind, text?, uri?, resourceText?, …}；图片、音频只说有一个）
fn content_text(result: &Value) -> String {
    let mut parts = Vec::new();
    for item in result["content"].as_array().into_iter().flatten() {
        let field = |key: &str| item[key].as_str().unwrap_or("").to_string();
        match item["kind"].as_str() {
            Some("text") => parts.push(field("text")),
            Some("image") => parts.push(format!("［图片 {}］", field("mimeType"))),
            Some("audio") => parts.push(format!("［音频 {}］", field("mimeType"))),
            Some("resource") => parts.push(item["resourceText"].as_str().map_or_else(|| field("uri"), str::to_string)),
            Some("resource_link") => parts.push(format!("{}（{}）", field("name"), field("uri"))),
            _ => {}
        }
    }
    parts.join("\n")
}

// MARK: - MCP 的 HTTP（friday-core 推 mcpClient.httpPost，这里发出去、把响应一块块交回）

static ABORTED: LazyLock<Mutex<HashMap<(u64, u64), bool>>> = LazyLock::new(|| Mutex::new(HashMap::new()));
static AGENT: LazyLock<ureq::Agent> = LazyLock::new(|| {
    ureq::Agent::config_builder().http_status_as_error(false).timeout_connect(Some(Duration::from_secs(30))).build().into()
});

fn install_http_host() {
    events::set_handler(Some(Box::new(|message: &str| {
        let Ok(message) = serde_json::from_str::<Value>(message) else { return };
        let data = message.get("data").cloned().unwrap_or(Value::Null);
        let number = |key: &str| data.get(key).and_then(Value::as_u64).unwrap_or(0);
        match message.get("event").and_then(Value::as_str) {
            Some("mcpClient.httpPost") => {
                let (client, post) = (number("client"), number("post"));
                ABORTED.lock().unwrap().insert((client, post), false);
                std::thread::spawn(move || http_post(client, post, &data));
            }
            Some("mcpClient.httpAbort") => {
                if let Some(flag) = ABORTED.lock().unwrap().get_mut(&(number("client"), number("post"))) {
                    *flag = true;
                }
            }
            Some("mcpClient.stderr") => log(&format!("[MCP] {}", data["text"].as_str().unwrap_or("").trim_end())),
            Some("mcpClient.exited") => {
                // MCP 服务没了：退出，让大脑过一会儿重起（重新接上）
                log(&format!("MCP 服务退出了：{}", data["reason"].as_str().unwrap_or("")));
                std::process::exit(1);
            }
            _ => {}
        }
    })));
}

fn http_post(client_id: u64, post: u64, data: &Value) {
    let aborted = || ABORTED.lock().unwrap().get(&(client_id, post)).copied().unwrap_or(true);
    let finish = |error: Option<&str>, was_aborted: bool| {
        ABORTED.lock().unwrap().remove(&(client_id, post));
        if let Some(client) = mcp_client::get(client_id) {
            client.http_end(post, error, was_aborted);
        }
    };
    let url = data.get("url").and_then(Value::as_str).unwrap_or("");
    let mut request = AGENT.post(url);
    for pair in data.get("headers").and_then(Value::as_array).into_iter().flatten() {
        if let (Some(key), Some(value)) = (pair.get(0).and_then(Value::as_str), pair.get(1).and_then(Value::as_str)) {
            request = request.header(key, value);
        }
    }
    let body = data.get("body").and_then(Value::as_str).unwrap_or("").to_string();
    let mut response = match request.send(body) {
        Ok(response) => response,
        Err(error) => return finish(Some(&error.to_string()), false),
    };
    let header = |name: &str| response.headers().get(name).and_then(|value| value.to_str().ok()).unwrap_or("").to_string();
    let (content_type, session) = (header("content-type"), header("mcp-session-id"));
    let Some(client) = mcp_client::get(client_id) else { return };
    client.http_head(post, response.status().as_u16() as i64, &content_type, &session);
    let mut reader = response.body_mut().as_reader();
    let mut buffer = [0u8; 8192];
    loop {
        if aborted() {
            return finish(None, true);
        }
        match reader.read(&mut buffer) {
            Ok(0) => return finish(None, false),
            Ok(count) => client.http_data(post, &buffer[..count]),
            Err(error) => return finish(Some(&error.to_string()), false),
        }
    }
}
