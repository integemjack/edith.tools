//! edith-friday：让 Friday 的客户端（Qt、Swift）连 EDITH——讲 Friday 的远程协议（Friday 仓库的 qt/docs/REMOTE.md），对内讲 EDITH 的能力协议。
//! 用户 2026-10-24 定：EDITH 当 Friday 的服务端，走网关，客户端不用改。
//!
//! - **一条连接一个网关**：客户端经 SSH 运行 `edith-friday --attach`（装成 `~/.friday-server/bin/friday-server` 时也认 `--attach`），
//!   网关在标准输入 / 输出上讲 Friday 协议。要一直干的活在各个目录的 EDITH 大脑里（断了照常跑），网关自己不用常驻；
//!   几个客户端各开一个网关，看到的都是大脑里的同一份。
//! - **一台机器的视图**：会话列表直接读 `~/.edith/projects/*/sessions/*.jsonl`（看会话不用起大脑）；在某个目录发话、新建会话时，
//!   按 EDITH 的规则算出它的工作目录（往上找 .edith / 到 git 根），`instances/` 里登记着就连上，没有就在那里起 `edith run --idle`（脱离这次 SSH）。
//! - **翻译**：EDITH 的历史（OpenAI 格式）换成 Friday 的 Conversation / ChatMessage（用户消息、助手消息里的文字 / 思考 / 工具卡片）；
//!   id 由会话 id 和位置算出来（同一条消息每次都是同一个 UUID）；正在干的那一轮按大脑推来的事件拼出来，做完再按存档重发整段。
//! - **批准**：网关连大脑时说自己能批准（approves）、是全权的连接，大脑要批准的事交给它，它变成 Friday 的「待确认」
//!   （工具卡片 awaitingApproval、runner.pendingApproval、attention 事件），客户端 runner.resolveApproval 回过去。
//!
//! 第一阶段只做：连上、看会话、新建 / 删 / 改名 / 置顶 / 草稿、发话（忙时补充或排队）、流式看回复、批准、停下。
//! 别的方法回 -32601（没有）或错误 4（这一版的 EDITH 还不支持），server.features 是空的，客户端就不去用那些功能。

use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime};

use serde_json::{Map, Value, json};
use sha2::Digest;

const VERSION: &str = env!("CARGO_PKG_VERSION");
/// Friday 远程协议的版本（== Friday 的 RemoteProtocol::version）
const PROTOCOL: i64 = 1;
const ENGINE_ID: &str = "edith";
const ROOT: &str = "00000000-0000-0000-0000-000000000000";
// Friday 协议的错误码
const NOT_GREETED: i64 = 2;
const NOT_FOUND: i64 = 3;
const NOT_ALLOWED: i64 = 4;

fn log(text: &str) {
    let _ = writeln!(std::io::stderr(), "{text}");
}

// MARK: - 路径、时间、id

fn user_home() -> PathBuf {
    std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE")).map(PathBuf::from).unwrap_or_else(|| PathBuf::from("."))
}

fn edith_home() -> PathBuf {
    match std::env::var_os("EDITH_HOME") {
        Some(dir) if !dir.is_empty() => PathBuf::from(dir),
        _ => user_home().join(".edith"),
    }
}

fn canonical(path: &Path) -> PathBuf {
    let path = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let text = path.to_string_lossy().into_owned();
    match text.strip_prefix(r"\\?\") {
        Some(rest) if !rest.starts_with("UNC\\") => PathBuf::from(rest),
        _ => path,
    }
}

/// 同 EDITH 的 home::find_workspace：往上找到第一个有 .edith/ 的目录（全局数据目录、家目录的不算），到 git 仓库的根为止
fn find_workspace(start: &Path) -> PathBuf {
    let start = canonical(start);
    let global = canonical(&edith_home());
    let user = canonical(&user_home());
    let mut current = Some(start.as_path());
    while let Some(candidate) = current {
        let data = candidate.join(".edith");
        if data.is_dir() && canonical(&data) != global && candidate != user {
            return candidate.to_path_buf();
        }
        if candidate.join(".git").exists() {
            return candidate.to_path_buf();
        }
        current = candidate.parent();
    }
    start
}

/// ISO 8601、UTC、不带毫秒（Swift 读带毫秒的会失败）
fn iso(time: SystemTime) -> String {
    chrono::DateTime::<chrono::Utc>::from(time).format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

fn iso_text(text: &str) -> String {
    chrono::DateTime::parse_from_rfc3339(text).map(|time| iso(time.into())).unwrap_or_else(|_| iso(SystemTime::now()))
}

/// 一个固定的 UUID（大写）：同样的输入每次都一样
fn stable_uuid(parts: &[&str]) -> String {
    let digest = sha2::Sha256::digest(parts.join("\u{1f}").as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(bytes).hyphenated().to_string().to_uppercase()
}

fn upper(id: &str) -> String {
    id.to_uppercase()
}

fn lower(id: &str) -> String {
    id.to_lowercase()
}

// MARK: - 读 EDITH 的会话存档（只追加的 .jsonl，同 EDITH 的 sessions.rs）

#[derive(Clone, Debug, Default)]
struct Stored {
    id: String,
    title: String,
    cwd: String,
    created: String,
    owner: String,
    history: Vec<Value>,
    updated: Option<SystemTime>,
    path: PathBuf,
}

fn read_session(path: &Path) -> Option<Stored> {
    let text = std::fs::read_to_string(path).ok()?;
    let id = path.file_stem()?.to_string_lossy().into_owned();
    let mut session = Stored { id, path: path.to_path_buf(), ..Default::default() };
    for line in text.lines() {
        let Ok(entry) = serde_json::from_str::<Value>(line) else { continue };
        let field = |key: &str| entry.get(key).and_then(Value::as_str).map(str::to_string);
        match entry["type"].as_str() {
            Some("meta") => {
                if let Some(cwd) = field("cwd") {
                    session.cwd = cwd;
                }
                if let Some(created) = field("created").filter(|_| session.created.is_empty()) {
                    session.created = created;
                }
                if let Some(owner) = field("owner") {
                    session.owner = owner;
                }
            }
            Some("message") => session.history.push(entry["message"].clone()),
            Some("reset") => session.history.clear(),
            Some("title") => session.title = field("title").unwrap_or_default(),
            _ => {}
        }
    }
    session.updated = std::fs::metadata(path).and_then(|meta| meta.modified()).ok();
    Some(session)
}

/// 这台上全部（主人的）会话
fn all_sessions() -> Vec<Stored> {
    let mut sessions = Vec::new();
    for project in std::fs::read_dir(edith_home().join("projects")).into_iter().flatten().flatten() {
        for entry in std::fs::read_dir(project.path().join("sessions")).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|extension| extension == "jsonl")
                && let Some(session) = read_session(&path)
                && session.owner.is_empty()
            {
                sessions.push(session);
            }
        }
    }
    sessions
}

fn find_session(id: &str) -> Option<Stored> {
    let id = lower(id);
    for project in std::fs::read_dir(edith_home().join("projects")).into_iter().flatten().flatten() {
        let path = project.path().join("sessions").join(format!("{id}.jsonl"));
        if path.is_file() {
            return read_session(&path).filter(|session| session.owner.is_empty());
        }
    }
    None
}

// MARK: - 网关自己记的（EDITH 没有的：置顶、草稿、最近的目录、每段会话选的档位）

#[derive(Default)]
struct Meta {
    value: Value,
}

impl Meta {
    fn path() -> PathBuf {
        edith_home().join("friday.json")
    }

    fn load() -> Meta {
        let value = std::fs::read(Self::path()).ok().and_then(|bytes| serde_json::from_slice(&bytes).ok()).unwrap_or_else(|| json!({}));
        Meta { value }
    }

    fn save(&self) {
        let _ = std::fs::create_dir_all(edith_home());
        let _ = std::fs::write(Self::path(), serde_json::to_vec_pretty(&self.value).unwrap_or_default());
    }

    fn pinned(&self, id: &str) -> bool {
        self.value["pinned"].as_array().is_some_and(|ids| ids.iter().any(|item| item.as_str() == Some(&lower(id))))
    }

    fn set_pinned(&mut self, id: &str, pinned: bool) {
        let mut ids: Vec<String> =
            self.value["pinned"].as_array().into_iter().flatten().filter_map(Value::as_str).map(str::to_string).collect();
        ids.retain(|item| *item != lower(id));
        if pinned {
            ids.push(lower(id));
        }
        self.value["pinned"] = json!(ids);
        self.save();
    }

    fn draft(&self, id: &str) -> Option<String> {
        self.value["drafts"][lower(id)].as_str().filter(|draft| !draft.is_empty()).map(str::to_string)
    }

    fn set_draft(&mut self, id: &str, draft: &str) {
        if !self.value["drafts"].is_object() {
            self.value["drafts"] = json!({});
        }
        if draft.is_empty() {
            self.value["drafts"].as_object_mut().unwrap().remove(&lower(id));
        } else {
            self.value["drafts"][lower(id)] = json!(draft);
        }
        self.save();
    }

    fn setting(&self, id: &str, key: &str) -> Option<String> {
        self.value["settings"][lower(id)][key].as_str().map(str::to_string)
    }

    fn set_setting(&mut self, id: &str, key: &str, value: &str) {
        if !self.value["settings"].is_object() {
            self.value["settings"] = json!({});
        }
        if !self.value["settings"][lower(id)].is_object() {
            self.value["settings"][lower(id)] = json!({});
        }
        self.value["settings"][lower(id)][key] = json!(value);
        self.save();
    }

    fn recent(&self) -> Vec<String> {
        self.value["recent"].as_array().into_iter().flatten().filter_map(Value::as_str).map(str::to_string).collect()
    }

    fn push_recent(&mut self, path: &str) {
        let mut recent = self.recent();
        recent.retain(|item| item != path);
        recent.insert(0, path.to_string());
        recent.truncate(10);
        self.value["recent"] = json!(recent);
        self.save();
    }

    fn remove_recent(&mut self, path: &str) {
        let mut recent = self.recent();
        recent.retain(|item| item != path);
        self.value["recent"] = json!(recent);
        self.save();
    }

    fn forget(&mut self, id: &str) {
        self.set_pinned(id, false);
        self.set_draft(id, "");
        if let Some(settings) = self.value["settings"].as_object_mut() {
            settings.remove(&lower(id));
            self.save();
        }
    }
}

// MARK: - 历史 → Friday 的消息

/// 正在干的一轮里的一块
#[derive(Clone, Debug)]
enum Piece {
    Text(String),
    Thinking(String),
    Tool { call: String, name: String, arguments: Value, status: String, result: String },
}

/// 内置工具在 Friday 卡片上的标题和图标（没有的不写）
fn tool_look(name: &str) -> Option<(&'static str, &'static str)> {
    Some(match name {
        "read_file" => ("读取文件", "doc.text"),
        "write_file" => ("写入文件", "square.and.pencil"),
        "edit_file" => ("修改文件", "pencil"),
        "list_directory" => ("列出目录", "folder"),
        "find_files" => ("查找文件", "doc.text.magnifyingglass"),
        "search_files" | "search_keywords" => ("搜索内容", "magnifyingglass"),
        "run_shell" => ("执行命令", "terminal"),
        "web_search" => ("搜索网页", "globe"),
        "fetch_url" => ("打开网页", "globe"),
        "get_current_time" => ("查看时间", "clock"),
        "delegate_task" | "run_parallel_tasks" | "run_workflow" => ("派子智能体", "person.2"),
        _ => return None,
    })
}

/// 工具参数给人看的样子（同 Friday 的 Json::stringify(value, true)：两空格缩进、「"键" : 值」）
fn pretty(arguments: &Value) -> String {
    serde_json::to_string_pretty(arguments).unwrap_or_default().replace("\": ", "\" : ")
}

/// 工具卡片的 id：会话 + 第几条消息 + 调用 id + 这条消息里第几次用这个调用 id（有的模型每轮都叫 call_0，光靠调用 id 会撞）。
/// 正在干时和存档后算出来的一样——客户端按它把「允许 / 拒绝」放在卡片上
fn call_uuid(session: &str, node: usize, call: &str, nth: usize) -> String {
    stable_uuid(&[session, "call", &node.to_string(), call, &nth.to_string()])
}

#[allow(clippy::too_many_arguments)]
fn tool_block(session: &str, node: usize, call: &str, nth: usize, name: &str, arguments: &Value, status: &str, result: &str) -> Value {
    let mut event = json!({
        "id": call_uuid(session, node, call, nth),
        "name": name,
        "arguments": pretty(arguments),
        "status": status,
        "result": result,
    });
    if let Some((title, symbol)) = tool_look(name) {
        event["title"] = json!(title);
        event["symbol"] = json!(symbol);
    }
    json!({ "id": stable_uuid(&[session, "block", &node.to_string(), call, &nth.to_string()]), "kind": { "tool": { "_0": event } } })
}

fn text_block(session: &str, key: &str, kind: &str, text: &str) -> Value {
    json!({ "id": stable_uuid(&[session, "block", key]), "kind": { kind: { "_0": text } } })
}

/// 交给模型的提醒（交付检查、写完没验证……）：不当一条用户消息显示，记成上一条回复的提示
fn is_framework(text: &str) -> bool {
    text.starts_with("（Friday 检查") || text.starts_with("（Friday 交付检查") || text.starts_with("（Friday 验收")
}

/// 用户消息给人看的样子：去掉大脑给模型标的来源（「（…在你工作时补充了下面这段…）」这类第一行）
fn shown_user_text(text: &str) -> String {
    if let Some((first, rest)) = text.split_once('\n')
        && first.starts_with('（')
        && first.ends_with('）')
        && ["传来", "补充了下面这段", "记下了下面这段"].iter().any(|mark| first.contains(mark))
    {
        return rest.to_string();
    }
    text.to_string()
}

fn user_text(message: &Value) -> String {
    match &message["content"] {
        Value::String(text) => text.clone(),
        Value::Array(parts) => parts.iter().filter_map(|part| part["text"].as_str()).collect::<Vec<_>>().join("\n"),
        _ => String::new(),
    }
}

struct Node {
    role: &'static str,
    blocks: Vec<Value>,
    notice: Vec<String>,
}

/// EDITH 的历史 → 一条条消息（用户的一条；一轮里模型说的话、调的工具合成一条助手消息）
fn group(session: &str, history: &[Value]) -> Vec<Node> {
    let mut nodes: Vec<Node> = Vec::new();
    // 调用 id → 它的卡片在哪（第几条消息、第几块）：结果按顺序配给最近一次这个 id 的调用（有的模型每轮都叫 call_0）
    let mut open: HashMap<String, (usize, usize)> = HashMap::new();
    // （第几条消息, 调用 id）→ 用过几次
    let mut uses: HashMap<(usize, String), usize> = HashMap::new();
    for (index, message) in history.iter().enumerate() {
        match message["role"].as_str() {
            Some("tool") => {
                let id = message["tool_call_id"].as_str().unwrap_or("");
                if let Some((node, block)) = open.remove(id) {
                    let result = user_text(message);
                    let event = &mut nodes[node].blocks[block]["kind"]["tool"]["_0"];
                    event["status"] = json!(stored_status(&result));
                    event["result"] = json!(result);
                }
            }
            Some("user") => {
                let text = user_text(message);
                if is_framework(&text) {
                    if let Some(last) = nodes.last_mut().filter(|node| node.role == "assistant") {
                        last.notice.push("交付检查追了一条提醒".into());
                    }
                    continue;
                }
                nodes.push(Node {
                    role: "user",
                    blocks: vec![text_block(session, &format!("{index}"), "text", &shown_user_text(&text))],
                    notice: Vec::new(),
                });
            }
            Some("assistant") => {
                if nodes.last().is_none_or(|node| node.role != "assistant") {
                    nodes.push(Node { role: "assistant", blocks: Vec::new(), notice: Vec::new() });
                }
                let position = nodes.len() - 1;
                let node = nodes.last_mut().unwrap();
                if let Some(thinking) = message["reasoning_content"].as_str().filter(|text| !text.trim().is_empty()) {
                    node.blocks.push(text_block(session, &format!("{index}.thinking"), "thinking", thinking));
                }
                if let Some(text) = message["content"].as_str().filter(|text| !text.trim().is_empty()) {
                    node.blocks.push(text_block(session, &format!("{index}.text"), "text", text));
                }
                for call in message["tool_calls"].as_array().into_iter().flatten() {
                    let id = call["id"].as_str().unwrap_or("");
                    let name = call["function"]["name"].as_str().unwrap_or("");
                    let arguments: Value =
                        serde_json::from_str(call["function"]["arguments"].as_str().unwrap_or("{}")).unwrap_or(json!({}));
                    // 没有结果的（做到一半被停下）：算失败
                    open.insert(id.to_string(), (position, node.blocks.len()));
                    let nth = uses.entry((position, id.to_string())).or_insert(0);
                    node.blocks.push(tool_block(session, position, id, *nth, name, &arguments, "failed", ""));
                    *nth += 1;
                }
            }
            _ => {}
        }
    }
    nodes
}

fn message_uuid(session: &str, node: usize) -> String {
    stable_uuid(&[session, "message", &node.to_string()])
}

fn message_json(session: &str, index: usize, node: Node, created: &str) -> Value {
    let mut message = json!({ "id": message_uuid(session, index), "role": node.role, "blocks": node.blocks, "createdAt": created });
    if index > 0 {
        message["parentID"] = json!(message_uuid(session, index - 1));
    }
    if !node.notice.is_empty() {
        message["notice"] = json!(node.notice.join("；"));
    }
    message
}

/// 存档里的工具结果算哪种状态（存档不记成败：按 EDITH 拒绝时的说法认）
fn stored_status(result: &str) -> &'static str {
    if result.starts_with("用户没批准") {
        "denied"
    } else if ["没有权限", "没做：", "不许：", "出错", "失败"].iter().any(|start| result.starts_with(start)) {
        "failed"
    } else {
        "succeeded"
    }
}

/// 一段会话的消息（含正在干的那一轮）→ Friday 的 nodes 和 selectedChildren
fn messages(session: &Stored, live: Option<&Live>) -> (Vec<Value>, Map<String, Value>) {
    let created = session.updated.map(iso).unwrap_or_else(|| iso_text(&session.created));
    let mut out: Vec<Value> = group(&session.id, &session.history)
        .into_iter()
        .enumerate()
        .map(|(index, node)| message_json(&session.id, index, node, &created))
        .collect();
    if let Some(live) = live {
        // 存档里还没有这一轮（做完才存）：接在后面，序号同存档后的
        out.truncate(live.node - 1);
        out.push(live.user_message(&session.id));
        out.push(live.message(&session.id));
    }
    let mut selected = Map::new();
    for message in &out {
        let parent = message["parentID"].as_str().unwrap_or(ROOT).to_string();
        selected.insert(parent, message["id"].clone());
    }
    (out, selected)
}

// MARK: - 正在干的一轮

#[derive(Clone, Debug)]
struct Live {
    turn: String,
    text: String,
    /// 这一轮的助手消息是第几条（存档里的消息数 + 1：前面一条是这一轮的用户消息）
    node: usize,
    pieces: Vec<Piece>,
    notice: Vec<String>,
    started: SystemTime,
    /// 这一轮生成了多少 token、用了几秒（stats 事件累计）
    tokens: f64,
    seconds: f64,
    /// 还没报用量的这段请求估了多少 token、第一个字什么时候到的
    pending: f64,
    first_output: Option<Instant>,
}

impl Live {
    /// 这段会话开始的一轮（turn.started；连上之前就开始的，第一次收到它的事件时补一个）
    fn start(session: &str, turn: &str, text: &str) -> Live {
        let stored = find_session(session).map(|stored| group(&stored.id, &stored.history).len()).unwrap_or(0);
        Live {
            turn: turn.to_string(),
            text: text.to_string(),
            node: stored + 1,
            pieces: Vec::new(),
            notice: Vec::new(),
            started: SystemTime::now(),
            tokens: 0.0,
            seconds: 0.0,
            pending: 0.0,
            first_output: None,
        }
    }

    fn blocks(&self, session: &str) -> Vec<Value> {
        self.pieces
            .iter()
            .enumerate()
            .map(|(index, piece)| match piece {
                Piece::Text(text) => text_block(session, &format!("live.{}.{index}", self.turn), "text", text),
                Piece::Thinking(text) => text_block(session, &format!("live.{}.{index}", self.turn), "thinking", text),
                Piece::Tool { call, name, arguments, status, result } => {
                    let nth =
                        self.pieces[..index].iter().filter(|earlier| matches!(earlier, Piece::Tool { call: id, .. } if id == call)).count();
                    tool_block(session, self.node, call, nth, name, arguments, status, result)
                }
            })
            .collect()
    }

    fn user_message(&self, session: &str) -> Value {
        let text = if self.text.is_empty() { "（连上之前开始的这一轮）" } else { &self.text };
        let node =
            Node { role: "user", blocks: vec![text_block(session, &format!("live.{}", self.turn), "text", text)], notice: Vec::new() };
        message_json(session, self.node - 1, node, &iso(self.started))
    }

    /// 正在写的那条助手消息（标「还在生成」）
    fn message(&self, session: &str) -> Value {
        let node = Node { role: "assistant", blocks: self.blocks(session), notice: self.notice.clone() };
        let mut message = message_json(session, self.node, node, &iso(self.started));
        // 报过用量的按报的；正在写的这段按估的、从第一个字算起
        let running = self.first_output.map_or(0.0, |first| first.elapsed().as_secs_f64());
        let (tokens, seconds) = (self.tokens + self.pending, self.seconds + running);
        let speed = if seconds > 0.0 { tokens / seconds } else { 0.0 };
        message["stats"] = json!({ "tokensPerSecond": (speed * 10.0).round() / 10.0, "tokenCount": tokens.round() as i64, "live": true, "startedAt": iso(self.started), "engineName": engine()["title"] });
        message
    }

    /// 估一段字有多少 token（汉字等一个字算一个，别的四个字符算一个）：服务商每段请求做完才报用量，流式时先按这个显示
    fn estimate(text: &str) -> f64 {
        text.chars().map(|c| if c.is_ascii() { 0.25 } else { 1.0 }).sum()
    }

    fn push_text(&mut self, thinking: bool, text: &str) {
        self.first_output.get_or_insert_with(Instant::now);
        self.pending += Self::estimate(text);
        match self.pieces.last_mut() {
            Some(Piece::Text(existing)) if !thinking => existing.push_str(text),
            Some(Piece::Thinking(existing)) if thinking => existing.push_str(text),
            _ => self.pieces.push(if thinking { Piece::Thinking(text.into()) } else { Piece::Text(text.into()) }),
        }
    }

    /// 最近一次用这个调用 id 的卡片
    fn tool_mut(&mut self, call: &str) -> Option<&mut Piece> {
        self.pieces.iter_mut().rev().find(|piece| matches!(piece, Piece::Tool { call: id, .. } if id == call))
    }

    /// 最近一次用这个调用 id 的卡片的 id
    fn card_id(&self, session: &str, call: &str) -> String {
        let uses = self.pieces.iter().filter(|piece| matches!(piece, Piece::Tool { call: id, .. } if id == call)).count();
        call_uuid(session, self.node, call, uses.saturating_sub(1))
    }
}

// MARK: - 往客户端写

static OUT: LazyLock<Mutex<std::io::Stdout>> = LazyLock::new(|| Mutex::new(std::io::stdout()));

fn send(message: &Value) {
    let mut out = OUT.lock().unwrap();
    let _ = writeln!(out, "{message}");
    let _ = out.flush();
}

/// EDITH_FRIDAY_TRACE=1：每个请求、每个事件都记一行（标准错误；出错的请求总是记）
static TRACE: LazyLock<bool> = LazyLock::new(|| std::env::var("EDITH_FRIDAY_TRACE").is_ok_and(|value| !value.is_empty() && value != "0"));

fn event(method: &str, params: Value) {
    if *TRACE && method != "message" {
        log(&format!("  → {method} {}", params["conversationID"].as_str().or(params["conversation"]["id"].as_str()).unwrap_or("")));
    }
    send(&json!({ "jsonrpc": "2.0", "method": method, "params": params }));
}

// MARK: - 大脑

/// 连着的一个 EDITH 大脑（一个工作目录）
struct Brain {
    workspace: String,
    writer: Mutex<TcpStream>,
    pending: Mutex<HashMap<u64, Sender<Result<Value, Value>>>>,
    next: AtomicU64,
}

impl Brain {
    fn write(&self, message: &Value) -> bool {
        let mut writer = self.writer.lock().unwrap();
        writeln!(writer, "{message}").and_then(|_| writer.flush()).is_ok()
    }

    fn request(&self, method: &str, params: Value, timeout: Duration) -> Result<Value, String> {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = mpsc::channel();
        self.pending.lock().unwrap().insert(id, sender);
        if !self.write(&json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params })) {
            return Err("和 EDITH 的连接断了".into());
        }
        match receiver.recv_timeout(timeout) {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(error)) => Err(error["message"].as_str().unwrap_or("EDITH 回了错误").to_string()),
            Err(_) => {
                self.pending.lock().unwrap().remove(&id);
                Err(format!("EDITH 等了 {} 秒没回话", timeout.as_secs()))
            }
        }
    }
}

/// 一件在等客户端批准的事
struct Pending {
    brain: Arc<Brain>,
    request: Value,
    session: String,
    call: String,
}

#[derive(Default)]
struct State {
    greeted: bool,
    brains: HashMap<String, Arc<Brain>>,
    subscribed: HashSet<String>,
    live: HashMap<String, Live>,
    generating: HashSet<String>,
    stopping: HashSet<String>,
    /// 事件 id（工具卡片的 UUID）→ 在等的批准
    approvals: HashMap<String, Pending>,
    /// 正在流式写、该推一次 message 的会话
    dirty: HashSet<String>,
    meta: Meta,
}

static STATE: LazyLock<Mutex<State>> = LazyLock::new(|| Mutex::new(State { meta: Meta::load(), ..Default::default() }));

fn engine() -> Value {
    let config: Value =
        std::fs::read(edith_home().join("config.json")).ok().and_then(|bytes| serde_json::from_slice(&bytes).ok()).unwrap_or(json!({}));
    let model = std::env::var("EDITH_MODEL").ok().or_else(|| config["model"].as_str().map(str::to_string)).unwrap_or_default();
    let base = std::env::var("EDITH_BASE_URL").ok().or_else(|| config["baseURL"].as_str().map(str::to_string)).unwrap_or_default();
    json!({
        "id": ENGINE_ID,
        "title": if model.is_empty() { "EDITH".to_string() } else { format!("EDITH · {model}") },
        "baseURL": base,
        "apiKeySecret": "EDITH_API_KEY",
        "model": model,
        "supportsVision": false,
        "supportsTools": true,
        "sendsEnableThinking": false,
        "extraBody": "",
    })
}

fn engines_state() -> Value {
    json!({ "engines": [engine()], "defaultEngineID": ENGINE_ID, "disallowedSubAgentEngines": [], "disabledServices": [] })
}

/// 标题：存档里的；还没有（第一轮做完才存）时先用正在干的这一轮的原话（同 EDITH 起的标题：前 40 个字）
fn title(session: &Stored, state: &State) -> String {
    if !session.title.is_empty() {
        return session.title.clone();
    }
    match state.live.get(&session.id).filter(|live| !live.text.trim().is_empty()) {
        Some(live) => live.text.replace('\n', " ").chars().take(40).collect(),
        None => "新对话".into(),
    }
}

fn summary(session: &Stored, state: &State) -> Value {
    let live = state.live.contains_key(&session.id);
    let mut value = json!({
        "id": upper(&session.id),
        "title": title(session, state),
        "updatedAt": session.updated.map(iso).unwrap_or_else(|| iso_text(&session.created)),
        "pinned": state.meta.pinned(&session.id),
        "isEmpty": session.history.is_empty() && !live,
        "hasDraft": state.meta.draft(&session.id).is_some(),
        "permissionMode": state.meta.setting(&session.id, "permissionMode").unwrap_or_else(|| "auto".into()),
        "engineID": ENGINE_ID,
    });
    if !session.cwd.is_empty() {
        value["workingDirectory"] = json!(session.cwd);
        if let Some(name) = Path::new(&session.cwd).file_name() {
            value["workingDirectoryName"] = json!(name.to_string_lossy());
        }
    }
    if let Some(thinking) = state.meta.setting(&session.id, "thinking") {
        value["thinking"] = json!(thinking);
    }
    value
}

fn summaries(state: &State) -> Value {
    let mut sessions = all_sessions();
    sessions.sort_by_key(|session| std::cmp::Reverse(session.updated));
    Value::Array(sessions.iter().map(|session| summary(session, state)).collect())
}

fn runner_state(state: &State) -> Value {
    let pending: HashSet<String> = state.approvals.values().map(|pending| upper(&pending.session)).collect();
    json!({
        "generating": state.generating.iter().map(|id| upper(id)).collect::<Vec<_>>(),
        "stopping": state.stopping.iter().map(|id| upper(id)).collect::<Vec<_>>(),
        "pendingApproval": pending.into_iter().collect::<Vec<_>>(),
        "pendingQuestion": [],
        "backgroundAgents": [],
        "liveAgents": state.generating.len(),
        "liveSpeed": 0,
        "queueRevision": 0,
    })
}

fn runtime(session: &Stored, state: &State) -> Value {
    let mut value = json!({
        "generating": state.generating.contains(&session.id),
        "stopping": state.stopping.contains(&session.id),
        "queue": [],
        "queuePaused": false,
        "pendingApproval": state.approvals.values().any(|pending| pending.session == session.id),
        "canRetry": false,
        "pendingTurnChanges": { "model": false, "thinking": false },
        "canSend": true,
        "engine": engine(),
        "thinkingLevel": state.meta.setting(&session.id, "thinking").unwrap_or_else(|| "off".into()),
        "thinkingLevels": [],
        "permissionMode": state.meta.setting(&session.id, "permissionMode").unwrap_or_else(|| "auto".into()),
        "mainLiveSpeed": 0,
    });
    if !session.cwd.is_empty() {
        value["workingDirectory"] = json!(session.cwd);
    }
    value
}

fn conversation(session: &Stored, state: &State) -> Value {
    let (nodes, selected) = messages(session, state.live.get(&session.id));
    let mut value = json!({
        "id": upper(&session.id),
        "title": title(session, state),
        "createdAt": iso_text(&session.created),
        "updatedAt": session.updated.map(iso).unwrap_or_else(|| iso_text(&session.created)),
        "nodes": nodes,
        "selectedChildren": selected,
        "permissionMode": state.meta.setting(&session.id, "permissionMode").unwrap_or_else(|| "auto".into()),
        "pinned": state.meta.pinned(&session.id),
        "engineID": ENGINE_ID,
    });
    if !session.cwd.is_empty() {
        value["workingDirectory"] = json!(session.cwd);
    }
    if let Some(draft) = state.meta.draft(&session.id) {
        value["draft"] = json!(draft);
    }
    value
}

/// 会话变了：推会话列表；订着的再推整段和运行状态
fn changed(session_id: &str, structure: bool) {
    let state = STATE.lock().unwrap();
    if !state.greeted {
        return;
    }
    event("summaries", json!({ "summaries": summaries(&state) }));
    event("runner", runner_state(&state));
    if state.subscribed.contains(session_id)
        && let Some(session) = find_session(session_id)
    {
        if structure {
            event("conversation", json!({ "conversation": conversation(&session, &state) }));
        }
        event("runtime", json!({ "conversationID": upper(session_id), "runtime": runtime(&session, &state) }));
    }
}

/// 订着的会话里正在写的那条助手消息
fn push_live_message(session_id: &str) {
    let state = STATE.lock().unwrap();
    if !state.subscribed.contains(session_id) {
        return;
    }
    // 只拼这一条（不读存档：流式时每 0.1 秒一次）
    if let Some(live) = state.live.get(session_id) {
        event("message", json!({ "conversationID": upper(session_id), "message": live.message(session_id) }));
    }
}

/// 大脑推来的事件
fn on_brain_event(params: &Value) {
    let session = params["session"].as_str().unwrap_or("").to_string();
    let turn = params["turn"].as_str().unwrap_or("").to_string();
    let data = &params["data"];
    // 客人（中转的远程设备）的会话不在 Friday 的列表里
    if find_session(&session).is_none() {
        return;
    }
    let kind = params["type"].as_str().unwrap_or("");
    match kind {
        "turn.started" => {
            {
                let mut state = STATE.lock().unwrap();
                state.live.insert(session.clone(), Live::start(&session, &turn, data["text"].as_str().unwrap_or("")));
                state.generating.insert(session.clone());
            }
            changed(&session, true);
        }
        "text" | "thinking" | "tool" | "result" | "notice" | "stats" => {
            let mut state = STATE.lock().unwrap();
            // 连上之前就开始的一轮：补一个
            if !state.live.contains_key(&session) {
                let live = Live::start(&session, &turn, "");
                state.live.insert(session.clone(), live);
                state.generating.insert(session.clone());
            }
            let live = state.live.get_mut(&session).unwrap();
            match kind {
                "text" => live.push_text(false, data["text"].as_str().unwrap_or("")),
                "thinking" => live.push_text(true, data["text"].as_str().unwrap_or("")),
                "tool" => live.pieces.push(Piece::Tool {
                    call: data["id"].as_str().unwrap_or("").to_string(),
                    name: data["name"].as_str().unwrap_or("").to_string(),
                    arguments: data["arguments"].clone(),
                    status: "running".into(),
                    result: String::new(),
                }),
                "stats" => {
                    // 这段请求做完了：估的换成报的
                    live.tokens += data["generatedTokens"].as_f64().unwrap_or(live.pending);
                    live.seconds += data["generateTime"]
                        .as_f64()
                        .unwrap_or_else(|| live.first_output.map_or(0.0, |first| first.elapsed().as_secs_f64()));
                    live.pending = 0.0;
                    live.first_output = None;
                }
                "result" => {
                    let ok = data["ok"].as_bool().unwrap_or(true);
                    let text = data["text"].as_str().unwrap_or("").to_string();
                    if let Some(Piece::Tool { status, result, .. }) = live.tool_mut(data["id"].as_str().unwrap_or("")) {
                        *status = if ok { "succeeded" } else { "failed" }.into();
                        *result = text;
                    }
                }
                _ => {
                    let text = data["text"].as_str().unwrap_or("").to_string();
                    if !text.is_empty() && !live.notice.contains(&text) {
                        live.notice.push(text);
                    }
                }
            }
            state.dirty.insert(session);
        }
        "done" => {
            {
                let mut state = STATE.lock().unwrap();
                // 排着的下一轮马上会 turn.started：这里先收掉这一轮
                state.live.remove(&session);
                state.generating.remove(&session);
                state.stopping.remove(&session);
                state.dirty.remove(&session);
                state.approvals.retain(|_, pending| pending.session != session);
            }
            changed(&session, true);
        }
        _ => {}
    }
}

/// 大脑要批准一件事：变成 Friday 的「待确认」
fn on_brain_approve(brain: &Arc<Brain>, request: &Value) {
    let params = &request["params"];
    let session = params["session"].as_str().unwrap_or("").to_string();
    let call = params["call"].as_str().unwrap_or("").to_string();
    {
        let mut state = STATE.lock().unwrap();
        if !state.live.contains_key(&session) {
            let live = Live::start(&session, params["turn"].as_str().unwrap_or(""), "");
            state.live.insert(session.clone(), live);
            state.generating.insert(session.clone());
        }
        let live = state.live.get_mut(&session).unwrap();
        let event_id = live.card_id(&session, &call);
        if let Some(Piece::Tool { status, .. }) = live.tool_mut(&call) {
            *status = "awaitingApproval".into();
        }
        state.approvals.insert(event_id, Pending { brain: brain.clone(), request: request.clone(), session: session.clone(), call });
        state.dirty.insert(session.clone());
    }
    push_live_message(&session);
    event("attention", json!({}));
    changed(&session, false);
}

fn handle_brain_message(brain: &Arc<Brain>, message: Value) {
    let method = message.get("method").and_then(Value::as_str);
    let id = message.get("id").filter(|id| !id.is_null()).cloned();
    match (method, id) {
        (Some("initialize"), Some(id)) => {
            brain.write(&json!({ "jsonrpc": "2.0", "id": id, "result": {
                "protocolVersion": "1",
                "name": "friday",
                "capability": { "title": "Friday 客户端", "version": VERSION, "description": "经 edith-friday 连进来的 Friday（Qt / Swift）" },
                "senses": [{ "kind": "message", "description": "Friday 里打的字" }],
                "actions": [],
                "receives": ["*"],
                "sessions": "all",
                "approves": true,
            }}));
        }
        (Some("initialized"), None) => {}
        (Some("event"), None) => on_brain_event(&message["params"]),
        (Some("approve"), Some(_)) => on_brain_approve(brain, &message),
        (Some("ping"), Some(id)) => {
            brain.write(&json!({ "jsonrpc": "2.0", "id": id, "result": {} }));
        }
        (Some("shutdown"), None) => {}
        (Some(other), Some(id)) => {
            brain.write(&json!({ "jsonrpc": "2.0", "id": id, "error": { "code": -32601, "message": format!("Friday 网关没有 {other}") } }));
        }
        (None, Some(id)) => {
            let sender = id.as_u64().and_then(|id| brain.pending.lock().unwrap().remove(&id));
            if let Some(sender) = sender {
                let outcome = match message.get("error") {
                    Some(error) => Err(error.clone()),
                    None => Ok(message.get("result").cloned().unwrap_or(Value::Null)),
                };
                let _ = sender.send(outcome);
            }
        }
        _ => {}
    }
}

/// 登记里在跑的大脑（`~/.edith/instances/`）：工作目录 → 登记
fn registry() -> Vec<Value> {
    let mut entries: Vec<Value> = std::fs::read_dir(edith_home().join("instances"))
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| serde_json::from_slice::<Value>(&std::fs::read(entry.path()).ok()?).ok())
        .filter(|entry| {
            entry["address"]
                .as_str()
                .and_then(|address| address.parse().ok())
                .is_some_and(|address| TcpStream::connect_timeout(&address, Duration::from_millis(500)).is_ok())
        })
        .collect();
    entries.sort_by(|a, b| b["started"].as_str().cmp(&a["started"].as_str()));
    entries
}

fn connect_brain(entry: &Value) -> Result<Arc<Brain>, String> {
    let address = entry["address"].as_str().ok_or("登记里没有地址")?;
    let mut stream = TcpStream::connect(address).map_err(|error| format!("连不上 EDITH {address}：{error}"))?;
    writeln!(stream, "{}", json!({ "jsonrpc": "2.0", "method": "auth", "params": { "token": entry["token"] } }))
        .map_err(|error| error.to_string())?;
    let reading = stream.try_clone().map_err(|error| error.to_string())?;
    let brain = Arc::new(Brain {
        workspace: entry["workspace"].as_str().unwrap_or("").to_string(),
        writer: Mutex::new(stream),
        pending: Mutex::new(HashMap::new()),
        next: AtomicU64::new(1),
    });
    let (ready_sender, ready) = mpsc::channel::<()>();
    {
        let brain = brain.clone();
        std::thread::spawn(move || {
            let mut ready_sender = Some(ready_sender);
            for line in BufReader::new(reading).lines().map_while(Result::ok) {
                let Ok(message) = serde_json::from_str::<Value>(&line) else { continue };
                if message["method"] == "initialized" {
                    if let Some(sender) = ready_sender.take() {
                        let _ = sender.send(());
                    }
                    continue;
                }
                handle_brain_message(&brain, message);
            }
            // 断了：摘掉；它在干的那几轮当结束了
            let workspace = brain.workspace.clone();
            let mut state = STATE.lock().unwrap();
            if state.brains.get(&workspace).is_some_and(|current| Arc::ptr_eq(current, &brain)) {
                state.brains.remove(&workspace);
            }
            state.approvals.retain(|_, pending| !Arc::ptr_eq(&pending.brain, &brain));
        });
    }
    ready.recv_timeout(Duration::from_secs(30)).map_err(|_| "EDITH 没握手".to_string())?;
    Ok(brain)
}

/// EDITH 程序在哪：同一个目录里的（装在一起的）→ EDITH_BIN → PATH 里的 edith
fn edith_program() -> PathBuf {
    if let Ok(program) = std::env::var("EDITH_BIN")
        && !program.is_empty()
    {
        return PathBuf::from(program);
    }
    let name = if cfg!(windows) { "edith.exe" } else { "edith" };
    if let Some(dir) = std::env::current_exe().ok().and_then(|exe| exe.parent().map(Path::to_path_buf))
        && dir.join(name).is_file()
    {
        return dir.join(name);
    }
    PathBuf::from(name)
}

/// 在这个工作目录起一个大脑（脱离这次 SSH：断了照常干活），等它登记
fn launch_brain(workspace: &Path) -> Result<Value, String> {
    let logs = edith_home().join("logs");
    let _ = std::fs::create_dir_all(&logs);
    let log_file = std::fs::File::create(logs.join(format!("brain-{}.log", std::process::id()))).map_err(|error| error.to_string())?;
    let mut command = std::process::Command::new(edith_program());
    command
        .args(["run", "--idle"])
        .current_dir(workspace)
        .stdin(std::process::Stdio::null())
        .stdout(log_file.try_clone().map_err(|error| error.to_string())?)
        .stderr(log_file);
    #[cfg(unix)]
    unsafe {
        use std::os::unix::process::CommandExt;
        command.pre_exec(|| {
            libc::setsid();
            Ok(())
        });
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        // 不挂在这次 SSH 的作业里、不开窗口
        const DETACHED_PROCESS: u32 = 0x0000_0008;
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
        const CREATE_BREAKAWAY_FROM_JOB: u32 = 0x0100_0000;
        command.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP | CREATE_BREAKAWAY_FROM_JOB);
        // 作业不许脱离（有的 SSH 服务这样）：退回不脱离（SSH 断了大脑可能跟着结束，下次连上来再起）
        if command.spawn().is_err() {
            command.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP);
            command.spawn().map_err(|error| format!("起不了 EDITH（{}）：{error}", edith_program().display()))?;
        }
    }
    #[cfg(not(windows))]
    command.spawn().map_err(|error| format!("起不了 EDITH（{}）：{error}", edith_program().display()))?;
    let wanted = workspace.to_string_lossy().into_owned();
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if let Some(entry) = registry().into_iter().find(|entry| entry["workspace"].as_str() == Some(wanted.as_str())) {
            return Ok(entry);
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    Err(format!("{} 里的 EDITH 30 秒内没起来（日志在 {}）", wanted, logs.display()))
}

/// 这个目录的大脑：连着的、登记着的，都没有就起一个
fn brain_for(directory: &str) -> Result<Arc<Brain>, String> {
    let workspace = find_workspace(Path::new(directory));
    let key = workspace.to_string_lossy().into_owned();
    if let Some(brain) = STATE.lock().unwrap().brains.get(&key).cloned() {
        return Ok(brain);
    }
    let entry = match registry().into_iter().find(|entry| entry["workspace"].as_str() == Some(key.as_str())) {
        Some(entry) => entry,
        None => launch_brain(&workspace)?,
    };
    let brain = connect_brain(&entry)?;
    STATE.lock().unwrap().brains.insert(key, brain.clone());
    Ok(brain)
}

fn default_directory() -> String {
    std::env::var("EDITH_FRIDAY_DIRECTORY").ok().filter(|dir| !dir.is_empty()).unwrap_or_else(|| user_home().to_string_lossy().into_owned())
}

// MARK: - 客户端的请求

type Reply = Result<Value, (i64, String)>;

fn not_allowed(what: &str) -> (i64, String) {
    (NOT_ALLOWED, format!("这一版的 EDITH 还不支持{what}"))
}

fn text_param(params: &Value, key: &str) -> Result<String, (i64, String)> {
    params[key].as_str().map(str::to_string).filter(|value| !value.is_empty()).ok_or((-32602, format!("缺少参数 {key}")))
}

fn server_info() -> Value {
    let user = std::env::var("USER").or_else(|_| std::env::var("USERNAME")).unwrap_or_default();
    static STARTED: LazyLock<String> = LazyLock::new(|| iso(SystemTime::now()));
    json!({
        // 0.0.0：客户端不对它自动升级（它会想换成 Friday 自己的服务端包）
        "version": "0.0.0",
        "edith": VERSION,
        "platform": match std::env::consts::OS { "macos" => "macos", "windows" => "windows", _ => "linux" },
        "arch": match std::env::consts::ARCH { "aarch64" => "arm64", "x86_64" => "x64", other => other },
        "hostname": hostname(),
        "user": user,
        "home": user_home().to_string_lossy(),
        "dataDir": edith_home().to_string_lossy(),
        "defaultWorkingDirectory": default_directory(),
        "pid": std::process::id(),
        "startedAt": *STARTED,
        "host": "server",
        "clientDevices": { "power": false },
        "features": [],
    })
}

fn hostname() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .or_else(|| std::env::var("COMPUTERNAME").ok())
        .or_else(|| {
            std::process::Command::new("hostname").output().ok().map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        })
        .unwrap_or_default()
}

fn directories_state(meta: &Meta) -> Value {
    json!({ "recent": meta.recent().iter().map(|path| json!({ "path": path, "exists": Path::new(path).is_dir() })).collect::<Vec<_>>() })
}

fn hello(params: &Value) -> Reply {
    let protocol = params["protocol"].as_i64().unwrap_or(0);
    if protocol != PROTOCOL {
        return Err((
            1,
            format!("协议版本不一致：本机 Friday 用第 {protocol} 版，这台的 EDITH 网关用第 {PROTOCOL} 版。请把两边换成同一版本。"),
        ));
    }
    let mut state = STATE.lock().unwrap();
    state.greeted = true;
    log(&format!("hello：{}", params["client"]));
    Ok(json!({
        "protocol": PROTOCOL,
        "server": server_info(),
        "trusted": true,
        "state": {
            "summaries": summaries(&state),
            "runner": runner_state(&state),
            "team": { "runs": [] },
            "engines": engines_state(),
            "secrets": { "names": [] },
            "directories": directories_state(&state.meta),
            "terminals": { "terminals": [], "available": false, "reason": "这一版的 EDITH 还没有内置终端" },
            "browser": { "tabs": [], "available": false, "reason": "这一版的 EDITH 还没有内置浏览器" },
        },
    }))
}

fn handle(method: &str, params: &Value) -> Reply {
    if method == "hello" {
        return hello(params);
    }
    if !STATE.lock().unwrap().greeted {
        return Err((NOT_GREETED, "先发 hello".into()));
    }
    match method {
        "conversation.subscribe" => {
            let id = lower(&text_param(params, "id")?);
            let session = find_session(&id).ok_or((NOT_FOUND, "没有这段会话".into()))?;
            let mut state = STATE.lock().unwrap();
            state.subscribed.insert(id);
            Ok(json!({ "conversation": conversation(&session, &state), "runtime": runtime(&session, &state) }))
        }
        "conversation.unsubscribe" => {
            STATE.lock().unwrap().subscribed.remove(&lower(&text_param(params, "id")?));
            Ok(json!({}))
        }
        "message.get" => {
            let id = lower(&text_param(params, "conversationID")?);
            let message_id = text_param(params, "messageID")?;
            let session = find_session(&id).ok_or((NOT_FOUND, "没有这段会话".into()))?;
            let state = STATE.lock().unwrap();
            let (nodes, _) = messages(&session, state.live.get(&id));
            let message = nodes
                .into_iter()
                .find(|node| node["id"].as_str() == Some(message_id.as_str()))
                .ok_or((NOT_FOUND, "没有这条消息".into()))?;
            Ok(json!({ "message": message, "version": 0 }))
        }
        "conversation.create" => {
            let id = lower(&text_param(params, "id")?);
            if find_session(&id).is_some() {
                return Ok(json!({ "id": upper(&id) }));
            }
            let directory =
                params["workingDirectory"].as_str().filter(|dir| !dir.is_empty()).map(str::to_string).unwrap_or_else(default_directory);
            if !Path::new(&directory).is_dir() {
                return Err((-32602, format!("工作目录不存在：{directory}")));
            }
            let brain = brain_for(&directory).map_err(|error| (NOT_ALLOWED, error))?;
            let mut create = json!({ "id": id, "cwd": directory });
            if let Some(title) = params["title"].as_str() {
                create["title"] = json!(title);
            }
            brain.request("sessions.create", create, Duration::from_secs(30)).map_err(|error| (NOT_ALLOWED, error))?;
            {
                let mut state = STATE.lock().unwrap();
                state.meta.push_recent(&directory);
                for key in ["permissionMode", "thinking"] {
                    if let Some(value) = params[key].as_str() {
                        state.meta.set_setting(&id, key, value);
                    }
                }
            }
            if let Some(title) = params["title"].as_str().filter(|title| !title.is_empty())
                && let Some(session) = find_session(&id)
            {
                append_title(&session, title);
            }
            changed(&id, true);
            event("directories", directories_state(&STATE.lock().unwrap().meta));
            Ok(json!({ "id": upper(&id) }))
        }
        "conversation.remove" => {
            let id = lower(&text_param(params, "id")?);
            let session = find_session(&id).ok_or((NOT_FOUND, "没有这段会话".into()))?;
            // 在干活的先停下
            let brain = STATE.lock().unwrap().brains.get(&find_workspace(Path::new(&session.cwd)).to_string_lossy().into_owned()).cloned();
            if let Some(brain) = brain {
                let _ = brain.request("sessions.stop", json!({ "id": id }), Duration::from_secs(10));
            }
            let _ = std::fs::remove_file(&session.path);
            if let Some(dir) = session.path.parent().and_then(Path::parent) {
                let _ = std::fs::remove_file(dir.join("reads").join(format!("{id}.json")));
            }
            {
                let mut state = STATE.lock().unwrap();
                state.subscribed.remove(&id);
                state.meta.forget(&id);
            }
            changed(&id, false);
            Ok(json!({}))
        }
        "conversation.rename" => {
            let id = lower(&text_param(params, "id")?);
            let title = params["title"].as_str().unwrap_or("").to_string();
            let session = find_session(&id).ok_or((NOT_FOUND, "没有这段会话".into()))?;
            append_title(&session, &title);
            changed(&id, true);
            Ok(json!({}))
        }
        "conversation.setPinned" => {
            let id = lower(&text_param(params, "id")?);
            STATE.lock().unwrap().meta.set_pinned(&id, params["pinned"].as_bool().unwrap_or(false));
            changed(&id, true);
            Ok(json!({}))
        }
        "conversation.setDraft" => {
            let id = lower(&text_param(params, "id")?);
            STATE.lock().unwrap().meta.set_draft(&id, params["draft"].as_str().unwrap_or(""));
            changed(&id, false);
            Ok(json!({}))
        }
        "runner.submit" => {
            let id = lower(&text_param(params, "conversationID")?);
            let mut text = params["text"].as_str().unwrap_or("").trim().to_string();
            let attachments: Vec<&str> = params["attachments"].as_array().into_iter().flatten().filter_map(Value::as_str).collect();
            if !attachments.is_empty() {
                text.push_str(&format!("\n\n（附件，在这台机器上：{}）", attachments.join("、")));
            }
            if text.is_empty() {
                return Err((-32602, "text 是空的".into()));
            }
            let session = find_session(&id).ok_or((NOT_FOUND, "没有这段会话".into()))?;
            let directory = if session.cwd.is_empty() { default_directory() } else { session.cwd.clone() };
            let brain = brain_for(&directory).map_err(|error| (NOT_ALLOWED, error))?;
            // 忙的时候：默认同输入框（补充进这一轮），whenBusy: queue 排到下一轮
            let urgency = if params["whenBusy"].as_str() == Some("queue") { "queue" } else { "interject" };
            brain
                .request("sense", json!({ "session": id, "text": text, "urgency": urgency, "kind": "user" }), Duration::from_secs(30))
                .map_err(|error| (NOT_ALLOWED, error))?;
            STATE.lock().unwrap().meta.set_draft(&id, "");
            Ok(json!({}))
        }
        "runner.stop" => {
            let id = lower(&text_param(params, "conversationID")?);
            let session = find_session(&id).ok_or((NOT_FOUND, "没有这段会话".into()))?;
            let brain = STATE.lock().unwrap().brains.get(&find_workspace(Path::new(&session.cwd)).to_string_lossy().into_owned()).cloned();
            if let Some(brain) = brain {
                STATE.lock().unwrap().stopping.insert(id.clone());
                changed(&id, false);
                brain.request("sessions.stop", json!({ "id": id }), Duration::from_secs(10)).map_err(|error| (NOT_ALLOWED, error))?;
            }
            Ok(json!({}))
        }
        "runner.stopAll" => {
            let (brains, working): (Vec<Arc<Brain>>, Vec<String>) = {
                let state = STATE.lock().unwrap();
                (state.brains.values().cloned().collect(), state.generating.iter().cloned().collect())
            };
            for id in working {
                for brain in &brains {
                    let _ = brain.request("sessions.stop", json!({ "id": id }), Duration::from_secs(10));
                }
            }
            Ok(json!({}))
        }
        "runner.resolveApproval" => {
            let event_id = upper(&text_param(params, "eventID")?);
            let approved = params["approved"].as_bool().unwrap_or(false);
            let pending = STATE.lock().unwrap().approvals.remove(&event_id).ok_or((NOT_FOUND, "会话已经不在等这个了".into()))?;
            let decision = if approved { json!({ "decision": "allow" }) } else { json!({ "decision": "deny", "reason": "" }) };
            pending.brain.write(&json!({ "jsonrpc": "2.0", "id": pending.request["id"], "result": decision }));
            {
                let mut state = STATE.lock().unwrap();
                if let Some(live) = state.live.get_mut(&pending.session)
                    && let Some(Piece::Tool { status, .. }) = live.tool_mut(&pending.call)
                {
                    *status = if approved { "running" } else { "denied" }.into();
                }
                state.dirty.insert(pending.session.clone());
            }
            changed(&pending.session, false);
            Ok(json!({}))
        }
        // 只记下（界面上显示选了什么），这一版的 EDITH 不按它们干活
        "runner.setPermissionMode" | "runner.setThinking" | "runner.setEngine" | "runner.setTeamMode" => {
            let id = lower(&text_param(params, "conversationID")?);
            let (key, value) = match method {
                "runner.setPermissionMode" => ("permissionMode", params["mode"].as_str()),
                "runner.setThinking" => ("thinking", params["level"].as_str()),
                "runner.setTeamMode" => ("teamMode", params["mode"].as_str()),
                _ => ("engineID", params["engineID"].as_str()),
            };
            if let Some(value) = value {
                STATE.lock().unwrap().meta.set_setting(&id, key, value);
            }
            changed(&id, true);
            Ok(json!({}))
        }
        "directories.get" => Ok(directories_state(&STATE.lock().unwrap().meta)),
        "directories.push" => {
            STATE.lock().unwrap().meta.push_recent(&text_param(params, "path")?);
            event("directories", directories_state(&STATE.lock().unwrap().meta));
            Ok(json!({}))
        }
        "directories.remove" => {
            STATE.lock().unwrap().meta.remove_recent(&text_param(params, "path")?);
            event("directories", directories_state(&STATE.lock().unwrap().meta));
            Ok(json!({}))
        }
        "conversation.search" => {
            let query = params["query"].as_str().unwrap_or("").to_lowercase();
            let hits: Vec<Value> = all_sessions()
                .into_iter()
                .filter(|session| {
                    !query.is_empty()
                        && (session.title.to_lowercase().contains(&query)
                            || session.history.iter().any(|message| user_text(message).to_lowercase().contains(&query)))
                })
                .map(|session| json!({ "conversationID": upper(&session.id), "title": session.title, "snippet": session.title }))
                .collect();
            Ok(json!({ "hits": hits }))
        }
        "server.status" => Ok(server_info()),
        "server.shutdown" => {
            // 只结束这个网关（大脑接着跑，下次连上来照常）
            std::thread::spawn(|| {
                std::thread::sleep(Duration::from_millis(200));
                std::process::exit(0);
            });
            Ok(json!({}))
        }
        "conversation.fork" => Err(not_allowed("从某条消息分叉")),
        "runner.retry" | "runner.resetSession" | "runner.selectBranch" => Err(not_allowed("重试、分支")),
        "runner.removeQueued" | "runner.resumeQueue" | "runner.cancelQuotaWait" | "runner.setWorkingDirectory" => {
            Err(not_allowed("这个操作"))
        }
        "agent.stop" | "agent.send" | "agent.removeQueued" => Err(not_allowed("在 Friday 里管子智能体")),
        "secrets.set" | "secrets.remove" | "engines.update" | "engines.remove" | "engines.setSubAgentAllowed" => {
            Err(not_allowed("在这里改密钥和模型（改 ~/.edith/config.json）"))
        }
        other => Err((-32601, format!("这一版的 EDITH 网关没有 {other}"))),
    }
}

/// 改标题：往会话存档追加一行 title（只追加的格式，大脑那边照样能读）
fn append_title(session: &Stored, title: &str) {
    if let Ok(mut file) = std::fs::OpenOptions::new().append(true).open(&session.path) {
        let _ = writeln!(file, "{}", json!({ "type": "title", "title": title }));
    }
}

/// 正在流式写的消息：每 0.1 秒最多推一次
fn flush_loop() {
    loop {
        std::thread::sleep(Duration::from_millis(100));
        let dirty: Vec<String> = std::mem::take(&mut STATE.lock().unwrap().dirty).into_iter().collect();
        for session in dirty {
            push_live_message(&session);
        }
    }
}

fn main() {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    if arguments.iter().any(|argument| argument == "--version" || argument == "-V") {
        println!("edith-friday {VERSION}");
        return;
    }
    // 认 `--attach`、`server --attach`（装成 friday-server、friday 时客户端这样运行），别的参数不认
    if let Some(other) = arguments.iter().find(|argument| !matches!(argument.as_str(), "--attach" | "server")) {
        log(&format!("不认识 {other}（edith-friday [--attach]：在标准输入 / 输出上讲 Friday 的远程协议）"));
        std::process::exit(2);
    }
    std::thread::spawn(flush_loop);
    // 按收到的顺序一个个处理（同 Friday 的服务端）：客户端紧接着发的 conversation.create、runner.submit 不能倒过来。
    // 慢的只有起大脑那一下（几秒）；批准、事件走大脑那条连接的线程，不排在这里
    let (sender, requests) = mpsc::channel::<Value>();
    let worker = std::thread::spawn(move || {
        for message in requests {
            let id = message.get("id").cloned().unwrap_or(Value::Null);
            let method = message["method"].as_str().unwrap_or("").to_string();
            let params = message.get("params").cloned().unwrap_or(json!({}));
            let reply = handle(&method, &params);
            match &reply {
                Err((code, message)) => log(&format!("{method} ✗ {code} {message}")),
                Ok(_) if *TRACE => log(&format!("{method} ✓")),
                Ok(_) => {}
            }
            if id.is_null() {
                continue;
            }
            match reply {
                Ok(result) => send(&json!({ "jsonrpc": "2.0", "id": id, "result": result })),
                Err((code, message)) => send(&json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })),
            }
        }
    });
    for line in std::io::stdin().lock().lines().map_while(Result::ok) {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<Value>(&line) {
            Ok(message) => {
                let _ = sender.send(message);
            }
            Err(_) => send(&json!({ "jsonrpc": "2.0", "id": null, "error": { "code": -32700, "message": "不是 JSON" } })),
        }
    }
    // 输入完了：还没回的回完再走
    drop(sender);
    let _ = worker.join();
    // 客户端走了：网关退出，大脑照常干活（在等批准的随连接断开自动回「没答上」）
}
