//! edith-friday：让 Friday 的客户端（Qt、Swift）连 EDITH——讲 Friday 的远程协议（Friday 仓库的 qt/docs/REMOTE.md），对内讲 EDITH 的能力协议。
//! 用户 2026-10-24 定：EDITH 当 Friday 的服务端，走网关，客户端不用改。
//!
//! - **一条连接一个网关**：客户端经 SSH 运行 `edith-friday --attach`（装成 `~/.friday-server/bin/friday-server` 时也认 `--attach`），
//!   网关在标准输入 / 输出上讲 Friday 协议。要一直干的活在各个目录的 EDITH 大脑里（断了照常跑），网关自己不用常驻；
//!   几个客户端各开一个网关，看到的都是大脑里的同一份。
//! - **一台机器的视图**：会话列表直接读 `~/.edith/projects/*/sessions/*.jsonl`（看会话不用起大脑）；新建的空白会话先记在
//!   `~/.edith/friday.json`，第一次发话时按 EDITH 的规则算出它的工作目录（往上找 .edith / 到 git 根），`instances/` 里登记着就连上，
//!   没有就在那里起 `edith run --idle --linger`（脱离这次 SSH；没人连着、闲半小时自己退）。连上来时连上全部在跑的大脑，接着显示在干的那几轮。
//! - **翻译**：EDITH 的历史（OpenAI 格式）换成 Friday 的 Conversation / ChatMessage（用户消息、助手消息里的文字 / 思考 / 工具卡片）；
//!   id 由会话 id 和位置算出来（同一条消息每次都是同一个 UUID）；正在干的那一轮按大脑推来的事件拼出来（干活时补充的话另起一段），
//!   做完再按存档重发整段。
//! - **批准**：网关连大脑时说自己能批准（approves）、是全权的连接，大脑要批准的事交给它，它变成 Friday 的「待确认」
//!   （工具卡片 awaitingApproval、runner.pendingApproval、attention 事件），客户端 runner.resolveApproval 回过去。
//!   客户端关了也不要紧：一直开着的大脑等能批准的连上来再问。
//!
//! 能做：连上、看会话、新建 / 删 / 改名 / 置顶 / 草稿、选工作目录（列目录、新建文件夹）、发话（忙时补充或排队）、附件、
//! 流式看回复、上下文进度环、批准、停下。别的方法回 -32601（没有）或错误 4（这一版的 EDITH 还不支持）；
//! server.features 只报做了的，客户端就不去用别的。

use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Read, Seek, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime};

use base64::Engine;
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
/// fs.read、file.upload 一块最多多大
const CHUNK: usize = 1024 * 1024;

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

/// 真实路径（Windows 去掉 \\?\，同 EDITH）
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

fn workspace_key(directory: &str) -> String {
    find_workspace(Path::new(directory)).to_string_lossy().into_owned()
}

fn default_directory() -> String {
    std::env::var("EDITH_FRIDAY_DIRECTORY").ok().filter(|dir| !dir.is_empty()).unwrap_or_else(|| user_home().to_string_lossy().into_owned())
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
    /// 存档文件；None 是空白会话（客户端新建了、还没发过话，EDITH 里还没有，记在 friday.json）
    path: Option<PathBuf>,
}

impl Stored {
    fn blank(&self) -> bool {
        self.path.is_none()
    }

    fn updated_at(&self) -> String {
        self.updated.map(iso).unwrap_or_else(|| iso_text(&self.created))
    }
}

fn read_session(path: &Path) -> Option<Stored> {
    let text = std::fs::read_to_string(path).ok()?;
    let id = path.file_stem()?.to_string_lossy().into_owned();
    let mut session = Stored { id, path: Some(path.to_path_buf()), ..Default::default() };
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

/// EDITH 存着的这段会话（客人的不算）
fn stored_session(id: &str) -> Option<Stored> {
    let id = lower(id);
    for project in std::fs::read_dir(edith_home().join("projects")).into_iter().flatten().flatten() {
        let path = project.path().join("sessions").join(format!("{id}.jsonl"));
        if path.is_file() {
            return read_session(&path).filter(|session| session.owner.is_empty());
        }
    }
    None
}

/// 这台上全部会话：EDITH 存着的（主人的）+ 空白的，新的在前
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
    for blank in META.blanks() {
        if !sessions.iter().any(|session| session.id == blank.id) {
            sessions.push(blank);
        }
    }
    sessions.sort_by_key(|session| std::cmp::Reverse(session.updated_at()));
    sessions
}

/// 一段会话：EDITH 存着的，没有就看是不是空白的
fn find_session(id: &str) -> Option<Stored> {
    stored_session(id).or_else(|| META.blank(id))
}

/// 改标题：往会话存档追加一行 title（只追加的格式，大脑那边照样能读）
fn append_title(path: &Path, title: &str) {
    if let Ok(mut file) = std::fs::OpenOptions::new().append(true).open(path) {
        let _ = writeln!(file, "{}", json!({ "type": "title", "title": title }));
    }
}

// MARK: - 网关自己记的（friday.json：EDITH 没有的置顶、草稿、最近的目录、档位、空白会话、最近一轮的用量）

/// 几个客户端各开一个网关、都写这个文件：每次改都先读回最新的再改，读的时候文件变了也重新读
#[derive(Default)]
struct Meta {
    cache: Mutex<(Value, Option<SystemTime>)>,
}

static META: LazyLock<Meta> = LazyLock::new(Meta::default);

impl Meta {
    fn path() -> PathBuf {
        edith_home().join("friday.json")
    }

    fn modified() -> Option<SystemTime> {
        std::fs::metadata(Self::path()).and_then(|meta| meta.modified()).ok()
    }

    fn read() -> Value {
        std::fs::read(Self::path()).ok().and_then(|bytes| serde_json::from_slice(&bytes).ok()).unwrap_or_else(|| json!({}))
    }

    fn value(&self) -> Value {
        let mut cache = self.cache.lock().unwrap();
        let modified = Self::modified();
        if cache.1.is_none() || cache.1 != modified {
            *cache = (Self::read(), modified);
        }
        cache.0.clone()
    }

    /// 读回最新的、改、写回（先写临时文件再换名，读的一方不会读到半个）
    fn update(&self, change: impl FnOnce(&mut Value)) {
        let mut cache = self.cache.lock().unwrap();
        let mut value = Self::read();
        change(&mut value);
        let _ = std::fs::create_dir_all(edith_home());
        let temporary = Self::path().with_extension(format!("json.{}", std::process::id()));
        if std::fs::write(&temporary, serde_json::to_vec_pretty(&value).unwrap_or_default()).is_ok() {
            let _ = std::fs::rename(&temporary, Self::path());
        }
        *cache = (value, Self::modified());
    }

    fn object<'a>(value: &'a mut Value, key: &str) -> &'a mut Map<String, Value> {
        if !value[key].is_object() {
            value[key] = json!({});
        }
        value[key].as_object_mut().unwrap()
    }

    fn strings(value: &Value, key: &str) -> Vec<String> {
        value[key].as_array().into_iter().flatten().filter_map(Value::as_str).map(str::to_string).collect()
    }

    fn pinned(&self, id: &str) -> bool {
        Self::strings(&self.value(), "pinned").contains(&lower(id))
    }

    fn set_pinned(&self, id: &str, pinned: bool) {
        self.update(|value| {
            let mut ids = Self::strings(value, "pinned");
            ids.retain(|item| *item != lower(id));
            if pinned {
                ids.push(lower(id));
            }
            value["pinned"] = json!(ids);
        });
    }

    fn draft(&self, id: &str) -> Option<String> {
        self.value()["drafts"][lower(id)].as_str().filter(|draft| !draft.is_empty()).map(str::to_string)
    }

    fn set_draft(&self, id: &str, draft: &str) {
        if self.draft(id).as_deref().unwrap_or("") == draft {
            return;
        }
        self.update(|value| {
            let drafts = Self::object(value, "drafts");
            if draft.is_empty() {
                drafts.remove(&lower(id));
            } else {
                drafts.insert(lower(id), json!(draft));
            }
        });
    }

    fn setting(&self, id: &str, key: &str) -> Option<String> {
        self.value()["settings"][lower(id)][key].as_str().map(str::to_string)
    }

    fn set_setting(&self, id: &str, key: &str, setting: &str) {
        self.update(|value| {
            let entry = Self::object(value, "settings").entry(lower(id)).or_insert_with(|| json!({}));
            if !entry.is_object() {
                *entry = json!({});
            }
            entry[key] = json!(setting);
        });
    }

    fn recent(&self) -> Vec<String> {
        Self::strings(&self.value(), "recent")
    }

    fn push_recent(&self, path: &str) {
        self.update(|value| {
            let mut recent = Self::strings(value, "recent");
            recent.retain(|item| item != path);
            recent.insert(0, path.to_string());
            recent.truncate(10);
            value["recent"] = json!(recent);
        });
    }

    fn remove_recent(&self, path: &str) {
        self.update(|value| {
            let mut recent = Self::strings(value, "recent");
            recent.retain(|item| item != path);
            value["recent"] = json!(recent);
        });
    }

    fn blank_from(id: &str, entry: &Value) -> Stored {
        Stored {
            id: lower(id),
            title: entry["title"].as_str().unwrap_or("").to_string(),
            cwd: entry["cwd"].as_str().unwrap_or("").to_string(),
            created: entry["created"].as_str().unwrap_or("").to_string(),
            ..Default::default()
        }
    }

    /// 空白会话（还没发过话）
    fn blank(&self, id: &str) -> Option<Stored> {
        let entry = self.value()["blank"][lower(id)].clone();
        entry.is_object().then(|| Self::blank_from(id, &entry))
    }

    fn blanks(&self) -> Vec<Stored> {
        let value = self.value();
        value["blank"].as_object().into_iter().flatten().map(|(id, entry)| Self::blank_from(id, entry)).collect()
    }

    fn set_blank(&self, id: &str, cwd: &str, title: &str) {
        let created =
            self.blank(id).map(|blank| blank.created).filter(|created| !created.is_empty()).unwrap_or_else(|| iso(SystemTime::now()));
        self.update(|value| {
            Self::object(value, "blank").insert(lower(id), json!({ "cwd": cwd, "title": title, "created": created }));
        });
    }

    fn remove_blank(&self, id: &str) {
        if self.blank(id).is_some() {
            self.update(|value| {
                Self::object(value, "blank").remove(&lower(id));
            });
        }
    }

    /// 最近一轮做完时的用量（上下文进度环）
    fn stats(&self, id: &str) -> Option<Value> {
        let stats = self.value()["stats"][lower(id)].clone();
        stats.is_object().then_some(stats)
    }

    fn set_stats(&self, id: &str, stats: Value) {
        self.update(|value| {
            Self::object(value, "stats").insert(lower(id), stats);
        });
    }

    /// 删会话：它的这些都不要了
    fn forget(&self, id: &str) {
        self.update(|value| {
            let id = lower(id);
            if let Some(ids) = value["pinned"].as_array_mut() {
                ids.retain(|item| item.as_str() != Some(id.as_str()));
            }
            for key in ["drafts", "settings", "blank", "stats"] {
                if let Some(entries) = value[key].as_object_mut() {
                    entries.remove(&id);
                }
            }
        });
    }
}

// MARK: - 历史 → Friday 的消息

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
        "load_tools" => ("取回工具", "wrench.and.screwdriver"),
        "use_skill" => ("用技能", "book"),
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

struct Card<'a> {
    call: &'a str,
    nth: usize,
    name: &'a str,
    arguments: &'a Value,
    status: &'a str,
    result: &'a str,
}

/// 卡片在哪：主会话的第几条消息（agent 是 None），或哪个子智能体的运行记录里；team：这台上知道的子智能体
#[derive(Clone, Copy)]
struct Place<'a> {
    session: &'a str,
    node: usize,
    agent: Option<&'a str>,
    team: &'a Team,
}

/// 子智能体运行记录里的卡片的 id
fn agent_call_uuid(session: &str, agent: &str, call: &str, nth: usize) -> String {
    stable_uuid(&[session, "agent-call", agent, call, &nth.to_string()])
}

/// 一个子智能体的卡片的 id（Agent 树里它的 id 也是这个）
fn agent_card_uuid(session: &str, agent: &str) -> String {
    stable_uuid(&[session, "agent", agent])
}

fn tool_block(place: Place, card: Card) -> Value {
    let id = match place.agent {
        None => call_uuid(place.session, place.node, card.call, card.nth),
        Some(agent) => agent_call_uuid(place.session, agent, card.call, card.nth),
    };
    let mut event = json!({
        "id": id,
        "name": card.name,
        "arguments": pretty(card.arguments),
        "status": card.status,
        "result": card.result,
    });
    if let Some((title, symbol)) = tool_look(card.name) {
        event["title"] = json!(title);
        event["symbol"] = json!(symbol);
    }
    if DISPATCH.contains(&card.name) {
        attach_agents(place, card.call, card.name, &mut event);
    }
    let key = format!("{}.{}.{}.{}", place.agent.unwrap_or(""), place.node, card.call, card.nth);
    json!({ "id": stable_uuid(&[place.session, "block", &key]), "kind": { "tool": { "_0": event } } })
}

/// 派子智能体的卡片：找到它派的（同一个调用、同一个上级），把运行记录装进去。delegate_task 卡片就是那个子智能体；
/// 并联、工作流的卡片里每个子智能体一张子卡片（同 Friday 的 SubAgentRunner、Workflow）
fn attach_agents(place: Place, call: &str, name: &str, event: &mut Value) {
    let mut agents: Vec<&AgentView> = place
        .team
        .values()
        .filter(|agent| {
            agent.session() == place.session && agent.meta["call"] == json!(call) && agent.meta["parent"].as_str() == place.agent
        })
        .collect();
    if agents.is_empty() {
        return;
    }
    agents.sort_by_key(|agent| agent.meta["slot"].as_u64().unwrap_or(0));
    if name == "delegate_task" {
        fill_agent_card(place, agents[0], event);
        return;
    }
    let children: Vec<Value> = agents
        .iter()
        .map(|agent| {
            let assignment = json!({ "task": agent.meta["task"], "name": agent.meta["name"], "role": agent.meta["role"] });
            let mut child = json!({
                "name": "delegate_task", "arguments": pretty(&assignment), "status": "running", "result": "",
                "title": "子智能体", "symbol": "person.2.badge.gearshape",
            });
            fill_agent_card(place, agent, &mut child);
            json!({ "id": stable_uuid(&[place.session, "block", "agent", agent.id()]), "kind": { "tool": { "_0": child } } })
        })
        .collect();
    let label = if name == "run_workflow" {
        "工作流"
    } else if place.agent.is_none() {
        "后台"
    } else {
        "并行"
    };
    event["agentName"] = json!(format!("{label} ×{}", agents.len()));
    event["transcript"] = json!(children);
    // 整张卡片：有在跑的就是在跑，都做成了算成功，有没做成的算失败
    event["status"] = json!(if agents.iter().any(|agent| agent.running()) {
        "running"
    } else if agents.iter().all(|agent| agent.state() == "done") {
        "succeeded"
    } else {
        "failed"
    });
}

/// 一个子智能体的卡片：名字、运行记录、状态、交回的结果
fn fill_agent_card(place: Place, agent: &AgentView, event: &mut Value) {
    event["id"] = json!(agent_card_uuid(place.session, agent.id()));
    event["agentName"] = agent.meta["name"].clone();
    event["transcript"] = json!(agent_transcript(place, agent));
    let reply = agent.meta["reply"].as_str().unwrap_or("");
    match agent.state() {
        "waiting" => {
            event["status"] = json!("running");
            event["queued"] = json!(true);
        }
        "running" => event["status"] = json!("running"),
        "done" => {
            event["status"] = json!("succeeded");
            event["result"] = json!(reply);
        }
        state => {
            event["status"] = json!("failed");
            event["result"] = json!(reply);
            if state == "stopped" {
                event["stopped"] = json!(true);
            }
        }
    }
}

/// 子智能体的运行记录 → Friday 的块（它往下派的照样装进卡片）
fn agent_transcript(place: Place, agent: &AgentView) -> Vec<Value> {
    let id = agent.id();
    let inner = Place { agent: Some(id), ..place };
    let mut uses: HashMap<&str, usize> = HashMap::new();
    agent
        .pieces
        .iter()
        .enumerate()
        .map(|(index, piece)| match piece {
            Piece::Text(text) => text_block(place.session, &format!("agent.{id}.{index}"), "text", text),
            Piece::Thinking(text) => text_block(place.session, &format!("agent.{id}.{index}"), "thinking", text),
            Piece::Tool { call, name, arguments, status, result } => {
                let nth = uses.entry(call.as_str()).or_insert(0);
                let block = tool_block(inner, Card { call, nth: *nth, name, arguments, status, result });
                *nth += 1;
                block
            }
        })
        .collect()
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

/// 子智能体交回的汇报（主智能体收到的一段话，存成用户消息）：是谁交的。Friday 里这样的消息标 fromAgent、不显示
fn report_from(text: &str) -> Option<String> {
    if let Some(rest) = text.strip_prefix("子 Agent「") {
        return rest.split_once('」').map(|(name, _)| name.to_string());
    }
    [("[并联派出的", "并联的子智能体"), ("[打回的", "打回的子智能体"), ("[工作流（", "工作流")]
        .iter()
        .find(|(prefix, _)| text.starts_with(prefix))
        .map(|(_, label)| label.to_string())
}

/// 用户在干活时补充的（大脑给模型包了一行「（…在你工作时补充了下面这段…）」）：Friday 里标「工作中补充」
fn is_interjection(text: &str) -> bool {
    text.split_once('\n').is_some_and(|(first, _)| first.starts_with('（') && first.contains("在你工作时补充了下面这段"))
}

#[derive(Default)]
struct Node {
    role: &'static str,
    blocks: Vec<Value>,
    notice: Vec<String>,
    from_agent: Option<String>,
    interjected: bool,
}

/// EDITH 的历史 → 一条条消息（用户的一条；一轮里模型说的话、调的工具合成一条助手消息）
fn group(session: &str, history: &[Value], team: &Team) -> Vec<Node> {
    let mut nodes: Vec<Node> = Vec::new();
    // 调用 id → 它的卡片在哪（第几条消息、第几块）：结果按顺序配给最近一次这个 id 的调用
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
                    // 装着子智能体的卡片：状态按子智能体的（后台派的，存档里只是「已派出」），结果有它交回的用它的
                    let agents = event["agentName"].is_string();
                    if !agents {
                        event["status"] = json!(stored_status(&result));
                    }
                    if !(agents && event["result"].as_str().is_some_and(|text| !text.is_empty())) {
                        event["result"] = json!(result);
                    }
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
                let blocks = vec![text_block(session, &format!("{index}"), "text", &shown_user_text(&text))];
                let (from_agent, interjected) = (report_from(&text), is_interjection(&text));
                nodes.push(Node { role: "user", blocks, from_agent, interjected, ..Default::default() });
            }
            Some("assistant") => {
                if nodes.last().is_none_or(|node| node.role != "assistant") {
                    nodes.push(Node { role: "assistant", ..Default::default() });
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
                    let card = Card { call: id, nth: *nth, name, arguments: &arguments, status: "failed", result: "" };
                    let place = Place { session, node: position, agent: None, team };
                    node.blocks.push(tool_block(place, card));
                    *nth += 1;
                }
            }
            _ => {}
        }
    }
    nodes
}

/// 存档里有几条消息（正在干的那一轮接在后面）
fn stored_count(session: &str) -> usize {
    stored_session(session).map_or(0, |stored| group(&stored.id, &stored.history, &Team::new()).len())
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
    if let Some(agent) = node.from_agent {
        message["fromAgent"] = json!(agent);
    }
    if node.interjected {
        message["interjected"] = json!(true);
    }
    message
}

/// 一段会话的消息（含正在干的那一轮）→ Friday 的 nodes 和 selectedChildren；没在干时最后一条回复挂上最近一轮的用量（进度环）
fn messages(session: &Stored, live: Option<&Live>, team: &Team) -> (Vec<Value>, Map<String, Value>) {
    let created = session.updated_at();
    let mut out: Vec<Value> = group(&session.id, &session.history, team)
        .into_iter()
        .enumerate()
        .map(|(index, node)| message_json(&session.id, index, node, &created))
        .collect();
    match live {
        Some(live) => {
            // 存档里还没有这一轮（做完才存）：接在后面，序号同存档后的
            out.truncate(live.node);
            out.extend(live.messages(&session.id, team));
        }
        None => {
            if let Some(stats) = META.stats(&session.id)
                && let Some(last) = out.last_mut().filter(|message| message["role"] == "assistant")
            {
                last["stats"] = stats;
            }
        }
    }
    let mut selected = Map::new();
    for message in &out {
        let parent = message["parentID"].as_str().unwrap_or(ROOT).to_string();
        selected.insert(parent, message["id"].clone());
    }
    (out, selected)
}

// MARK: - 正在干的一轮

/// 正在干的一轮里的一块
#[derive(Clone, Debug)]
enum Piece {
    Text(String),
    Thinking(String),
    Tool { call: String, name: String, arguments: Value, status: String, result: String },
}

/// 最近一次用这个调用 id 的那一块
fn find_piece(pieces: &[Piece], call: &str) -> Option<usize> {
    pieces.iter().rposition(|piece| matches!(piece, Piece::Tool { call: id, .. } if id == call))
}

/// 这一块之前用过几次同一个调用 id（卡片 id 按它分开）
fn nth_before(pieces: &[Piece], index: usize, call: &str) -> usize {
    pieces[..index].iter().filter(|piece| matches!(piece, Piece::Tool { call: id, .. } if id == call)).count()
}

fn set_piece(pieces: &mut [Piece], call: &str, new_status: &str, new_result: Option<&str>) {
    if let Some(index) = find_piece(pieces, call)
        && let Piece::Tool { status, result, .. } = &mut pieces[index]
    {
        *status = new_status.into();
        if let Some(text) = new_result {
            *result = text.into();
        }
    }
}

fn push_text_piece(pieces: &mut Vec<Piece>, thinking: bool, text: &str) {
    match pieces.last_mut() {
        Some(Piece::Text(existing)) if !thinking => existing.push_str(text),
        Some(Piece::Thinking(existing)) if thinking => existing.push_str(text),
        _ => pieces.push(if thinking { Piece::Thinking(text.into()) } else { Piece::Text(text.into()) }),
    }
}

/// 一轮里的一段：用户的一句（开头那句，或干活时补充进来的）和之后模型做的
#[derive(Clone, Debug, Default)]
struct Segment {
    text: String,
    pieces: Vec<Piece>,
    notice: Vec<String>,
    /// 这句是子智能体交回的汇报（不显示）、用户在干活时补充的
    from_agent: Option<String>,
    interjected: bool,
}

#[derive(Clone, Debug)]
struct Live {
    turn: String,
    /// 这一轮第一条消息（用户的那句）是第几条：存档里的消息数
    node: usize,
    segments: Vec<Segment>,
    started: SystemTime,
    /// 这一轮生成了多少 token、用了几秒（stats 事件累计）
    tokens: f64,
    seconds: f64,
    /// 还没报用量的这段请求估了多少 token、第一个字什么时候到的
    pending: f64,
    first_output: Option<Instant>,
    /// 最近一段请求的上下文（进度环）
    context: Value,
}

impl Live {
    /// 这段会话开始的一轮（turn.started；连上之前就开始的，第一次收到它的事件或 sessions.list 报着时补一个）
    fn start(session: &str, turn: &str, text: &str) -> Live {
        Live {
            turn: turn.to_string(),
            node: stored_count(session),
            segments: vec![Segment { text: text.to_string(), ..Default::default() }],
            started: SystemTime::now(),
            tokens: 0.0,
            seconds: 0.0,
            pending: 0.0,
            first_output: None,
            context: json!({}),
        }
    }

    fn text(&self) -> &str {
        &self.segments[0].text
    }

    fn current(&mut self) -> &mut Segment {
        self.segments.last_mut().unwrap()
    }

    /// 估一段字有多少 token（汉字等一个字算一个，别的四个字符算一个）：服务商每段请求做完才报用量，流式时先按这个显示
    fn estimate(text: &str) -> f64 {
        text.chars().map(|c| if c.is_ascii() { 0.25 } else { 1.0 }).sum()
    }

    fn push_text(&mut self, thinking: bool, text: &str) {
        self.first_output.get_or_insert_with(Instant::now);
        self.pending += Self::estimate(text);
        push_text_piece(&mut self.current().pieces, thinking, text);
    }

    fn push_tool(&mut self, call: &str, name: &str, arguments: Value) {
        let tool = Piece::Tool { call: call.into(), name: name.into(), arguments, status: "running".into(), result: String::new() };
        self.current().pieces.push(tool);
    }

    fn notice(&mut self, text: &str) {
        let segment = self.current();
        if !text.is_empty() && !segment.notice.iter().any(|existing| existing == text) {
            segment.notice.push(text.to_string());
        }
    }

    /// 一段请求做完了：估的换成报的，记下上下文
    fn stats(&mut self, data: &Value) {
        self.tokens += data["generatedTokens"].as_f64().unwrap_or(self.pending);
        self.seconds +=
            data["generateTime"].as_f64().unwrap_or_else(|| self.first_output.map_or(0.0, |first| first.elapsed().as_secs_f64()));
        self.pending = 0.0;
        self.first_output = None;
        self.context = data.clone();
    }

    /// 最近一次用这个调用 id 的卡片：(第几段, 第几块)
    fn find_tool(&self, call: &str) -> Option<(usize, usize)> {
        self.segments.iter().enumerate().rev().find_map(|(segment, part)| find_piece(&part.pieces, call).map(|index| (segment, index)))
    }

    fn set_tool(&mut self, call: &str, new_status: &str, new_result: Option<&str>) {
        if let Some((segment, _)) = self.find_tool(call) {
            set_piece(&mut self.segments[segment].pieces, call, new_status, new_result);
        }
    }

    /// 最近一次用这个调用 id 的卡片的 id（批准按它对上卡片）
    fn card_id(&self, session: &str, call: &str) -> String {
        let Some((segment, index)) = self.find_tool(call) else { return call_uuid(session, self.node + 1, call, 0) };
        call_uuid(session, self.node + 2 * segment + 1, call, nth_before(&self.segments[segment].pieces, index, call))
    }

    /// 这一轮的用量（Friday 的 GenerationStats）
    fn stats_json(&self, live: bool) -> Value {
        // 报过用量的按报的；正在写的这段按估的、从第一个字算起
        let running = self.first_output.map_or(0.0, |first| first.elapsed().as_secs_f64());
        let (tokens, seconds) = (self.tokens + self.pending, self.seconds + running);
        let speed = if seconds > 0.0 { tokens / seconds } else { 0.0 };
        let mut stats = json!({
            "tokensPerSecond": (speed * 10.0).round() / 10.0,
            "tokenCount": tokens.round() as i64,
            "startedAt": iso(self.started),
            "engineName": engine()["title"],
        });
        if live {
            stats["live"] = json!(true);
        }
        if let Some(parts) = self.context["contextParts"].as_object() {
            let total: f64 = parts.values().filter_map(Value::as_f64).sum();
            stats["contextParts"] = json!(parts);
            stats["contextTokenCount"] = json!(total as i64);
        }
        for (from, to) in [("contextLimit", "contextLimit"), ("promptTokens", "promptTokenCount")] {
            if let Some(number) = self.context[from].as_i64() {
                stats[to] = json!(number);
            }
        }
        stats
    }

    /// 这一轮的消息：每段一条用户的、一条助手的
    fn messages(&self, session: &str, team: &Team) -> Vec<Value> {
        let created = iso(self.started);
        let mut out = Vec::new();
        for (segment, part) in self.segments.iter().enumerate() {
            let text = if part.text.is_empty() { "（连上之前开始的这一轮）" } else { &part.text };
            let blocks = vec![text_block(session, &format!("live.{}.{segment}", self.turn), "text", text)];
            let node =
                Node { role: "user", blocks, from_agent: part.from_agent.clone(), interjected: part.interjected, ..Default::default() };
            out.push(message_json(session, self.node + 2 * segment, node, &created));
            out.push(self.assistant(session, segment, &created, team));
        }
        out
    }

    fn assistant(&self, session: &str, segment: usize, created: &str, team: &Team) -> Value {
        let node = self.node + 2 * segment + 1;
        let part = &self.segments[segment];
        let mut uses: HashMap<&str, usize> = HashMap::new();
        let blocks = part
            .pieces
            .iter()
            .enumerate()
            .map(|(index, piece)| match piece {
                Piece::Text(text) => text_block(session, &format!("live.{}.{segment}.{index}", self.turn), "text", text),
                Piece::Thinking(text) => text_block(session, &format!("live.{}.{segment}.{index}", self.turn), "thinking", text),
                Piece::Tool { call, name, arguments, status, result } => {
                    let nth = uses.entry(call.as_str()).or_insert(0);
                    let place = Place { session, node, agent: None, team };
                    let block = tool_block(place, Card { call, nth: *nth, name, arguments, status, result });
                    *nth += 1;
                    block
                }
            })
            .collect();
        let mut message =
            message_json(session, node, Node { role: "assistant", blocks, notice: part.notice.clone(), ..Default::default() }, created);
        // 正在写的那一条：标「还在生成」
        if segment + 1 == self.segments.len() {
            message["stats"] = self.stats_json(true);
        }
        message
    }

    /// 正在写的那条助手消息（流式时推它）
    fn current_message(&self, session: &str, team: &Team) -> Value {
        self.assistant(session, self.segments.len() - 1, &iso(self.started), team)
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

// MARK: - 子智能体（EDITH 的 agent / agent.event 事件、agents.list、存下来的运行记录 <项目数据目录>/agents/<会话>/<id>.json）

/// 派子智能体的工具（卡片里装子智能体的运行记录）
const DISPATCH: &[&str] = &["delegate_task", "run_parallel_tasks", "run_workflow"];

/// 网关知道的一个子智能体：EDITH 报的（id、name、session、parent、call、slot、kind、depth、state、started、finished、steps、activity、reply）
/// 和它的运行记录
#[derive(Clone, Debug, Default)]
struct AgentView {
    meta: Value,
    pieces: Vec<Piece>,
    last_output: Option<SystemTime>,
}

/// EDITH 的子智能体 id → 网关这边的样子
type Team = HashMap<String, AgentView>;

impl AgentView {
    fn id(&self) -> &str {
        self.meta["id"].as_str().unwrap_or("")
    }

    fn session(&self) -> &str {
        self.meta["session"].as_str().unwrap_or("")
    }

    fn state(&self) -> &str {
        self.meta["state"].as_str().unwrap_or("running")
    }

    fn running(&self) -> bool {
        matches!(self.state(), "waiting" | "running")
    }

    /// 存下来的 / agents.list 报的一条（带 transcript）
    fn from_record(mut record: Value) -> AgentView {
        let transcript = record.as_object_mut().and_then(|object| object.remove("transcript")).unwrap_or_default();
        let pieces = transcript
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|piece| {
                let field = |key: &str| piece[key].as_str().unwrap_or("").to_string();
                Some(match piece["type"].as_str()? {
                    "text" => Piece::Text(field("text")),
                    "thinking" => Piece::Thinking(field("text")),
                    "tool" => Piece::Tool {
                        call: field("call"),
                        name: field("name"),
                        arguments: piece["arguments"].clone(),
                        status: field("status"),
                        result: field("result"),
                    },
                    _ => return None,
                })
            })
            .collect();
        AgentView { meta: record, pieces, last_output: None }
    }

    /// agent.event：它干活时的一步
    fn apply(&mut self, kind: &str, data: &Value) {
        let text = data["text"].as_str().unwrap_or("");
        match kind {
            "text" => push_text_piece(&mut self.pieces, false, text),
            "thinking" => push_text_piece(&mut self.pieces, true, text),
            "tool" => self.pieces.push(Piece::Tool {
                call: data["id"].as_str().unwrap_or("").to_string(),
                name: data["name"].as_str().unwrap_or("").to_string(),
                arguments: data["arguments"].clone(),
                status: "running".into(),
                result: String::new(),
            }),
            "result" => {
                let status = if data["ok"].as_bool().unwrap_or(true) { "succeeded" } else { "failed" };
                set_piece(&mut self.pieces, data["id"].as_str().unwrap_or(""), status, Some(text));
            }
            _ => {}
        }
        self.last_output = Some(SystemTime::now());
    }

    /// 运行记录里最近一次用这个调用 id 的卡片的 id
    fn card_id(&self, session: &str, call: &str) -> String {
        let nth = find_piece(&self.pieces, call).map_or(0, |index| nth_before(&self.pieces, index, call));
        agent_call_uuid(session, self.id(), call, nth)
    }
}

/// 这段会话存下来的子智能体（第一次打开它时读；在跑的以 EDITH 报的为准）
fn load_agents(session: &Stored) {
    let Some(project) = session.path.as_ref().and_then(|path| path.parent()).and_then(Path::parent) else { return };
    if !STATE.lock().unwrap().agents_loaded.insert(session.id.clone()) {
        return;
    }
    let records: Vec<AgentView> = std::fs::read_dir(project.join("agents").join(&session.id))
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| serde_json::from_slice::<Value>(&std::fs::read(entry.path()).ok()?).ok())
        .map(AgentView::from_record)
        .collect();
    let mut state = STATE.lock().unwrap();
    for mut record in records {
        if state.agents.contains_key(record.id()) {
            continue;
        }
        // 存的时候还在跑、现在 EDITH 不报它了：那个大脑已经退了
        if record.running() {
            record.meta["state"] = json!("stopped");
        }
        state.agents.insert(record.id().to_string(), record);
    }
}

/// Friday 的 TeamState：在跑的子智能体（Agent 树）。id 是它的卡片 id，parentKey 是上级的卡片 id（主智能体派的是会话 id）
fn team_state(state: &State) -> Value {
    let mut runs: Vec<&AgentView> = state.agents.values().filter(|agent| agent.running()).collect();
    runs.sort_by(|a, b| a.meta["started"].as_str().cmp(&b.meta["started"].as_str()));
    let runs: Vec<Value> = runs
        .iter()
        .map(|agent| {
            let session = agent.session();
            let parent = agent.meta["parent"].as_str();
            let waiting_approval = state.approvals.values().any(|pending| pending.agent.as_deref() == Some(agent.id()));
            let children_running = state.agents.values().any(|child| child.running() && child.meta["parent"].as_str() == Some(agent.id()));
            let activity = if agent.state() == "waiting" {
                json!({ "kind": "queued" })
            } else if waiting_approval {
                json!({ "kind": "waitingApproval" })
            } else if children_running {
                json!({ "kind": "waitingChildren" })
            } else if agent.meta["activity"]["kind"] == "tool" {
                let tool = agent.meta["activity"]["title"].as_str().unwrap_or("");
                json!({ "kind": "tool", "title": tool_look(tool).map_or(tool, |look| look.0) })
            } else {
                json!({ "kind": "generating" })
            };
            let mut run = json!({
                "id": agent_card_uuid(session, agent.id()),
                "name": agent.meta["name"],
                "parentKey": parent.map_or_else(|| upper(session), |parent| agent_card_uuid(session, parent)),
                "conversationID": upper(session),
                "background": agent.meta["depth"].as_u64().unwrap_or(1) <= 1,
                "depth": agent.meta["depth"].as_u64().unwrap_or(1),
                "running": agent.state() == "running",
                "detached": false,
                "stopRequested": false,
                "activity": activity,
                "steps": agent.meta["steps"].as_u64().unwrap_or(0),
                "speed": 0,
                "passTokens": 0,
                "queue": [],
            });
            if let Some(started) = agent.meta["started"].as_str() {
                run["started"] = json!(started);
            }
            if let Some(last) = agent.last_output {
                run["lastOutput"] = json!(iso(last));
            }
            run
        })
        .collect();
    json!({ "runs": runs })
}

/// 大脑推来的子智能体事件：agent（谁、状态）、agent.event（它的一步）
fn on_agent_event(session: &str, kind: &str, data: &Value) {
    if *TRACE {
        log(&format!("  ← {kind} {}", data["id"].as_str().or(data["agent"].as_str()).unwrap_or("")));
    }
    let mut state = STATE.lock().unwrap();
    if kind == "agent" {
        let id = data["id"].as_str().unwrap_or("").to_string();
        let view = state.agents.entry(id).or_default();
        view.meta = data.clone();
        view.last_output.get_or_insert_with(SystemTime::now);
    } else {
        let id = data["agent"].as_str().unwrap_or("").to_string();
        let view = state
            .agents
            .entry(id.clone())
            .or_insert_with(|| AgentView { meta: json!({ "id": id, "session": session, "state": "running" }), ..Default::default() });
        view.apply(data["type"].as_str().unwrap_or(""), &data["data"]);
    }
    state.team_dirty.insert(session.to_string());
    // 主智能体正在干的这一轮里的卡片也跟着变
    if state.live.contains_key(session) {
        state.dirty.insert(session.to_string());
    }
}

/// 子智能体变了的会话：推装着它们的那几条消息；Agent 树、运行状态也推一次（flush_loop 每 0.3 秒）
fn flush_team() {
    let sessions: Vec<String> = std::mem::take(&mut STATE.lock().unwrap().team_dirty).into_iter().collect();
    if sessions.is_empty() {
        return;
    }
    for id in &sessions {
        let subscribed = STATE.lock().unwrap().subscribed.contains(id);
        let Some(session) = subscribed.then(|| find_session(id)).flatten() else { continue };
        let state = STATE.lock().unwrap();
        let live_node = state.live.get(id).map(|live| live.node);
        let (nodes, _) = messages(&session, state.live.get(id), &state.agents);
        for (index, node) in nodes.iter().enumerate() {
            // 正在写的那几条 flush_loop 自己推
            let has_agents = node["blocks"]
                .as_array()
                .is_some_and(|blocks| blocks.iter().any(|block| block["kind"]["tool"]["_0"]["agentName"].is_string()));
            if has_agents && live_node.is_none_or(|live| index < live) {
                event("message", json!({ "conversationID": upper(id), "message": node }));
            }
        }
    }
    let state = STATE.lock().unwrap();
    if state.greeted {
        event("team", team_state(&state));
        event("runner", runner_state(&state));
    }
}

// MARK: - 状态

/// 一件在等客户端批准的事
struct Pending {
    brain: Arc<Brain>,
    request: Value,
    session: String,
    call: String,
    /// 子智能体要做的：哪一个（卡片在它的运行记录里）
    agent: Option<String>,
}

#[derive(Default)]
struct State {
    greeted: bool,
    /// 工作目录 → 连着的大脑
    brains: HashMap<String, Arc<Brain>>,
    subscribed: HashSet<String>,
    live: HashMap<String, Live>,
    generating: HashSet<String>,
    stopping: HashSet<String>,
    /// 事件 id（工具卡片的 UUID）→ 在等的批准
    approvals: HashMap<String, Pending>,
    /// 忙时排着的话（Friday 的 QueuedInput）：开始那一轮时拿掉
    queued: HashMap<String, Vec<Value>>,
    queue_revision: u64,
    /// 正在流式写、该推一次 message 的会话
    dirty: HashSet<String>,
    /// 子智能体
    agents: Team,
    /// 读过存下来的子智能体的会话
    agents_loaded: HashSet<String>,
    /// 子智能体变了、该推一次的会话
    team_dirty: HashSet<String>,
}

static STATE: LazyLock<Mutex<State>> = LazyLock::new(|| Mutex::new(State::default()));

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
    match state.live.get(&session.id).filter(|live| !live.text().trim().is_empty()) {
        Some(live) => live.text().replace('\n', " ").chars().take(40).collect(),
        None => "新对话".into(),
    }
}

fn permission_mode(id: &str) -> String {
    META.setting(id, "permissionMode").unwrap_or_else(|| "auto".into())
}

fn summary(session: &Stored, state: &State) -> Value {
    let live = state.live.contains_key(&session.id);
    let mut value = json!({
        "id": upper(&session.id),
        "title": title(session, state),
        "updatedAt": session.updated_at(),
        "pinned": META.pinned(&session.id),
        "isEmpty": session.history.is_empty() && !live,
        "hasDraft": META.draft(&session.id).is_some(),
        "permissionMode": permission_mode(&session.id),
        "engineID": ENGINE_ID,
    });
    if !session.cwd.is_empty() {
        value["workingDirectory"] = json!(session.cwd);
        if let Some(name) = Path::new(&session.cwd).file_name() {
            value["workingDirectoryName"] = json!(name.to_string_lossy());
        }
    }
    if let Some(thinking) = META.setting(&session.id, "thinking") {
        value["thinking"] = json!(thinking);
    }
    value
}

fn summaries(state: &State) -> Value {
    Value::Array(all_sessions().iter().map(|session| summary(session, state)).collect())
}

fn runner_state(state: &State) -> Value {
    let pending: HashSet<String> = state.approvals.values().map(|pending| upper(&pending.session)).collect();
    // 后台还有子智能体在跑的会话（主智能体派的都在后台跑）
    let background: HashSet<String> = state.agents.values().filter(|agent| agent.running()).map(|agent| upper(agent.session())).collect();
    let agents = state.agents.values().filter(|agent| agent.running()).count();
    json!({
        "generating": state.generating.iter().map(|id| upper(id)).collect::<Vec<_>>(),
        "stopping": state.stopping.iter().map(|id| upper(id)).collect::<Vec<_>>(),
        "pendingApproval": pending.into_iter().collect::<Vec<_>>(),
        "pendingQuestion": [],
        "backgroundAgents": background.into_iter().collect::<Vec<_>>(),
        "liveAgents": state.generating.len() + agents,
        "liveSpeed": 0,
        "queueRevision": state.queue_revision,
    })
}

fn runtime(session: &Stored, state: &State) -> Value {
    let mut value = json!({
        "generating": state.generating.contains(&session.id),
        "stopping": state.stopping.contains(&session.id),
        "queue": state.queued.get(&session.id).cloned().unwrap_or_default(),
        "queuePaused": false,
        "pendingApproval": state.approvals.values().any(|pending| pending.session == session.id),
        "canRetry": false,
        "pendingTurnChanges": { "model": false, "thinking": false },
        "canSend": true,
        "engine": engine(),
        "thinkingLevel": META.setting(&session.id, "thinking").unwrap_or_else(|| "off".into()),
        "thinkingLevels": [],
        "permissionMode": permission_mode(&session.id),
        "mainLiveSpeed": 0,
    });
    if !session.cwd.is_empty() {
        value["workingDirectory"] = json!(session.cwd);
    }
    value
}

fn conversation(session: &Stored, state: &State) -> Value {
    let (nodes, selected) = messages(session, state.live.get(&session.id), &state.agents);
    let mut value = json!({
        "id": upper(&session.id),
        "title": title(session, state),
        "createdAt": iso_text(&session.created),
        "updatedAt": session.updated_at(),
        "nodes": nodes,
        "selectedChildren": selected,
        "permissionMode": permission_mode(&session.id),
        "pinned": META.pinned(&session.id),
        "engineID": ENGINE_ID,
    });
    if !session.cwd.is_empty() {
        value["workingDirectory"] = json!(session.cwd);
    }
    if let Some(draft) = META.draft(&session.id) {
        value["draft"] = json!(draft);
    }
    value
}

/// 会话变了：推会话列表；订着的再推整段（structure）和运行状态
fn changed(session_id: &str, structure: bool) {
    let session = find_session(session_id);
    if let Some(session) = &session {
        load_agents(session);
    }
    let state = STATE.lock().unwrap();
    if !state.greeted {
        return;
    }
    event("summaries", json!({ "summaries": summaries(&state) }));
    event("runner", runner_state(&state));
    if state.subscribed.contains(session_id)
        && let Some(session) = session
    {
        if structure {
            event("conversation", json!({ "conversation": conversation(&session, &state) }));
        }
        event("runtime", json!({ "conversationID": upper(session_id), "runtime": runtime(&session, &state) }));
    }
}

/// 订着的会话里正在写的那条助手消息（只拼这一条、不读存档：流式时每 0.1 秒一次）
fn push_live_message(session_id: &str) {
    let state = STATE.lock().unwrap();
    if state.subscribed.contains(session_id)
        && let Some(live) = state.live.get(session_id)
    {
        event("message", json!({ "conversationID": upper(session_id), "message": live.current_message(session_id, &state.agents) }));
    }
}

/// 正在流式写的消息：每 0.1 秒最多推一次
fn flush_loop() {
    let mut tick = 0u64;
    loop {
        tick += 1;
        std::thread::sleep(Duration::from_millis(100));
        let dirty: Vec<String> = std::mem::take(&mut STATE.lock().unwrap().dirty).into_iter().collect();
        for session in dirty {
            push_live_message(&session);
        }
        if tick.is_multiple_of(3) {
            flush_team();
        }
    }
}

/// 大脑推来的事件
fn on_brain_event(params: &Value) {
    let session = params["session"].as_str().unwrap_or("").to_string();
    let turn = params["turn"].as_str().unwrap_or("").to_string();
    let data = &params["data"];
    // 客人（中转的远程设备）的会话不在 Friday 的列表里
    if stored_session(&session).is_none() {
        return;
    }
    let kind = params["type"].as_str().unwrap_or("");
    if kind == "agent" || kind == "agent.event" {
        on_agent_event(&session, kind, data);
        return;
    }
    // 这一轮要是连上之前就开始的：先补一个（存档里的消息数要在锁外读）
    let known = STATE.lock().unwrap().live.get(&session).is_some_and(|live| live.turn == turn);
    let fresh = (kind != "done" && !known)
        .then(|| Live::start(&session, &turn, if kind == "turn.started" { data["text"].as_str().unwrap_or("") } else { "" }));
    let structure = {
        let mut state = STATE.lock().unwrap();
        if let Some(fresh) = fresh {
            state.live.insert(session.clone(), fresh);
        }
        let from_agent = (data["origin"]["kind"] == "agent")
            .then(|| report_from(data["text"].as_str().unwrap_or("")).unwrap_or_else(|| "子智能体".into()));
        match kind {
            "turn.started" => {
                if let Some(live) = state.live.get_mut(&session)
                    && live.segments.len() == 1
                    && live.segments[0].pieces.is_empty()
                {
                    live.segments[0].from_agent = from_agent;
                }
                state.generating.insert(session.clone());
                // 排着的那句开始了：从队列里拿掉
                let text = data["text"].as_str().unwrap_or("");
                if let Some(queue) = state.queued.get_mut(&session)
                    && let Some(index) = queue.iter().position(|item| item["text"] == text)
                {
                    queue.remove(index);
                    state.queue_revision += 1;
                }
                true
            }
            "done" => {
                if let Some(live) = state.live.remove(&session) {
                    META.set_stats(&session, live.stats_json(false));
                }
                state.generating.remove(&session);
                state.stopping.remove(&session);
                state.dirty.remove(&session);
                state.approvals.retain(|_, pending| pending.session != session);
                true
            }
            _ => {
                state.generating.insert(session.clone());
                let live = state.live.get_mut(&session).unwrap();
                let text = data["text"].as_str().unwrap_or("");
                let mut structure = false;
                match kind {
                    // 干活时补充的话塞进来了：另起一段
                    "input" => {
                        let interjected = from_agent.is_none();
                        live.segments.push(Segment { text: text.to_string(), from_agent, interjected, ..Default::default() });
                        structure = true;
                    }
                    "text" => live.push_text(false, text),
                    "thinking" => live.push_text(true, text),
                    "tool" => {
                        live.push_tool(data["id"].as_str().unwrap_or(""), data["name"].as_str().unwrap_or(""), data["arguments"].clone())
                    }
                    "result" => {
                        let status = if data["ok"].as_bool().unwrap_or(true) { "succeeded" } else { "failed" };
                        live.set_tool(data["id"].as_str().unwrap_or(""), status, Some(text));
                    }
                    "stats" => live.stats(data),
                    "notice" => live.notice(text),
                    _ => {}
                }
                state.dirty.insert(session.clone());
                structure
            }
        }
    };
    if structure {
        changed(&session, true);
    }
}

/// 大脑要批准一件事：变成 Friday 的「待确认」
fn on_brain_approve(brain: &Arc<Brain>, request: &Value) {
    let params = &request["params"];
    let session = params["session"].as_str().unwrap_or("").to_string();
    let call = params["call"].as_str().unwrap_or("").to_string();
    let turn = params["turn"].as_str().unwrap_or("");
    // 子智能体要做的：卡片在它的运行记录里（主智能体这一轮可能早结束了）
    if let Some(agent) = params["agent"].as_str().filter(|agent| !agent.is_empty()) {
        {
            let mut state = STATE.lock().unwrap();
            let view = state.agents.entry(agent.to_string()).or_insert_with(|| AgentView {
                meta: json!({ "id": agent, "session": session, "state": "running" }),
                ..Default::default()
            });
            if find_piece(&view.pieces, &call).is_none() {
                view.apply("tool", &json!({ "id": call, "name": params["tool"], "arguments": params["arguments"] }));
            }
            let event_id = view.card_id(&session, &call);
            set_piece(&mut view.pieces, &call, "awaitingApproval", None);
            let pending =
                Pending { brain: brain.clone(), request: request.clone(), session: session.clone(), call, agent: Some(agent.to_string()) };
            state.approvals.insert(event_id, pending);
            state.team_dirty.insert(session.clone());
            if state.live.contains_key(&session) {
                state.dirty.insert(session.clone());
            }
        }
        event("attention", json!({}));
        changed(&session, false);
        return;
    }
    let fresh = (!STATE.lock().unwrap().live.contains_key(&session)).then(|| Live::start(&session, turn, ""));
    {
        let mut state = STATE.lock().unwrap();
        if let Some(fresh) = fresh {
            state.live.entry(session.clone()).or_insert(fresh);
        }
        state.generating.insert(session.clone());
        let live = state.live.get_mut(&session).unwrap();
        // 卡片是连上之前出的（客户端关了又打开）：按批准请求补一张，界面上才有地方放「允许 / 拒绝」
        if live.find_tool(&call).is_none() {
            live.push_tool(&call, params["tool"].as_str().unwrap_or(""), params["arguments"].clone());
        }
        let event_id = live.card_id(&session, &call);
        live.set_tool(&call, "awaitingApproval", None);
        state
            .approvals
            .insert(event_id, Pending { brain: brain.clone(), request: request.clone(), session: session.clone(), call, agent: None });
        state.dirty.insert(session.clone());
    }
    push_live_message(&session);
    event("attention", json!({}));
    changed(&session, false);
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

    /// 发请求等回话（别拿着 STATE 的锁调：回话排在事件后面，处理事件要那把锁）
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

fn handle_brain_message(brain: &Arc<Brain>, message: Value) {
    let method = message.get("method").and_then(Value::as_str);
    let id = message.get("id").filter(|id| !id.is_null()).cloned();
    match (method, id) {
        (Some("initialize"), Some(id)) => {
            brain.write(&json!({ "jsonrpc": "2.0", "id": id, "result": {
                "protocolVersion": "1",
                "name": "friday",
                "capability": { "title": "Friday 客户端", "version": VERSION, "description": "经 edith-friday 连进来的 Friday（Qt / Swift）" },
                "senses": [{ "kind": "user", "description": "Friday 里打的字" }],
                "actions": [],
                "receives": ["*"],
                "sessions": "all",
                "approves": true,
            }}));
        }
        (Some("event"), None) => on_brain_event(&message["params"]),
        (Some("approve"), Some(_)) => on_brain_approve(brain, &message),
        (Some("ping"), Some(id)) => {
            brain.write(&json!({ "jsonrpc": "2.0", "id": id, "result": {} }));
        }
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

/// 登记里在跑的大脑（`~/.edith/instances/`，接入地址连得上的），新起的在前
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

/// 连上一个大脑：主令牌、握手；之后它推来的事件、批准都由读的线程处理
fn connect_brain(entry: &Value, wait: Duration) -> Result<Arc<Brain>, String> {
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
            disconnected(&brain);
        });
    }
    ready.recv_timeout(wait).map_err(|_| "EDITH 没握手".to_string())?;
    Ok(brain)
}

/// 和一个大脑断了（它退出了）：摘掉；它在干的那几轮当结束了，重发整段
fn disconnected(brain: &Arc<Brain>) {
    let live: Vec<String> = {
        let mut state = STATE.lock().unwrap();
        if state.brains.get(&brain.workspace).is_some_and(|current| Arc::ptr_eq(current, brain)) {
            state.brains.remove(&brain.workspace);
        }
        state.approvals.retain(|_, pending| !Arc::ptr_eq(&pending.brain, brain));
        state.live.keys().cloned().collect()
    };
    let ended: Vec<String> =
        live.into_iter().filter(|id| stored_session(id).is_some_and(|session| workspace_key(&session.cwd) == brain.workspace)).collect();
    {
        let mut state = STATE.lock().unwrap();
        for id in &ended {
            state.live.remove(id);
            state.generating.remove(id);
            state.stopping.remove(id);
            state.queued.remove(id);
        }
    }
    for id in &ended {
        changed(id, true);
    }
}

/// 连上并记下一个大脑，再看它有哪些会话在干活（后连上来的接着显示）
fn attach_brain(entry: &Value, wait: Duration) -> Result<Arc<Brain>, String> {
    let brain = connect_brain(entry, wait)?;
    let brain = {
        let mut state = STATE.lock().unwrap();
        state.brains.entry(brain.workspace.clone()).or_insert(brain).clone()
    };
    let listed = brain.request("sessions.list", json!({}), Duration::from_secs(10)).unwrap_or_default();
    let working: Vec<(String, String, String)> = listed
        .as_array()
        .into_iter()
        .flatten()
        .filter(|session| session["working"] == true && session.get("owner").is_none())
        .map(|session| {
            let field = |value: &Value| value.as_str().unwrap_or("").to_string();
            (field(&session["id"]), field(&session["turn"]["id"]), field(&session["turn"]["text"]))
        })
        .collect();
    for (id, turn, text) in &working {
        let fresh = Live::start(id, turn, text);
        let mut state = STATE.lock().unwrap();
        state.live.entry(id.clone()).or_insert(fresh);
        state.generating.insert(id.clone());
    }
    // 它的子智能体（在跑的、这个进程里跑过的）：Agent 树、卡片里的运行记录接着显示；旧版的 EDITH 没有 agents.list
    let agents = brain.request("agents.list", json!({}), Duration::from_secs(10)).unwrap_or_default();
    let mut touched: HashSet<String> = working.iter().map(|(id, ..)| id.clone()).collect();
    {
        let mut state = STATE.lock().unwrap();
        for record in agents.as_array().into_iter().flatten() {
            let view = AgentView::from_record(record.clone());
            touched.insert(view.session().to_string());
            state.team_dirty.insert(view.session().to_string());
            state.agents.insert(view.id().to_string(), view);
        }
    }
    for id in &touched {
        changed(id, true);
    }
    Ok(brain)
}

/// 连上登记着、还没连的大脑（hello 时、之后每隔几秒：别的客户端起的大脑也要收它的事件、替它批准）
fn attach_all() {
    let connected: HashSet<String> = STATE.lock().unwrap().brains.keys().cloned().collect();
    for entry in registry() {
        let workspace = entry["workspace"].as_str().unwrap_or("").to_string();
        if connected.contains(&workspace) {
            continue;
        }
        if let Err(error) = attach_brain(&entry, Duration::from_secs(3)) {
            log(&format!("连不上 {workspace} 的大脑：{error}"));
        }
    }
}

/// EDITH 程序在哪：EDITH_BIN → 同一个目录里的（装在一起的）→ PATH 里的 edith
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

/// 起一个大脑进程（脱离这次 SSH：断了照常干活）
fn spawn_brain(workspace: &Path, linger: Option<&str>) -> Result<std::process::Child, String> {
    let logs = edith_home().join("logs");
    let _ = std::fs::create_dir_all(&logs);
    let log_file = std::fs::File::create(logs.join(format!("brain-{}.log", std::process::id()))).map_err(|error| error.to_string())?;
    let mut command = std::process::Command::new(edith_program());
    command.args(["run", "--idle"]);
    if let Some(seconds) = linger {
        command.args(["--linger", seconds]);
    }
    command
        .current_dir(workspace)
        .stdin(std::process::Stdio::null())
        .stdout(log_file.try_clone().map_err(|error| error.to_string())?)
        .stderr(log_file);
    let failed = |error: std::io::Error| format!("起不了 EDITH（{}）：{error}", edith_program().display());
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
        if let Ok(child) = command.spawn() {
            return Ok(child);
        }
        command.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP);
    }
    command.spawn().map_err(failed)
}

/// 在这个工作目录起一个大脑，等它登记。没人连着、闲 EDITH_FRIDAY_LINGER 秒（默认半小时，0 是一直开着）就自己退；
/// 旧版的 EDITH 不认 --linger 的退回一直开着
fn launch_brain(workspace: &Path) -> Result<Value, String> {
    let linger = std::env::var("EDITH_FRIDAY_LINGER").ok().filter(|value| !value.is_empty()).unwrap_or_else(|| "1800".into());
    let mut linger = (linger != "0").then_some(linger);
    let mut child = spawn_brain(workspace, linger.as_deref())?;
    let wanted = workspace.to_string_lossy().into_owned();
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if let Some(entry) = registry().into_iter().find(|entry| entry["workspace"].as_str() == Some(wanted.as_str())) {
            return Ok(entry);
        }
        if let Ok(Some(status)) = child.try_wait() {
            if linger.take().is_some() {
                child = spawn_brain(workspace, None)?;
                continue;
            }
            return Err(format!("{wanted} 里的 EDITH 起来就退了（{status}，日志在 {}）", edith_home().join("logs").display()));
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    Err(format!("{wanted} 里的 EDITH 30 秒内没起来（日志在 {}）", edith_home().join("logs").display()))
}

/// 这个目录的大脑：连着的、登记着的，都没有就起一个
fn brain_for(directory: &str) -> Result<Arc<Brain>, String> {
    let key = workspace_key(directory);
    if let Some(brain) = STATE.lock().unwrap().brains.get(&key).cloned() {
        return Ok(brain);
    }
    let entry = match registry().into_iter().find(|entry| entry["workspace"].as_str() == Some(key.as_str())) {
        Some(entry) => entry,
        None => launch_brain(Path::new(&key))?,
    };
    attach_brain(&entry, Duration::from_secs(30))
}

/// 已经连着的、这段会话所在目录的大脑（停下、删的时候用：没连着说明没在干活）
fn connected_brain(session: &Stored) -> Option<Arc<Brain>> {
    STATE.lock().unwrap().brains.get(&workspace_key(&session.cwd)).cloned()
}

// MARK: - 文件：选工作目录、文件卡片预览、附件上传

fn base64_encode(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn base64_decode(text: &str) -> Result<Vec<u8>, (i64, String)> {
    base64::engine::general_purpose::STANDARD.decode(text.trim()).map_err(|error| (-32602, format!("data 不是 base64：{error}")))
}

/// 主目录和各个盘（Linux / macOS 是 /）
fn roots() -> Vec<Value> {
    let mut roots = vec![json!({ "name": "主目录", "path": user_home().to_string_lossy() })];
    if cfg!(windows) {
        for letter in 'A'..='Z' {
            let drive = format!("{letter}:\\");
            if Path::new(&drive).is_dir() {
                roots.push(json!({ "name": format!("{letter}:"), "path": drive }));
            }
        }
    } else {
        roots.push(json!({ "name": "/", "path": "/" }));
    }
    roots
}

/// fs.list：某个目录下的子目录（选工作目录用；files: true 连文件一起，文件卡片用）
fn fs_list(params: &Value) -> Reply {
    let files = params["files"].as_bool() == Some(true);
    let hidden = params["hidden"].as_bool() == Some(true);
    let path = match params["path"].as_str().filter(|path| !path.is_empty()) {
        Some(path) => PathBuf::from(path),
        None if files => PathBuf::from(default_directory()),
        None => user_home(),
    };
    let entries = std::fs::read_dir(&path).map_err(|error| (NOT_FOUND, format!("打不开 {}：{error}", path.display())))?;
    let mut listed: Vec<(bool, String, Value)> = Vec::new();
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if !hidden && name.starts_with('.') {
            continue;
        }
        let full = entry.path();
        let Ok(meta) = std::fs::metadata(&full) else { continue };
        let dir = meta.is_dir();
        if !dir && !files {
            continue;
        }
        let mut item = json!({ "name": name, "path": full.to_string_lossy() });
        if files {
            item["dir"] = json!(dir);
            if !dir {
                item["size"] = json!(meta.len());
            }
            if let Ok(modified) = meta.modified() {
                item["modified"] = json!(iso(modified));
            }
        }
        listed.push((dir, name.to_lowercase(), item));
    }
    // 目录在前，按名字
    listed.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
    let truncated = listed.len() > 5000;
    listed.truncate(5000);
    let mut result = json!({
        "path": path.to_string_lossy(),
        "entries": listed.into_iter().map(|(_, _, item)| item).collect::<Vec<_>>(),
        "roots": roots(),
        "truncated": truncated,
    });
    if let Some(parent) = path.parent() {
        result["parent"] = json!(parent.to_string_lossy());
    }
    Ok(result)
}

/// fs.mkdir：选工作目录的对话框里「新建文件夹」
fn fs_mkdir(params: &Value) -> Reply {
    let path = text_param(params, "path")?;
    let name = params["name"].as_str().unwrap_or("").trim().to_string();
    if name.is_empty() || name == "." || name == ".." || name.contains(['/', '\\']) {
        return Err((-32602, format!("文件夹名不对：{name}")));
    }
    let target = Path::new(&path).join(&name);
    if target.exists() {
        return Err((NOT_ALLOWED, format!("已经有 {name} 了")));
    }
    std::fs::create_dir(&target).map_err(|error| (NOT_ALLOWED, format!("建不了 {}：{error}", target.display())))?;
    Ok(json!({ "path": target.to_string_lossy() }))
}

/// fs.read：读这台机器上一个文件的一段（文件卡片预览，每次最多 1 MB）
fn fs_read(params: &Value) -> Reply {
    let path = text_param(params, "path")?;
    let offset = params["offset"].as_u64().unwrap_or(0);
    let length = (params["length"].as_u64().unwrap_or(CHUNK as u64) as usize).min(CHUNK);
    let mut file = std::fs::File::open(&path).map_err(|error| (NOT_FOUND, format!("打不开 {path}：{error}")))?;
    let size = file.metadata().map(|meta| meta.len()).unwrap_or(0);
    file.seek(std::io::SeekFrom::Start(offset)).map_err(|error| (NOT_ALLOWED, error.to_string()))?;
    let mut data = Vec::with_capacity(length);
    file.take(length as u64).read_to_end(&mut data).map_err(|error| (NOT_ALLOWED, error.to_string()))?;
    Ok(json!({ "data": base64_encode(&data), "size": size, "eof": offset + data.len() as u64 >= size }))
}

/// file.upload：客户端把附件一块块传上来，放在 ~/.edith/uploads/<uploadID>/<名字>，最后一块回这台上的路径
fn file_upload(params: &Value) -> Reply {
    let upload = text_param(params, "uploadID")?;
    if !upload.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
        return Err((-32602, "uploadID 只能是字母、数字、- 和 _".into()));
    }
    let name = Path::new(params["name"].as_str().unwrap_or("")).file_name().map(|name| name.to_string_lossy().into_owned());
    let name = name.filter(|name| !name.is_empty() && name != "." && name != "..").unwrap_or_else(|| "附件".into());
    let dir = edith_home().join("uploads").join(&upload);
    std::fs::create_dir_all(&dir).map_err(|error| (NOT_ALLOWED, error.to_string()))?;
    let path = dir.join(&name);
    let data = base64_decode(params["data"].as_str().unwrap_or(""))?;
    if data.len() > CHUNK {
        return Err((-32602, "一块最多 1 MB".into()));
    }
    let offset = params["offset"].as_u64().unwrap_or(0);
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(offset == 0)
        .open(&path)
        .map_err(|error| (NOT_ALLOWED, format!("写不了 {}：{error}", path.display())))?;
    file.seek(std::io::SeekFrom::Start(offset)).and_then(|_| file.write_all(&data)).map_err(|error| (NOT_ALLOWED, error.to_string()))?;
    if params["last"].as_bool() == Some(true) {
        return Ok(json!({ "path": path.to_string_lossy() }));
    }
    Ok(json!({}))
}

// MARK: - 客户端的请求

type Reply = Result<Value, (i64, String)>;

fn not_allowed(what: &str) -> (i64, String) {
    (NOT_ALLOWED, format!("这一版的 EDITH 还不支持{what}"))
}

fn text_param(params: &Value, key: &str) -> Result<String, (i64, String)> {
    params[key].as_str().map(str::to_string).filter(|value| !value.is_empty()).ok_or((-32602, format!("缺少参数 {key}")))
}

fn session_param(params: &Value, key: &str) -> Result<Stored, (i64, String)> {
    let id = text_param(params, key)?;
    find_session(&id).ok_or((NOT_FOUND, "没有这段会话".into()))
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
        // 选目录时新建文件夹；runner.submit 认 whenBusy
        "features": ["mkdir", "submitFrom"],
    })
}

fn directories_state() -> Value {
    json!({ "recent": META.recent().iter().map(|path| json!({ "path": path, "exists": Path::new(path).is_dir() })).collect::<Vec<_>>() })
}

fn hello(params: &Value) -> Reply {
    let protocol = params["protocol"].as_i64().unwrap_or(0);
    if protocol != PROTOCOL {
        return Err((
            1,
            format!("协议版本不一致：本机 Friday 用第 {protocol} 版，这台的 EDITH 网关用第 {PROTOCOL} 版。请把两边换成同一版本。"),
        ));
    }
    log(&format!("hello：{}", params["client"]));
    // 先连上在跑的大脑：在干活的会话、在等的批准马上就有
    attach_all();
    let mut state = STATE.lock().unwrap();
    state.greeted = true;
    Ok(json!({
        "protocol": PROTOCOL,
        "server": server_info(),
        "trusted": true,
        "state": {
            "summaries": summaries(&state),
            "runner": runner_state(&state),
            "team": team_state(&state),
            "engines": engines_state(),
            "secrets": { "names": [] },
            "directories": directories_state(),
            "terminals": { "terminals": [], "available": false, "reason": "这一版的 EDITH 还没有内置终端" },
            "browser": { "tabs": [], "available": false, "reason": "这一版的 EDITH 还没有内置浏览器" },
        },
    }))
}

/// 空白会话第一次发话：在 EDITH 里建出来（用客户端给的 id）
fn materialize(session: &Stored) -> Result<(Arc<Brain>, Stored), (i64, String)> {
    let directory = if session.cwd.is_empty() { default_directory() } else { session.cwd.clone() };
    let brain = brain_for(&directory).map_err(|error| (NOT_ALLOWED, error))?;
    if !session.blank() {
        return Ok((brain, session.clone()));
    }
    brain
        .request("sessions.create", json!({ "id": session.id, "cwd": directory }), Duration::from_secs(30))
        .map_err(|error| (NOT_ALLOWED, error))?;
    if !session.title.is_empty()
        && let Some(path) = stored_session(&session.id).and_then(|stored| stored.path)
    {
        append_title(&path, &session.title);
    }
    META.remove_blank(&session.id);
    let stored = stored_session(&session.id).ok_or((NOT_ALLOWED, "EDITH 建了会话却找不到存档".to_string()))?;
    Ok((brain, stored))
}

fn submit(params: &Value) -> Reply {
    let session = session_param(params, "conversationID")?;
    let mut text = params["text"].as_str().unwrap_or("").trim().to_string();
    let attachments: Vec<&str> = params["attachments"].as_array().into_iter().flatten().filter_map(Value::as_str).collect();
    if !attachments.is_empty() {
        text.push_str(&format!("\n\n（附件，在这台机器上：{}）", attachments.join("、")));
    }
    if text.trim().is_empty() {
        return Err((-32602, "text 是空的".into()));
    }
    let (brain, session) = materialize(&session)?;
    // 忙的时候：默认同输入框（补充进这一轮），whenBusy: queue 排到下一轮
    let queue = params["whenBusy"].as_str() == Some("queue");
    let urgency = if queue { "queue" } else { "interject" };
    let reply = brain
        .request("sense", json!({ "session": session.id, "text": text, "urgency": urgency, "kind": "user" }), Duration::from_secs(30))
        .map_err(|error| (NOT_ALLOWED, error))?;
    META.set_draft(&session.id, "");
    if reply["disposition"] == "queued" {
        let item = json!({
            "id": uuid::Uuid::new_v4().to_string().to_uppercase(),
            "text": text,
            "attachments": attachments,
            "createdAt": iso(SystemTime::now()),
        });
        let mut state = STATE.lock().unwrap();
        state.queued.entry(session.id.clone()).or_default().push(item);
        state.queue_revision += 1;
    }
    changed(&session.id, true);
    Ok(json!({}))
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
            let session = session_param(params, "id")?;
            load_agents(&session);
            let mut state = STATE.lock().unwrap();
            state.subscribed.insert(session.id.clone());
            Ok(json!({ "conversation": conversation(&session, &state), "runtime": runtime(&session, &state) }))
        }
        "conversation.unsubscribe" => {
            STATE.lock().unwrap().subscribed.remove(&lower(&text_param(params, "id")?));
            Ok(json!({}))
        }
        "message.get" => {
            let session = session_param(params, "conversationID")?;
            let message_id = text_param(params, "messageID")?;
            load_agents(&session);
            let state = STATE.lock().unwrap();
            let (nodes, _) = messages(&session, state.live.get(&session.id), &state.agents);
            let message = nodes
                .into_iter()
                .find(|node| node["id"].as_str() == Some(message_id.as_str()))
                .ok_or((NOT_FOUND, "没有这条消息".into()))?;
            Ok(json!({ "message": message, "version": 0 }))
        }
        "conversation.create" => {
            // 只记下（空白会话）：第一次发话时才在 EDITH 里建、才起大脑
            let id = lower(&text_param(params, "id")?);
            if find_session(&id).is_some() {
                return Ok(json!({ "id": upper(&id) }));
            }
            let directory =
                params["workingDirectory"].as_str().filter(|dir| !dir.is_empty()).map(str::to_string).unwrap_or_else(default_directory);
            if !Path::new(&directory).is_dir() {
                return Err((-32602, format!("工作目录不存在：{directory}")));
            }
            META.set_blank(&id, &directory, params["title"].as_str().unwrap_or(""));
            META.push_recent(&directory);
            for key in ["permissionMode", "thinking"] {
                if let Some(value) = params[key].as_str() {
                    META.set_setting(&id, key, value);
                }
            }
            changed(&id, true);
            event("directories", directories_state());
            Ok(json!({ "id": upper(&id) }))
        }
        "conversation.remove" => {
            let session = session_param(params, "id")?;
            if let Some(path) = &session.path {
                // 交给大脑删（停下、存档、改动基线、子智能体的运行记录一起）；旧版的 EDITH 没有 sessions.remove：停下后这里删
                let removed = brain_for(&session.cwd)
                    .ok()
                    .is_some_and(|brain| brain.request("sessions.remove", json!({ "id": session.id }), Duration::from_secs(30)).is_ok());
                if !removed {
                    if let Some(brain) = connected_brain(&session) {
                        let _ = brain.request("sessions.stop", json!({ "id": session.id }), Duration::from_secs(10));
                    }
                    let _ = std::fs::remove_file(path);
                    if let Some(project) = path.parent().and_then(Path::parent) {
                        let _ = std::fs::remove_file(project.join("reads").join(format!("{}.json", session.id)));
                        let _ = std::fs::remove_dir_all(project.join("agents").join(&session.id));
                        let _ = std::fs::remove_dir_all(project.join("changes").join(&session.id));
                    }
                }
            }
            META.forget(&session.id);
            {
                let mut state = STATE.lock().unwrap();
                state.subscribed.remove(&session.id);
                state.queued.remove(&session.id);
                state.agents.retain(|_, agent| agent.session() != session.id);
            }
            changed(&session.id, false);
            Ok(json!({}))
        }
        "conversation.rename" => {
            let session = session_param(params, "id")?;
            let title = params["title"].as_str().unwrap_or("").to_string();
            match &session.path {
                Some(path) => append_title(path, &title),
                None => META.set_blank(&session.id, &session.cwd, &title),
            }
            changed(&session.id, true);
            Ok(json!({}))
        }
        "conversation.setPinned" => {
            let session = session_param(params, "id")?;
            META.set_pinned(&session.id, params["pinned"].as_bool().unwrap_or(false));
            changed(&session.id, true);
            Ok(json!({}))
        }
        "conversation.setDraft" => {
            let session = session_param(params, "id")?;
            META.set_draft(&session.id, params["draft"].as_str().unwrap_or(""));
            changed(&session.id, false);
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
        "runner.submit" => submit(params),
        "runner.stop" => {
            let session = session_param(params, "conversationID")?;
            if let Some(brain) = connected_brain(&session) {
                {
                    let mut state = STATE.lock().unwrap();
                    state.stopping.insert(session.id.clone());
                    // 排着的也不干了（同 EDITH 的 sessions.stop）
                    if state.queued.remove(&session.id).is_some() {
                        state.queue_revision += 1;
                    }
                }
                changed(&session.id, false);
                brain
                    .request("sessions.stop", json!({ "id": session.id }), Duration::from_secs(10))
                    .map_err(|error| (NOT_ALLOWED, error))?;
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
                let status = if approved { "running" } else { "denied" };
                match &pending.agent {
                    Some(agent) => {
                        if let Some(view) = state.agents.get_mut(agent) {
                            set_piece(&mut view.pieces, &pending.call, status, None);
                        }
                        state.team_dirty.insert(pending.session.clone());
                    }
                    None => {
                        if let Some(live) = state.live.get_mut(&pending.session) {
                            live.set_tool(&pending.call, status, None);
                        }
                    }
                }
                state.dirty.insert(pending.session.clone());
            }
            changed(&pending.session, false);
            Ok(json!({}))
        }
        // Agent 树里停掉、追加指示：agentID 是它的卡片 id
        "agent.stop" | "agent.send" => {
            let card = upper(&text_param(params, "agentID")?);
            let found = STATE
                .lock()
                .unwrap()
                .agents
                .values()
                .find(|agent| agent_card_uuid(agent.session(), agent.id()) == card)
                .map(|agent| (agent.id().to_string(), agent.session().to_string()));
            let (agent, session) = found.ok_or((NOT_FOUND, "没有这个子智能体".to_string()))?;
            let stored = stored_session(&session).ok_or((NOT_FOUND, "没有这段会话".to_string()))?;
            let brain = brain_for(&stored.cwd).map_err(|error| (NOT_ALLOWED, error))?;
            if method == "agent.stop" {
                brain.request("agents.stop", json!({ "id": agent }), Duration::from_secs(10)).map_err(|error| (NOT_ALLOWED, error))?;
                return Ok(json!({}));
            }
            let mut text = params["text"].as_str().unwrap_or("").trim().to_string();
            let attachments: Vec<&str> = params["attachments"].as_array().into_iter().flatten().filter_map(Value::as_str).collect();
            if !attachments.is_empty() {
                text.push_str(&format!("\n\n（附件，在这台机器上：{}）", attachments.join("、")));
            }
            brain
                .request("agents.send", json!({ "id": agent, "text": text }), Duration::from_secs(10))
                .map_err(|error| (NOT_ALLOWED, error))?;
            Ok(json!({}))
        }
        // 会话改动（文件卡片的「本次会话改动」、改动视图、撤销）：交给这段会话所在目录的大脑（changes.*）
        "fs.changes" | "fs.diff" | "fs.revert" => {
            let session = session_param(params, "conversationID")?;
            if session.blank() {
                return match method {
                    "fs.changes" => Ok(json!({ "files": [] })),
                    "fs.revert" => Ok(json!({ "failed": [] })),
                    _ => Err((NOT_FOUND, "这段会话没动过它".into())),
                };
            }
            let brain = brain_for(&session.cwd).map_err(|error| (NOT_ALLOWED, error))?;
            let mut request = json!({ "session": session.id });
            if let Some(path) = params["path"].as_str().filter(|path| !path.is_empty()) {
                request["path"] = json!(path);
            }
            let target = match method {
                "fs.changes" => "changes.list",
                "fs.diff" => "changes.diff",
                _ => "changes.revert",
            };
            brain
                .request(target, request, Duration::from_secs(30))
                .map_err(|error| (if method == "fs.diff" { NOT_FOUND } else { NOT_ALLOWED }, error))
        }
        "runner.setWorkingDirectory" => {
            // 只有还没发过话的能换（EDITH 的会话记在它工作目录的名下）
            let session = session_param(params, "conversationID")?;
            if !session.history.is_empty() {
                return Err((NOT_ALLOWED, "有内容的会话不能换工作目录".into()));
            }
            let directory = params["path"].as_str().filter(|path| !path.is_empty()).map(str::to_string).unwrap_or_else(default_directory);
            if !Path::new(&directory).is_dir() {
                return Err((-32602, format!("工作目录不存在：{directory}")));
            }
            if let Some(path) = &session.path {
                let _ = std::fs::remove_file(path);
            }
            META.set_blank(&session.id, &directory, &session.title);
            META.push_recent(&directory);
            changed(&session.id, true);
            event("directories", directories_state());
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
                META.set_setting(&id, key, value);
            }
            changed(&id, true);
            Ok(json!({}))
        }
        "directories.get" => Ok(directories_state()),
        "directories.push" => {
            META.push_recent(&text_param(params, "path")?);
            event("directories", directories_state());
            Ok(json!({}))
        }
        "directories.remove" => {
            META.remove_recent(&text_param(params, "path")?);
            event("directories", directories_state());
            Ok(json!({}))
        }
        "fs.list" => fs_list(params),
        "fs.mkdir" => fs_mkdir(params),
        "fs.read" => fs_read(params),
        "file.upload" => file_upload(params),
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
        "runner.removeQueued" | "runner.resumeQueue" => Err(not_allowed("把排着的话拿掉")),
        "runner.cancelQuotaWait" => Err(not_allowed("这个操作")),
        "agent.removeQueued" => Err(not_allowed("把子智能体排着的话拿掉")),
        "secrets.set" | "secrets.remove" | "engines.update" | "engines.remove" | "engines.setSubAgentAllowed" => {
            Err(not_allowed("在这里改密钥和模型（改 ~/.edith/config.json）"))
        }
        other => Err((-32601, format!("这一版的 EDITH 网关没有 {other}"))),
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
    // 别的客户端（或 edith run）新起的大脑：几秒看一次，连上
    std::thread::spawn(|| {
        loop {
            std::thread::sleep(Duration::from_secs(5));
            if STATE.lock().unwrap().greeted {
                attach_all();
            }
        }
    });
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
    // 输入完了：还没回的回完再走。客户端走了网关就退出，大脑照常干活（在等的批准它会等下一个连上来的）
    drop(sender);
    let _ = worker.join();
}
