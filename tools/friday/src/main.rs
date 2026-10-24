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

/// 会话没给工作目录时用哪：EDITH_FRIDAY_DIRECTORY → 设置 › 智能体里选的（friday.json）→ 家目录
fn default_directory() -> String {
    std::env::var("EDITH_FRIDAY_DIRECTORY")
        .ok()
        .filter(|dir| !dir.is_empty())
        .or_else(|| META.value()["workingDirectory"].as_str().filter(|dir| Path::new(dir).is_dir()).map(str::to_string))
        .unwrap_or_else(|| user_home().to_string_lossy().into_owned())
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

/// EDITH 里藏起来的另一支（改一条重发、切分支时存的，记在 #branch 名下）
const ARCHIVED: &str = "#branch";

fn archived_session(id: &str) -> Option<Stored> {
    let id = lower(id);
    for project in std::fs::read_dir(edith_home().join("projects")).into_iter().flatten().flatten() {
        let path = project.path().join("sessions").join(format!("{id}.jsonl"));
        if path.is_file() {
            return read_session(&path).filter(|session| session.owner == ARCHIVED);
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

    /// 主会话一张卡片的增删行数（不刷新缓存：卡片多时每张都看一眼文件太慢）
    fn line(&self, session: &str, card: &str) -> Option<[i64; 2]> {
        let cache = self.cache.lock().unwrap();
        let entry = &cache.0["lines"][lower(session)][card];
        Some([entry[0].as_i64()?, entry[1].as_i64()?])
    }

    fn set_line(&self, session: &str, card: &str, lines: [i64; 2]) {
        self.update(|value| {
            let sessions = Self::object(value, "lines");
            let entry = sessions.entry(lower(session)).or_insert_with(|| json!({}));
            if !entry.is_object() {
                *entry = json!({});
            }
            entry[card] = json!(lines);
        });
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

    /// 最近一轮怎么结束的：{at（那时存档有几条）, error?, stopped?}（出错、被停下存档里不记）
    fn ended(&self, id: &str) -> Option<Value> {
        let ended = self.value()["ended"][lower(id)].clone();
        ended.is_object().then_some(ended)
    }

    fn set_ended(&self, id: &str, at: usize, error: &str, stopped: bool) {
        let current = self.ended(id);
        if error.is_empty() && !stopped && current.is_none() {
            return;
        }
        self.update(|value| {
            let entries = Self::object(value, "ended");
            if error.is_empty() && !stopped {
                entries.remove(&lower(id));
            } else {
                entries.insert(lower(id), json!({ "at": at, "error": (!error.is_empty()).then_some(error), "stopped": stopped }));
            }
        });
    }

    /// 分出去的各支：(主干的先后, [(藏起来的会话, 分出来的先后)])
    fn branches(&self, id: &str) -> (u64, Vec<(String, u64)>) {
        let entry = &self.value()["branches"][lower(id)];
        let paths = entry["paths"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|path| Some((path["id"].as_str()?.to_string(), path["rank"].as_u64()?)))
            .collect();
        (entry["main"].as_u64().unwrap_or(0), paths)
    }

    fn set_branches(&self, id: &str, main: u64, paths: &[(String, u64)]) {
        self.update(|value| {
            let paths: Vec<Value> = paths.iter().map(|(path, rank)| json!({ "id": path, "rank": rank })).collect();
            Self::object(value, "branches").insert(lower(id), json!({ "main": main, "paths": paths }));
        });
    }

    /// 跟着消息走的（卡片的增删行数、最近一轮的用量、怎么结束的）：主干整个存成另一支时抄一份过去，
    /// 切分支时主干的给存起来的那支、换上来的那支的给主干
    fn move_with_branch(&self, main: &str, archive: &str, picked: Option<&str>) {
        self.update(|value| {
            for key in ["lines", "stats", "ended"] {
                let entries = Self::object(value, key);
                let current = entries.get(&lower(main)).cloned();
                match picked {
                    Some(picked) => {
                        entries.remove(&lower(main));
                        if let Some(entry) = entries.remove(&lower(picked)) {
                            entries.insert(lower(main), entry);
                        }
                    }
                    // 改一条重发：主干截短了，用量、结束的样子不再是它的（行数还对得上共同的开头）
                    None if key != "lines" => {
                        entries.remove(&lower(main));
                    }
                    None => {}
                }
                if let Some(current) = current {
                    entries.insert(lower(archive), current);
                }
            }
        });
    }

    /// 删会话：它的这些都不要了
    fn forget(&self, id: &str) {
        self.update(|value| {
            let id = lower(id);
            if let Some(ids) = value["pinned"].as_array_mut() {
                ids.retain(|item| item.as_str() != Some(id.as_str()));
            }
            for key in ["drafts", "settings", "blank", "stats", "lines", "ended", "branches"] {
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
        "save_memory" => ("记住", "brain"),
        "search_memory" => ("查记忆", "brain"),
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
    /// 这一步加了几行、删了几行（卡片上的「+N −M」）
    lines: Option<[i64; 2]>,
}

/// 卡片在哪：主会话的第几条消息（agent 是 None），或哪个子智能体的运行记录里；team：这台上知道的子智能体。
/// session 算 id 用（分出去的另一支用那一支的 id，卡片不和这段的撞），owner 是子智能体记在哪段会话名下
#[derive(Clone, Copy)]
struct Place<'a> {
    session: &'a str,
    owner: &'a str,
    node: usize,
    agent: Option<&'a str>,
    team: &'a Team,
}

/// 子智能体运行记录里的卡片的 id
fn agent_call_uuid(session: &str, agent: &str, call: &str, nth: usize) -> String {
    stable_uuid(&[session, "agent-call", agent, call, &nth.to_string()])
}

/// 子智能体还没看的一条追加指示的 id（Friday 的 QueuedInput）
fn agent_inbox_uuid(session: &str, agent: &str, item: &str) -> String {
    stable_uuid(&[session, "agent-inbox", agent, item])
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
    // 主会话的卡片：存档里不记行数，正在干时记在 friday.json 里
    let lines =
        card.lines.or_else(|| place.agent.is_none().then(|| META.line(place.session, event["id"].as_str().unwrap_or(""))).flatten());
    if let Some([added, removed]) = lines {
        event["linesAdded"] = json!(added);
        event["linesRemoved"] = json!(removed);
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
        .filter(|agent| agent.session() == place.owner && agent.meta["call"] == json!(call) && agent.meta["parent"].as_str() == place.agent)
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
            Piece::Tool { call, name, arguments, status, result, lines } => {
                let nth = uses.entry(call.as_str()).or_insert(0);
                let block = tool_block(inner, Card { call, nth: *nth, name, arguments, status, result, lines: *lines });
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
    /// 这条消息到历史的第几条为止（不含）：截短、分叉按它
    end: usize,
    /// 这一轮出错了 / 被用户停下（存档里不记，网关记在 friday.json）
    error: Option<String>,
    stopped: bool,
}

/// EDITH 的历史 → 一条条消息（用户的一条；一轮里模型说的话、调的工具合成一条助手消息）
fn group(session: &str, history: &[Value], team: &Team) -> Vec<Node> {
    group_in(session, session, history, team)
}

/// 同 group；owner：子智能体记在哪段会话名下（分出去的另一支的卡片照样装上它派的子智能体）
fn group_in(session: &str, owner: &str, history: &[Value], team: &Team) -> Vec<Node> {
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
                    let card = Card { call: id, nth: *nth, name, arguments: &arguments, status: "failed", result: "", lines: None };
                    let place = Place { session, owner, node: position, agent: None, team };
                    node.blocks.push(tool_block(place, card));
                    *nth += 1;
                }
            }
            _ => {}
        }
        if let Some(last) = nodes.last_mut() {
            last.end = index + 1;
        }
    }
    nodes
}

/// 每条消息连同它前面全部历史的指纹：分出去的几支里同样的几条（共同的开头）指纹一样，按它认出是同一条
fn chain(history: &[Value], nodes: &[Node]) -> Vec<String> {
    let mut previous = String::new();
    let mut start = 0;
    nodes
        .iter()
        .map(|node| {
            let mut digest = sha2::Sha256::new();
            digest.update(previous.as_bytes());
            for message in &history[start..node.end.max(start)] {
                digest.update(message.to_string().as_bytes());
                digest.update(b"\n");
            }
            start = node.end.max(start);
            previous = digest.finalize().iter().map(|byte| format!("{byte:02x}")).collect();
            previous.clone()
        })
        .collect()
}

fn message_uuid(session: &str, node: usize) -> String {
    stable_uuid(&[session, "message", &node.to_string()])
}

fn message_json(session: &str, index: usize, node: Node, created: &str) -> Value {
    let parent = (index > 0).then(|| message_uuid(session, index - 1));
    message_with(&message_uuid(session, index), parent.as_deref(), node, created)
}

fn message_with(id: &str, parent: Option<&str>, node: Node, created: &str) -> Value {
    let mut message = json!({ "id": id, "role": node.role, "blocks": node.blocks, "createdAt": created });
    if let Some(parent) = parent {
        message["parentID"] = json!(parent);
    }
    if let Some(error) = node.error {
        message["error"] = json!(error);
    }
    if node.stopped {
        message["stoppedByUser"] = json!(true);
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
    let mut nodes = group(&session.id, &session.history, team);
    let branches = META.branches(&session.id);
    let hashes = if branches.1.is_empty() { Vec::new() } else { chain(&session.history, &nodes) };
    if live.is_none() {
        mark_ended(session, &mut nodes);
    }
    let mut out: Vec<Value> = nodes.into_iter().enumerate().map(|(index, node)| message_json(&session.id, index, node, &created)).collect();
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
    if !branches.1.is_empty() {
        add_branches(session, &branches, &hashes, team, &mut out, &mut selected);
    }
    (out, selected)
}

/// 最近一轮出错了 / 被停下（存档里不记）：标在最后一条回复上；出错时一个字还没回的补一条空的回复。
/// 只认那之后存档没变过的（又说了话、重试、换了分支就不是它了）
fn mark_ended(session: &Stored, nodes: &mut Vec<Node>) {
    let Some(ended) = META.ended(&session.id).filter(|ended| ended["at"].as_u64() == Some(session.history.len() as u64)) else {
        return;
    };
    let error = ended["error"].as_str().filter(|error| !error.is_empty()).map(str::to_string);
    if error.is_some() && nodes.last().is_none_or(|node| node.role != "assistant") {
        nodes.push(Node { role: "assistant", end: session.history.len(), ..Default::default() });
    }
    if let Some(last) = nodes.last_mut().filter(|node| node.role == "assistant") {
        last.stopped = ended["stopped"] == true && error.is_none();
        last.error = error;
    }
}

/// 停在一条出错的回复上：它的 id（Friday 的 failedMessageID，有它才能重试）
fn failed_message(session: &Stored) -> Option<String> {
    META.ended(&session.id).filter(|ended| ended["at"].as_u64() == Some(session.history.len() as u64) && ended["error"].is_string())?;
    let mut nodes = group(&session.id, &session.history, &Team::new());
    mark_ended(session, &mut nodes);
    nodes.last().filter(|node| node.error.is_some()).map(|_| message_uuid(&session.id, nodes.len() - 1))
}

/// 分出去的一支（EDITH 里藏起来的会话）：它的消息，和这段共同的那几条用这段的 id
struct BranchPath {
    archive: Stored,
    rank: u64,
    nodes: Vec<Node>,
    ids: Vec<String>,
}

fn branch_uuid(session: &str, hash: &str) -> String {
    stable_uuid(&[session, "branch", hash])
}

/// 这段会话分出去的各支（新分出来的在前）。hashes：这段（主干）每条消息的指纹
fn branch_paths(session: &Stored, paths: &[(String, u64)], hashes: &[String], team: &Team) -> Vec<BranchPath> {
    let known: HashMap<&str, String> =
        hashes.iter().enumerate().map(|(index, hash)| (hash.as_str(), message_uuid(&session.id, index))).collect();
    let mut ordered = paths.to_vec();
    ordered.sort_by_key(|(_, rank)| std::cmp::Reverse(*rank));
    ordered
        .into_iter()
        .filter_map(|(id, rank)| {
            let archive = archived_session(&id)?;
            let nodes = group_in(&archive.id, &session.id, &archive.history, team);
            let ids = chain(&archive.history, &nodes)
                .iter()
                .map(|hash| known.get(hash.as_str()).cloned().unwrap_or_else(|| branch_uuid(&session.id, hash)))
                .collect();
            Some(BranchPath { archive, rank, nodes, ids })
        })
        .collect()
}

/// 分出去的各支接进消息树（同 Friday 的分叉树）：共同的开头不重复，从分开的那条起接在共同的最后一条下面；
/// 同一处的几支按分出来的先后排（切分支的箭头「1/2」「2/2」不乱），没选过的地方选新分出来的
fn add_branches(
    session: &Stored,
    (main_rank, paths): &(u64, Vec<(String, u64)>),
    hashes: &[String],
    team: &Team,
    out: &mut Vec<Value>,
    selected: &mut Map<String, Value>,
) {
    let main_rank = *main_rank;
    let mut rank_of: HashMap<String, u64> =
        out.iter().filter_map(|message| message["id"].as_str()).map(|id| (id.to_string(), main_rank)).collect();
    for path in branch_paths(session, paths, hashes, team) {
        let created = path.archive.updated_at();
        let mut parent: Option<String> = None;
        for (node, id) in path.nodes.into_iter().zip(path.ids) {
            match rank_of.get(&id) {
                Some(rank) => {
                    let rank = (*rank).min(path.rank);
                    rank_of.insert(id.clone(), rank);
                }
                None => {
                    rank_of.insert(id.clone(), path.rank);
                    selected.entry(parent.clone().unwrap_or_else(|| ROOT.into())).or_insert(json!(id));
                    out.push(message_with(&id, parent.as_deref(), node, &created));
                }
            }
            parent = Some(id);
        }
    }
    // 从根往下排：上一条在下一条前面，同一处的几支按先后
    let mut children: HashMap<String, Vec<(u64, usize)>> = HashMap::new();
    for (index, message) in out.iter().enumerate() {
        let id = message["id"].as_str().unwrap_or("");
        let parent = message["parentID"].as_str().unwrap_or(ROOT).to_string();
        children.entry(parent).or_default().push((rank_of.get(id).copied().unwrap_or(main_rank), index));
    }
    let mut order = Vec::with_capacity(out.len());
    let mut stack = vec![ROOT.to_string()];
    while let Some(parent) = stack.pop() {
        let Some(mut kids) = children.remove(&parent) else { continue };
        kids.sort();
        for (_, index) in kids.iter().rev() {
            stack.push(out[*index]["id"].as_str().unwrap_or("").to_string());
        }
        // 一条进 order 时它的上一条早就在里面了（上一条处理时才轮到它）
        order.extend(kids.iter().map(|(_, index)| *index));
    }
    if order.len() == out.len() {
        let mut taken: Vec<Option<Value>> = std::mem::take(out).into_iter().map(Some).collect();
        *out = order.into_iter().filter_map(|index| taken[index].take()).collect();
    }
}

// MARK: - 正在干的一轮

/// 正在干的一轮里的一块
#[derive(Clone, Debug)]
enum Piece {
    Text(String),
    Thinking(String),
    Tool { call: String, name: String, arguments: Value, status: String, result: String, lines: Option<[i64; 2]> },
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

/// result 事件里的增删行数（{added, removed}）
fn lines_of(data: &Value) -> Option<[i64; 2]> {
    Some([data["lines"]["added"].as_i64()?, data["lines"]["removed"].as_i64()?])
}

fn set_piece_lines(pieces: &mut [Piece], call: &str, value: Option<[i64; 2]>) {
    if let Some(index) = find_piece(pieces, call)
        && let Piece::Tool { lines, .. } = &mut pieces[index]
    {
        *lines = value;
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
    /// 这一轮用的模型（用量里显示）
    engine: String,
    /// 会话的标题、工作目录（推动静时用，不每次读存档）
    title: String,
    cwd: String,
    /// 给简报数的
    tally: Tally,
}

impl Live {
    /// 这段会话开始的一轮（turn.started；连上之前就开始的，第一次收到它的事件或 sessions.list 报着时补一个）
    fn start(session: &str, turn: &str, text: &str) -> Live {
        let stored = stored_session(session);
        let title = stored.as_ref().map(|stored| stored.title.clone()).filter(|title| !title.is_empty());
        Live {
            turn: turn.to_string(),
            node: stored.as_ref().map_or(0, |stored| group(&stored.id, &stored.history, &Team::new()).len()),
            title: title.unwrap_or_else(|| text.replace('\n', " ").chars().take(40).collect()),
            cwd: stored.map(|stored| stored.cwd).unwrap_or_default(),
            tally: Tally::default(),
            segments: vec![Segment { text: text.to_string(), ..Default::default() }],
            started: SystemTime::now(),
            tokens: 0.0,
            seconds: 0.0,
            pending: 0.0,
            first_output: None,
            context: json!({}),
            engine: engine_for(session)["title"].as_str().unwrap_or("EDITH").to_string(),
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
        let tool =
            Piece::Tool { call: call.into(), name: name.into(), arguments, status: "running".into(), result: String::new(), lines: None };
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
            "engineName": self.engine,
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
                Piece::Tool { call, name, arguments, status, result, lines } => {
                    let nth = uses.entry(call.as_str()).or_insert(0);
                    let place = Place { session, owner: session, node, agent: None, team };
                    let block = tool_block(place, Card { call, nth: *nth, name, arguments, status, result, lines: *lines });
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
                        lines: lines_of(piece),
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
                lines: None,
            }),
            "result" => {
                let status = if data["ok"].as_bool().unwrap_or(true) { "succeeded" } else { "failed" };
                let call = data["id"].as_str().unwrap_or("");
                set_piece(&mut self.pieces, call, status, Some(text));
                set_piece_lines(&mut self.pieces, call, lines_of(data));
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
                // 它还没看的追加指示（Agent 树里能拿掉）
                "queue": agent.meta["inbox"].as_array().into_iter().flatten().map(|item| json!({
                    "id": agent_inbox_uuid(session, agent.id(), item["id"].as_str().unwrap_or("")),
                    "text": item["text"], "attachments": [], "createdAt": agent.meta["started"],
                })).collect::<Vec<_>>(),
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
    /// 排着的那句（QueuedInput 的 id）→ EDITH 排队时回的轮（拿掉时按它）
    queued_turns: HashMap<String, String>,
    queue_revision: u64,
    /// 正在流式写、该推一次 message 的会话
    dirty: HashSet<String>,
    /// 子智能体
    agents: Team,
    /// 读过存下来的子智能体的会话
    agents_loaded: HashSet<String>,
    /// 子智能体变了、该推一次的会话
    team_dirty: HashSet<String>,
    /// 订了会话的动静（activity.subscribe）
    activity: bool,
    activity_seq: u64,
    /// 每段会话最近一轮的简报
    briefs: HashMap<String, Value>,
    /// 开着设置页（config.subscribe）
    config: bool,
    /// 看过记住的事（memory.*）：之后推 memory 事件，按它要的最近几条截
    memory: Option<usize>,
    /// 各个大脑开着的终端（工作目录 → TerminalInfo 们）
    terminals: HashMap<String, Vec<Value>>,
    /// 各个大脑的浏览器（工作目录 → BrowserState），标签在哪个大脑里（刚开的还没等到 browser 通知）
    browsers: HashMap<String, Value>,
    browser_tabs: HashMap<String, String>,
}

static STATE: LazyLock<Mutex<State>> = LazyLock::new(|| Mutex::new(State::default()));

// MARK: - 模型、密钥（EDITH 的 ~/.edith/config.json、secrets.json）

fn config_path() -> PathBuf {
    edith_home().join("config.json")
}

fn read_json_file(path: &Path) -> Value {
    std::fs::read(path).ok().and_then(|bytes| serde_json::from_slice(&bytes).ok()).filter(Value::is_object).unwrap_or(json!({}))
}

/// 写 EDITH 的配置文件（里面有密钥：只有自己能读；先写临时文件再换名）
fn write_json_file(path: &Path, value: &Value) -> Result<(), (i64, String)> {
    let failed = |error: std::io::Error| (NOT_ALLOWED, format!("写不了 {}：{error}", path.display()));
    std::fs::create_dir_all(edith_home()).map_err(failed)?;
    let temporary = path.with_extension(format!("json.{}", std::process::id()));
    std::fs::write(&temporary, serde_json::to_vec_pretty(value).unwrap_or_default()).map_err(failed)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o600));
    }
    std::fs::rename(&temporary, path).map_err(failed)
}

/// 没手动设思考方式时（同 friday.core 的 inferred_thinking）：自己开关的 toggle，本机的不猜，在线的分档
fn inferred_thinking(base_url: &str, sends_enable_thinking: bool) -> &'static str {
    let host = base_url.to_lowercase();
    let local = ["://localhost", "://127.0.0.1", "://[::1]", "://0.0.0.0"].iter().any(|prefix| host.contains(prefix))
        || host.starts_with("power://");
    if sends_enable_thinking {
        "toggle"
    } else if local {
        "none"
    } else {
        "effort"
    }
}

/// EDITH 用的模型 → Friday 的 EngineConfig：顶层那个（baseURL / model，环境变量盖掉）id 是 edith；config.json 的 engines 本来就是 Friday 的格式
fn engines() -> Vec<Value> {
    let config = read_json_file(&config_path());
    let mut list = Vec::new();
    let model =
        std::env::var("EDITH_MODEL").ok().filter(|model| !model.is_empty()).or_else(|| config["model"].as_str().map(str::to_string));
    let base =
        std::env::var("EDITH_BASE_URL").ok().filter(|base| !base.is_empty()).or_else(|| config["baseURL"].as_str().map(str::to_string));
    if let Some(model) = model.filter(|model| !model.is_empty()) {
        let base = base.unwrap_or_default();
        list.push(json!({
            "id": ENGINE_ID,
            "title": format!("EDITH · {model}"),
            "baseURL": base,
            "apiKeySecret": "EDITH_API_KEY",
            "model": model,
            "supportsVision": config["supportsVision"].as_bool().unwrap_or(false),
            "supportsTools": true,
            "sendsEnableThinking": false,
            "extraBody": config["extraBody"].as_str().unwrap_or(""),
            "thinking": config["thinking"].as_str().unwrap_or_else(|| inferred_thinking(&base, false)),
        }));
    }
    for entry in config["engines"].as_array().into_iter().flatten().filter(|entry| entry["id"].is_string()) {
        let mut engine = entry.clone();
        let text = |key: &str| entry[key].as_str().unwrap_or("").to_string();
        if text("title").is_empty() {
            engine["title"] = json!(text("model"));
        }
        if engine["thinking"].as_str().is_none_or(str::is_empty) {
            engine["thinking"] = json!(inferred_thinking(&text("baseURL"), entry["sendsEnableThinking"].as_bool().unwrap_or(false)));
        }
        for (key, default) in [("supportsVision", true), ("supportsTools", true), ("sendsEnableThinking", false)] {
            if !engine[key].is_boolean() {
                engine[key] = json!(default);
            }
        }
        list.push(engine);
    }
    list
}

/// 新对话默认用的：config.json 的 defaultEngine（在列表里的），没有就第一个
fn default_engine_id(engines: &[Value]) -> String {
    let wanted = read_json_file(&config_path())["defaultEngine"].as_str().map(str::to_string);
    wanted
        .filter(|id| engines.iter().any(|engine| engine["id"] == json!(id)))
        .or_else(|| engines.first().and_then(|engine| engine["id"].as_str()).map(str::to_string))
        .unwrap_or_else(|| ENGINE_ID.into())
}

/// 这段会话用的模型：界面上选的（还在的），没选用默认的
fn engine_for(session: &str) -> Value {
    let all = engines();
    let chosen = META.setting(session, "engineID").filter(|id| all.iter().any(|engine| engine["id"] == json!(id)));
    let id = chosen.unwrap_or_else(|| default_engine_id(&all));
    all.into_iter().find(|engine| engine["id"] == json!(id)).unwrap_or_else(|| json!({ "id": ENGINE_ID, "title": "EDITH", "model": "" }))
}

fn engines_state() -> Value {
    let all = engines();
    let config = read_json_file(&config_path());
    json!({
        "defaultEngineID": default_engine_id(&all),
        "engines": all,
        "disallowedSubAgentEngines": config["disallowedSubAgentEngines"].as_array().cloned().unwrap_or_default(),
        "disabledServices": [],
    })
}

/// 思考档位（同 Friday 的 EngineConfig::thinkingLevels）
fn thinking_levels(engine: &Value) -> Vec<&'static str> {
    match engine["thinking"].as_str().unwrap_or("none") {
        "toggle" | "thinkingType" => vec!["off", "medium"],
        "effort" => vec!["off", "low", "medium", "high"],
        _ => Vec::new(),
    }
}

/// 密钥库里有哪些名字（值不出这台）
fn secret_names() -> Value {
    let secrets = read_json_file(&edith_home().join("secrets.json"));
    let mut names: Vec<&String> = secrets.as_object().map(|all| all.keys().collect()).unwrap_or_default();
    names.sort();
    json!({ "names": names })
}

/// engines.update / remove、secrets.set / remove、设默认模型、给不给子智能体用：改 EDITH 的 config.json、secrets.json（下一轮起生效）
fn engines_request(method: &str, params: &Value) -> Reply {
    if method.starts_with("secrets.") {
        let name = text_param(params, "name")?;
        let path = edith_home().join("secrets.json");
        let mut secrets = read_json_file(&path);
        if method == "secrets.set" {
            secrets[&name] = json!(params["value"].as_str().unwrap_or(""));
        } else if let Some(all) = secrets.as_object_mut() {
            all.remove(&name);
        }
        write_json_file(&path, &secrets)?;
        event("secrets", secret_names());
        return Ok(json!({}));
    }
    let path = config_path();
    let mut config = read_json_file(&path);
    let mut list: Vec<Value> = config["engines"].as_array().cloned().unwrap_or_default();
    match method {
        "engines.update" => {
            let engine = params["engine"].clone();
            let id = engine["id"].as_str().filter(|id| !id.is_empty()).ok_or((-32602, "engine 要有 id".to_string()))?.to_string();
            if id == ENGINE_ID {
                // 顶层那个：改 config.json 顶层的几项
                for (from, to) in [
                    ("baseURL", "baseURL"),
                    ("model", "model"),
                    ("supportsVision", "supportsVision"),
                    ("extraBody", "extraBody"),
                    ("thinking", "thinking"),
                ] {
                    if !engine[from].is_null() {
                        config[to] = engine[from].clone();
                    }
                }
            } else {
                match list.iter_mut().find(|entry| entry["id"] == json!(id)) {
                    Some(entry) => *entry = engine,
                    None => list.push(engine),
                }
            }
        }
        "engines.remove" => {
            let id = text_param(params, "id")?;
            if id == ENGINE_ID {
                return Err((NOT_ALLOWED, "顶层的模型不能在这里删（改 ~/.edith/config.json）".into()));
            }
            list.retain(|entry| entry["id"] != json!(id));
        }
        _ => {
            // engines.setSubAgentAllowed：EDITH 的子智能体用派它的那一轮的模型，这里只记下给设置页看
            let id = text_param(params, "id")?;
            let mut disallowed: Vec<Value> = config["disallowedSubAgentEngines"].as_array().cloned().unwrap_or_default();
            disallowed.retain(|item| *item != json!(id));
            if params["allowed"].as_bool() == Some(false) {
                disallowed.push(json!(id));
            }
            config["disallowedSubAgentEngines"] = json!(disallowed);
        }
    }
    if list.is_empty() {
        if let Some(object) = config.as_object_mut() {
            object.remove("engines");
        }
    } else {
        config["engines"] = json!(list);
    }
    write_json_file(&path, &config)?;
    event("engines", engines_state());
    Ok(json!({}))
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

/// 这段会话的权限模式：选过的，没选过的按设置里新对话的默认（config.json 的 defaultPermissionMode），再没有是自动
fn permission_mode(id: &str) -> String {
    META.setting(id, "permissionMode").unwrap_or_else(default_permission_mode)
}

fn default_permission_mode() -> String {
    read_json_file(&config_path())["defaultPermissionMode"]
        .as_str()
        .filter(|mode| PERMISSION_MODES.contains(mode))
        .unwrap_or("auto")
        .to_string()
}

const PERMISSION_MODES: [&str; 3] = ["auto", "plan", "full"];

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
        "engineID": engine_for(&session.id)["id"],
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
        "engine": engine_for(&session.id),
        "thinkingLevel": META.setting(&session.id, "thinking").unwrap_or_else(|| "off".into()),
        "thinkingLevels": thinking_levels(&engine_for(&session.id)),
        "permissionMode": permission_mode(&session.id),
        "mainLiveSpeed": 0,
    });
    if !session.cwd.is_empty() {
        value["workingDirectory"] = json!(session.cwd);
    }
    if !state.generating.contains(&session.id)
        && let Some(failed) = failed_message(session)
    {
        value["failedMessageID"] = json!(failed);
        value["canRetry"] = json!(true);
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
        "engineID": engine_for(&session.id)["id"],
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
    let mut ended = None;
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
                    live.segments[0].from_agent = from_agent.clone();
                }
                state.generating.insert(session.clone());
                let by = if from_agent.is_some() { "agent" } else { "user" };
                if let Some(live) = state.live.get_mut(&session) {
                    live.tally.by = by.into();
                }
                let request = data["text"].as_str().unwrap_or("").chars().take(500).collect::<String>();
                activity_event(&mut state, &session, "started", json!({ "by": by, "request": request }));
                // 排着的那句开始了：从队列里拿掉（按 EDITH 排队时回的轮对，对不上再按原话）
                let text = data["text"].as_str().unwrap_or("");
                let by_turn: Option<String> = state.queued_turns.iter().find(|(_, queued)| **queued == turn).map(|(item, _)| item.clone());
                if let Some(queue) = state.queued.get_mut(&session)
                    && let Some(index) = queue
                        .iter()
                        .position(|item| by_turn.as_ref().is_some_and(|id| item["id"] == json!(id)))
                        .or_else(|| queue.iter().position(|item| item["text"] == text))
                {
                    let removed = queue.remove(index);
                    if let Some(id) = removed["id"].as_str() {
                        state.queued_turns.remove(id);
                    }
                    state.queue_revision += 1;
                }
                true
            }
            "done" => {
                if let Some(live) = state.live.remove(&session) {
                    META.set_stats(&session, live.stats_json(false));
                    let brief = turn_brief(&session, &live, data);
                    state.briefs.insert(session.clone(), brief.clone());
                    activity_event(&mut state, &session, "finished", json!({ "brief": brief }));
                }
                ended = Some((data["error"].as_str().unwrap_or("").to_string(), data["stopped"] == true));
                state.generating.remove(&session);
                state.stopping.remove(&session);
                state.dirty.remove(&session);
                let gone: Vec<String> =
                    state.approvals.iter().filter(|(_, pending)| pending.session == session).map(|(id, _)| id.clone()).collect();
                if !gone.is_empty() {
                    state.approvals.retain(|_, pending| pending.session != session);
                    let resolved = gone.iter().map(|id| json!({ "eventID": id, "outcome": "stopped" })).collect();
                    approvals_changed(&mut state, &session, resolved);
                }
                true
            }
            _ => {
                state.generating.insert(session.clone());
                let live = state.live.get_mut(&session).unwrap();
                let text = data["text"].as_str().unwrap_or("");
                let mut structure = false;
                let mut announce = None;
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
                        let (call, name) = (data["id"].as_str().unwrap_or(""), data["name"].as_str().unwrap_or(""));
                        live.push_tool(call, name, data["arguments"].clone());
                        if let Some(mut tool) = live.tally.note_tool(name, &data["arguments"]) {
                            tool["eventID"] = json!(live.card_id(&session, call));
                            announce = Some(tool);
                        }
                    }
                    "result" => {
                        let status = if data["ok"].as_bool().unwrap_or(true) { "succeeded" } else { "failed" };
                        let call = data["id"].as_str().unwrap_or("");
                        if data["name"] == "run_shell" && status == "failed" {
                            live.tally.failed += 1;
                        }
                        live.set_tool(call, status, Some(text));
                        if let Some(lines) = lines_of(data)
                            && let Some((segment, _)) = live.find_tool(call)
                        {
                            set_piece_lines(&mut live.segments[segment].pieces, call, Some(lines));
                            META.set_line(&session, &live.card_id(&session, call), lines);
                        }
                    }
                    "stats" => live.stats(data),
                    "notice" => live.notice(text),
                    _ => {}
                }
                state.dirty.insert(session.clone());
                if let Some(tool) = announce {
                    activity_event(&mut state, &session, "tool", json!({ "tool": tool }));
                }
                structure
            }
        }
    };
    // 出错、被停下：存档里不记，记下那时存档有几条（之后变了就不算了）
    if let Some((error, stopped)) = ended {
        let at = stored_session(&session).map_or(0, |stored| stored.history.len());
        META.set_ended(&session, at, &error, stopped);
    }
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
            approvals_changed(&mut state, &session, Vec::new());
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
        approvals_changed(&mut state, &session, Vec::new());
        state.dirty.insert(session.clone());
    }
    push_live_message(&session);
    event("attention", json!({}));
    changed(&session, false);
}

/// 这件事已经有人答了（同一台的另一个客户端，或停下了）：收掉这边的待确认
fn on_approve_cancel(params: &Value) {
    let session = params["session"].as_str().unwrap_or("").to_string();
    let call = params["call"].as_str().unwrap_or("");
    let agent = params["agent"].as_str().filter(|agent| !agent.is_empty()).map(str::to_string);
    let removed = {
        let mut state = STATE.lock().unwrap();
        let gone: Vec<String> = state
            .approvals
            .iter()
            .filter(|(_, pending)| pending.session == session && pending.call == call && pending.agent == agent)
            .map(|(id, _)| id.clone())
            .collect();
        for id in &gone {
            state.approvals.remove(id);
        }
        let removed = !gone.is_empty();
        if removed {
            let resolved = gone.iter().map(|id| json!({ "eventID": id, "outcome": "answered" })).collect();
            approvals_changed(&mut state, &session, resolved);
            match &agent {
                Some(agent) => {
                    if let Some(view) = state.agents.get_mut(agent) {
                        set_piece(&mut view.pieces, call, "running", None);
                    }
                    state.team_dirty.insert(session.clone());
                }
                None => {
                    if let Some(live) = state.live.get_mut(&session) {
                        live.set_tool(call, "running", None);
                    }
                }
            }
            state.dirty.insert(session.clone());
        }
        removed
    };
    if removed {
        changed(&session, false);
    }
}

// MARK: - 会话的动静（activity.*：客户端的助手看这台的会话，同 Friday 的 ActivityTap / TurnBrief）

/// 一轮里给简报数的：改了哪些文件、跑了几条命令（几条没成）、派了几个子智能体；谁开始的；上一条工具动静什么时候推的
#[derive(Clone, Debug, Default)]
struct Tally {
    files: HashSet<String>,
    commands: i64,
    failed: i64,
    agents: i64,
    by: String,
    last_tool: Option<Instant>,
}

impl Tally {
    /// 记下一个工具；值得一提的（跑命令、写文件、派子智能体、上网）每段会话每 2 秒最多推一条
    fn note_tool(&mut self, name: &str, arguments: &Value) -> Option<Value> {
        let text = |key: &str| arguments[key].as_str().unwrap_or("").to_string();
        let summary = match name {
            "write_file" | "edit_file" => {
                self.files.insert(text("path"));
                text("path")
            }
            "run_shell" => {
                self.commands += 1;
                text("command")
            }
            "delegate_task" => {
                self.agents += 1;
                text("task")
            }
            "run_parallel_tasks" | "run_workflow" => {
                let count = arguments["tasks"].as_array().or(arguments["steps"].as_array()).map_or(1, Vec::len);
                self.agents += count as i64;
                format!("{count} 个子智能体")
            }
            "fetch_url" => text("url"),
            "web_search" => text("query"),
            _ => return None,
        };
        if self.last_tool.is_some_and(|last| last.elapsed() < Duration::from_secs(2)) {
            return None;
        }
        self.last_tool = Some(Instant::now());
        let title = tool_look(name).map_or(name, |look| look.0);
        Some(json!({ "tool": name, "title": title, "summary": summary.chars().take(200).collect::<String>() }))
    }
}

/// 去掉 Markdown 的样子（简报的 answer、line）
fn plain(text: &str) -> String {
    text.lines()
        .map(|line| line.trim().trim_start_matches(['#', '>', '-', '*', ' ']).replace(['*', '`'], ""))
        .filter(|line| !line.trim().is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

/// 一轮做完的简报（同 Friday 的 TurnBrief）：状态、挑出来的一句、改了几个文件、跑了几条命令……
fn turn_brief(session: &str, live: &Live, done: &Value) -> Value {
    let answer = plain(done["answer"].as_str().unwrap_or(""));
    let error = done["error"].as_str().filter(|error| !error.is_empty());
    let asks = answer.trim_end().ends_with(['？', '?']);
    let status = if error.is_some() {
        "error"
    } else if done["stopped"] == true {
        "stopped"
    } else if asks {
        "question"
    } else {
        "done"
    };
    // 挑一句：问句挑最后一句，别的挑第一句
    let sentences: Vec<&str> =
        answer.split_inclusive(['。', '！', '？', '!', '?', '\n']).map(str::trim).filter(|line| !line.is_empty()).collect();
    let picked = if asks { sentences.last() } else { sentences.first() };
    let line = error.map(str::to_string).unwrap_or_else(|| picked.map(|line| line.chars().take(80).collect()).unwrap_or_default());
    let mut brief = json!({
        "messageID": message_uuid(session, live.node + 2 * (live.segments.len() - 1) + 1),
        "status": status,
        "line": line,
        "filesChanged": live.tally.files.len(),
        "commands": live.tally.commands,
        "commandsFailed": live.tally.failed,
        "subAgents": live.tally.agents,
        "seconds": live.started.elapsed().map_or(0, |elapsed| elapsed.as_secs()),
        "stepsDone": 0,
        "stepsTotal": 0,
        "finishedAt": iso(SystemTime::now()),
        "answer": answer.chars().take(1200).collect::<String>(),
    });
    if asks {
        brief["question"] = json!(line);
    }
    brief
}

/// 推一条动静给订了的客户端（标题、工作目录按正在干的那一轮记的，没有就读存档）
fn activity_event(state: &mut State, session: &str, kind: &str, extra: Value) {
    if !state.activity {
        return;
    }
    state.activity_seq += 1;
    let (title, cwd) = match state.live.get(session) {
        Some(live) => (live.title.clone(), live.cwd.clone()),
        None => stored_session(session).map(|stored| (stored.title, stored.cwd)).unwrap_or_default(),
    };
    let mut params =
        json!({ "conversationID": upper(session), "kind": kind, "seq": state.activity_seq, "at": iso(SystemTime::now()), "title": title });
    if !cwd.is_empty() {
        params["workingDirectory"] = json!(cwd);
    }
    if let (Some(params), Some(extra)) = (params.as_object_mut(), extra.as_object()) {
        params.extend(extra.clone());
    }
    event("activity", params);
}

/// 这段会话在等的批准（PendingApproval）
fn pending_approvals(state: &State, session: &str) -> Vec<Value> {
    state
        .approvals
        .iter()
        .filter(|(_, pending)| pending.session == session)
        .map(|(id, pending)| {
            let tool = pending.request["params"]["tool"].as_str().unwrap_or("");
            json!({ "eventID": id, "tool": tool, "title": tool_look(tool).map_or(tool, |look| look.0), "summary": pending.request["params"]["summary"] })
        })
        .collect()
}

/// 待确认的变了：现在全部的，和少掉的那几个各是怎么结束的
fn approvals_changed(state: &mut State, session: &str, resolved: Vec<Value>) {
    let approvals = pending_approvals(state, session);
    let mut extra = json!({ "approvals": approvals });
    if !resolved.is_empty() {
        extra["resolved"] = json!(resolved);
    }
    activity_event(state, session, "approval", extra);
}

/// activity.subscribe 的快照：有内容的会话现在的样子（新的在前）
fn activity_snapshot(state: &State) -> Value {
    let sessions: Vec<Value> = all_sessions()
        .into_iter()
        .filter(|session| !session.history.is_empty() || state.live.contains_key(&session.id))
        .map(|session| {
            let approvals = pending_approvals(state, &session.id);
            let status = if !approvals.is_empty() {
                "approval"
            } else if state.generating.contains(&session.id) {
                "running"
            } else if failed_message(&session).is_some() {
                "error"
            } else if state.briefs.get(&session.id).is_some_and(|brief| brief["status"] == "question") {
                "question"
            } else {
                "idle"
            };
            let mut item = json!({
                "id": upper(&session.id), "title": title(&session, state), "status": status, "approvals": approvals,
                "updatedAt": session.updated_at(),
            });
            if !session.cwd.is_empty() {
                item["workingDirectory"] = json!(session.cwd);
            }
            if let Some(brief) = state.briefs.get(&session.id) {
                item["lastBrief"] = brief.clone();
            }
            if let Some(live) = state.live.get(&session.id).filter(|live| !live.tally.by.is_empty()) {
                item["startedBy"] = json!(live.tally.by);
            }
            item
        })
        .collect();
    json!({ "sessions": sessions, "assistant": false })
}

// MARK: - 终端（term.*：EDITH 大脑开的伪终端，客户端断了照常跑）

/// TerminalsState：全部大脑的终端（会话 id 换成 Friday 的大写）
fn terminals_state_of(state: &State) -> Value {
    let mut all: Vec<Value> = state
        .terminals
        .values()
        .flatten()
        .map(|terminal| {
            let mut terminal = terminal.clone();
            terminal["conversationID"] = json!(upper(terminal["conversationID"].as_str().unwrap_or("")));
            terminal
        })
        .collect();
    all.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
    json!({ "terminals": all, "available": true })
}

fn terminals_state() -> Value {
    terminals_state_of(&STATE.lock().unwrap())
}

/// 这个终端在哪个大脑里
fn terminal_brain(id: &str) -> Result<Arc<Brain>, (i64, String)> {
    let state = STATE.lock().unwrap();
    let workspace = state
        .terminals
        .iter()
        .find(|(_, list)| list.iter().any(|terminal| terminal["id"] == json!(id)))
        .map(|(workspace, _)| workspace.clone())
        .ok_or((NOT_FOUND, format!("没有终端 {id}")))?;
    state.brains.get(&workspace).cloned().ok_or((NOT_FOUND, format!("终端 {id} 的大脑没连着")))
}

fn terminal_request(method: &str, params: &Value) -> Reply {
    let id = text_param(params, "id")?;
    let forward = |brain: &Brain, target: &str, request: Value| {
        brain.request(target, request, Duration::from_secs(30)).map_err(|error| (NOT_FOUND, error))
    };
    if method == "term.open" {
        // 在这段会话所在目录的大脑里开（没发过话的空白会话也行：它记着工作目录）
        let session = session_param(params, "conversationID")?;
        let directory = params["workingDirectory"]
            .as_str()
            .filter(|dir| !dir.is_empty())
            .map(str::to_string)
            .or_else(|| (!session.cwd.is_empty()).then(|| session.cwd.clone()))
            .unwrap_or_else(default_directory);
        let brain = brain_for(&directory).map_err(|error| (NOT_ALLOWED, error))?;
        let mut request = json!({ "id": id, "session": session.id, "cwd": directory, "cols": params["cols"], "rows": params["rows"] });
        if let Some(command) = params["command"].as_str() {
            request["command"] = json!(command);
        }
        let info = forward(&brain, "terminals.open", request)?;
        {
            let mut state = STATE.lock().unwrap();
            let list = state.terminals.entry(brain.workspace.clone()).or_default();
            list.retain(|terminal| terminal["id"] != json!(id));
            list.push(info);
        }
        event("terminals", terminals_state());
        return Ok(json!({}));
    }
    let brain = terminal_brain(&id)?;
    let target = match method {
        "term.attach" => "terminals.attach",
        "term.detach" => "terminals.detach",
        "term.write" => "terminals.write",
        "term.resize" => "terminals.resize",
        "term.close" => "terminals.close",
        other => return Err((-32601, format!("这一版的 EDITH 网关没有 {other}"))),
    };
    let mut request = json!({ "id": id });
    for key in ["data", "cols", "rows"] {
        if !params[key].is_null() {
            request[key] = params[key].clone();
        }
    }
    let reply = forward(&brain, target, request)?;
    Ok(if method == "term.attach" { reply } else { json!({}) })
}

// MARK: - 浏览器（browser.*：EDITH 大脑起的无头 Chrome，标签断了照常在）

/// BrowserState：全部大脑的标签（会话 id 换成 Friday 的大写）；有一个大脑能用就算能用，都还没连上时先说能用（开的时候才知道）
fn browser_state_of(state: &State) -> Value {
    let mut tabs = Vec::new();
    let mut reason = None;
    let mut available = state.browsers.is_empty();
    for browser in state.browsers.values() {
        if browser["available"] == true {
            available = true;
        } else if reason.is_none() {
            reason = browser["reason"].as_str().map(str::to_string);
        }
        for tab in browser["tabs"].as_array().into_iter().flatten() {
            let mut tab = tab.clone();
            tab["conversationID"] = json!(upper(tab["conversationID"].as_str().unwrap_or("")));
            tabs.push(tab);
        }
    }
    let mut value = json!({ "available": available, "tabs": tabs });
    if !available && let Some(reason) = reason {
        value["reason"] = json!(reason);
    }
    value
}

fn browser_state() -> Value {
    browser_state_of(&STATE.lock().unwrap())
}

fn browser_request(method: &str, params: &Value) -> Reply {
    if method == "browser.open" {
        // 在这段会话所在目录的大脑里开
        let session = session_param(params, "conversationID")?;
        let directory = if session.cwd.is_empty() { default_directory() } else { session.cwd.clone() };
        let brain = brain_for(&directory).map_err(|error| (NOT_ALLOWED, error))?;
        let opened = brain
            .request("browser.open", json!({ "session": session.id, "url": params["url"].as_str().unwrap_or("") }), Duration::from_secs(60))
            .map_err(|error| (NOT_ALLOWED, error))?;
        if let Some(tab) = opened["tab"].as_str() {
            STATE.lock().unwrap().browser_tabs.insert(tab.to_string(), brain.workspace.clone());
        }
        return Ok(opened);
    }
    // 找不到标签的不理（多半刚关了，同 Friday）
    let tab = params["tab"].as_str().unwrap_or("");
    let brain = {
        let state = STATE.lock().unwrap();
        state.browser_tabs.get(tab).and_then(|workspace| state.brains.get(workspace)).cloned()
    };
    let Some(brain) = brain else { return Ok(json!({})) };
    brain.request(method, params.clone(), Duration::from_secs(30)).map_err(|error| (NOT_ALLOWED, error))?;
    Ok(json!({}))
}

// MARK: - 设置页（config.*、settings.*、tools.*、mcp.*：改的是 EDITH 的 config.json、mcp.json、技能目录）

/// SettingsState（设置 › 智能体）
fn settings_state() -> Value {
    let config = read_json_file(&config_path());
    let all = engines();
    json!({
        "systemPrompt": config["systemPrompt"].as_str().unwrap_or(""),
        "defaultSystemPrompt": "",
        "enableThinking": config["enableThinking"].as_bool().unwrap_or(false),
        "requireApproval": config["permissions"]["mode"].as_str() != Some("auto"),
        "confirmPaidCalls": false,
        "defaultPermissionMode": default_permission_mode(),
        "defaultEngineID": default_engine_id(&all),
        "workingDirectory": default_directory(),
        "teamMode": META.value()["teamMode"].as_str().unwrap_or("delegatesHeavy"),
        "userStops": [],
        "lightEngineID": "follow",
        "quotaFallback": "wait",
        "subAgentEngineID": "",
    })
}

/// EDITH 的工具清单（tools.list，问默认工作目录的大脑：它才知道接了哪些能力、MCP 服务有哪些工具）
fn tools_listing() -> Value {
    brain_for(&default_directory())
        .ok()
        .and_then(|brain| brain.request("tools.list", json!({}), Duration::from_secs(15)).ok())
        .filter(Value::is_object)
        .unwrap_or_else(|| json!({}))
}

/// ToolsState（设置 › 工具）
fn tools_state(listing: &Value) -> Value {
    let disabled: Vec<&str> = listing["disabled"].as_array().into_iter().flatten().filter_map(Value::as_str).collect();
    let descriptors: Vec<Value> = listing["tools"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|tool| {
            let name = tool["name"].as_str().unwrap_or("");
            let permission = if tool["readOnly"] == true {
                "readOnly"
            } else {
                match name {
                    "write_file" | "edit_file" | "save_memory" => "writeFiles",
                    "fetch_url" | "web_search" => "network",
                    _ => "execute",
                }
            };
            let (title, symbol) = tool_look(name).unwrap_or((name, "wrench.and.screwdriver"));
            let group = tool["group"].as_str().unwrap_or("builtin");
            let mut descriptor = json!({
                "name": name, "title": title, "description": tool["description"], "symbol": symbol, "permission": permission,
                "origin": if group == "capability" { "capability" } else { group }, "enabled": !disabled.contains(&name),
                "alwaysConfirm": false, "consumesQuota": false, "problem": null,
            });
            if tool["parameters"].is_object() {
                descriptor["parametersText"] = json!(pretty(&tool["parameters"]));
            }
            descriptor
        })
        .collect();
    let skills: Vec<Value> = listing["skills"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|skill| {
            let file = skill["file"].as_str().unwrap_or("");
            let directory = Path::new(file).parent().map(|dir| dir.to_string_lossy().into_owned()).unwrap_or_default();
            json!({ "name": skill["name"], "declaredName": skill["name"], "description": skill["description"], "directory": directory, "file": file })
        })
        .collect();
    json!({
        "descriptors": descriptors, "scriptTools": [], "toolsDirectory": "", "skills": skills,
        "skillsDirectory": listing["skillsDirectory"].as_str().unwrap_or(""),
    })
}

fn mcp_path() -> PathBuf {
    edith_home().join("mcp.json")
}

/// EDITH 全局那层的 mcp.json 里的服务（{名字: 配置}）
fn mcp_servers() -> Map<String, Value> {
    read_json_file(&mcp_path())["mcpServers"].as_object().cloned().unwrap_or_default()
}

fn write_mcp_servers(servers: Map<String, Value>) -> Result<(), (i64, String)> {
    let mut file = read_json_file(&mcp_path());
    file["mcpServers"] = Value::Object(servers);
    write_json_file(&mcp_path(), &file)
}

/// McpState（设置 › 工具 › MCP 服务）：服务是 EDITH 合起来的三层（全局、项目、本地）；EDITH 用到时才连，连过的报工具
fn mcp_state(listing: &Value) -> Value {
    let unknown: Vec<&str> = listing["mcp"]["unknown"].as_array().into_iter().flatten().filter_map(Value::as_str).collect();
    let tools: Vec<&Value> = listing["tools"].as_array().into_iter().flatten().collect();
    let mut servers: Map<String, Value> = listing["mcp"]["servers"].as_object().cloned().unwrap_or_default();
    // 大脑没起来时只看全局那层
    if servers.is_empty() {
        servers = mcp_servers();
    }
    let list: Vec<Value> = servers
        .iter()
        .map(|(name, config)| {
            let prefix = format!("{name}__");
            let names: Vec<String> = tools
                .iter()
                .filter_map(|tool| tool["name"].as_str())
                .filter_map(|tool| tool.strip_prefix(&prefix).map(str::to_string))
                .collect();
            let disabled = config["disabled"] == true;
            let status = if disabled {
                "disabled"
            } else if unknown.contains(&name.as_str()) {
                "connecting"
            } else {
                "connected"
            };
            let remote = config["url"].is_string();
            let summary = if remote {
                config["url"].as_str().unwrap_or("").to_string()
            } else {
                let args: Vec<&str> = config["args"].as_array().into_iter().flatten().filter_map(Value::as_str).collect();
                format!("{} {}", config["command"].as_str().unwrap_or(""), args.join(" ")).trim().to_string()
            };
            json!({
                "name": name, "config": config, "summary": summary, "status": status,
                "error": if status == "connecting" { "用到时再连" } else { "" },
                "tools": names, "toolCount": names.len(), "log": "", "autoApprove": config["autoApprove"] == true, "disabled": disabled, "isRemote": remote,
            })
        })
        .collect();
    let text = std::fs::read_to_string(mcp_path()).unwrap_or_else(|_| "{\n  \"mcpServers\": {}\n}\n".into());
    json!({
        "servers": list, "configPath": mcp_path().to_string_lossy(), "homeDirectory": user_home().to_string_lossy(),
        "configText": text, "missingSecrets": {},
    })
}

fn capabilities_state() -> Value {
    json!({ "entries": [], "packages": [], "problems": [], "jobs": [], "availability": {}, "optionSpecs": {}, "packageFor": {}, "providersDirectory": "" })
}

/// 设置页开着的客户端：推变了的那几份
fn config_changed(parts: &[&str]) {
    if !STATE.lock().unwrap().config {
        return;
    }
    let listing = if parts.iter().any(|part| *part == "tools" || *part == "mcp") { tools_listing() } else { json!({}) };
    let mut params = json!({});
    for part in parts {
        params[*part] = match *part {
            "settings" => settings_state(),
            "tools" => tools_state(&listing),
            "mcp" => mcp_state(&listing),
            _ => capabilities_state(),
        };
    }
    event("config", params);
}

/// 贴来的 MCP 配置：Claude Desktop / Cursor 的 {"mcpServers": {…}}、VS Code 的 {"servers": {…}}、或者直接 {名字: 配置}
fn parse_mcp_text(text: &str) -> Result<Map<String, Value>, String> {
    let value: Value = serde_json::from_str(text.trim()).map_err(|error| format!("不是 JSON：{error}"))?;
    let servers = value.get("mcpServers").or_else(|| value.get("servers")).cloned().unwrap_or(value);
    let servers = servers.as_object().cloned().ok_or("没找到服务的配置")?;
    if servers.values().any(|config| !config.is_object()) {
        return Err("每个服务的配置都要是对象".into());
    }
    Ok(servers)
}

fn settings_request(method: &str, params: &Value) -> Reply {
    let name = || text_param(params, "name");
    match method {
        "config.subscribe" => {
            STATE.lock().unwrap().config = true;
            let listing = tools_listing();
            Ok(
                json!({ "settings": settings_state(), "tools": tools_state(&listing), "mcp": mcp_state(&listing), "capabilities": capabilities_state() }),
            )
        }
        "config.unsubscribe" => {
            STATE.lock().unwrap().config = false;
            Ok(json!({}))
        }
        "settings.get" => Ok(settings_state()),
        "settings.update" => {
            if let Some(mode) = params["defaultPermissionMode"].as_str().filter(|mode| !PERMISSION_MODES.contains(mode)) {
                return Err((-32602, format!("不认识的权限模式：{mode}")));
            }
            let path = config_path();
            let mut config = read_json_file(&path);
            for (from, to) in
                [("systemPrompt", "systemPrompt"), ("enableThinking", "enableThinking"), ("defaultPermissionMode", "defaultPermissionMode")]
            {
                if !params[from].is_null() {
                    config[to] = params[from].clone();
                }
            }
            if let Some(required) = params["requireApproval"].as_bool() {
                if !config["permissions"].is_object() {
                    config["permissions"] = json!({});
                }
                match required {
                    true => {
                        config["permissions"].as_object_mut().unwrap().remove("mode");
                    }
                    false => config["permissions"]["mode"] = json!("auto"),
                }
            }
            let engines_changed = params["defaultEngineID"].is_string();
            if let Some(engine) = params["defaultEngineID"].as_str() {
                match engine {
                    "" => {
                        config.as_object_mut().unwrap().remove("defaultEngine");
                    }
                    engine => config["defaultEngine"] = json!(engine),
                }
            }
            write_json_file(&path, &config)?;
            if let Some(directory) = params["workingDirectory"].as_str() {
                if !directory.is_empty() && !Path::new(directory).is_dir() {
                    return Err((-32602, format!("工作目录不存在：{directory}")));
                }
                META.update(|value| value["workingDirectory"] = json!(directory));
            }
            if let Some(mode) = params["teamMode"].as_str() {
                META.update(|value| value["teamMode"] = json!(mode));
            }
            if engines_changed {
                event("engines", engines_state());
            }
            config_changed(&["settings"]);
            Ok(settings_state())
        }
        "tools.setEnabled" => {
            let tool = name()?;
            let path = config_path();
            let mut config = read_json_file(&path);
            let mut disabled: Vec<Value> = config["disabledTools"].as_array().cloned().unwrap_or_default();
            disabled.retain(|item| *item != json!(tool));
            if params["enabled"].as_bool() == Some(false) {
                disabled.push(json!(tool));
            }
            config["disabledTools"] = json!(disabled);
            write_json_file(&path, &config)?;
            config_changed(&["tools"]);
            Ok(json!({}))
        }
        "tools.refreshSkills" => {
            config_changed(&["tools"]);
            Ok(json!({}))
        }
        "tools.skill" | "tools.deleteSkill" => {
            let wanted = name()?;
            let listing = tools_listing();
            let skill = listing["skills"]
                .as_array()
                .into_iter()
                .flatten()
                .find(|skill| skill["name"] == json!(wanted))
                .cloned()
                .ok_or((NOT_FOUND, format!("没有技能 {wanted}")))?;
            let file = PathBuf::from(skill["file"].as_str().unwrap_or(""));
            let directory = file.parent().map(Path::to_path_buf).unwrap_or_default();
            if method == "tools.deleteSkill" {
                // 只删 EDITH 自己技能目录里的（别的是项目、Claude Code、Friday 的）：挪进 ~/.edith/trash
                let own = canonical(&edith_home().join("skills"));
                if !canonical(&directory).starts_with(&own) {
                    return Err((NOT_ALLOWED, format!("{} 不在 EDITH 的技能目录里，到它自己那边删", directory.display())));
                }
                let trash = edith_home().join("trash");
                let _ = std::fs::create_dir_all(&trash);
                let target = trash.join(format!("{wanted}-{}", chrono::Utc::now().format("%Y%m%d%H%M%S")));
                std::fs::rename(&directory, &target).map_err(|error| (NOT_ALLOWED, format!("挪不动：{error}")))?;
                config_changed(&["tools"]);
                return Ok(json!({}));
            }
            let content = std::fs::read_to_string(&file).map_err(|error| (NOT_FOUND, format!("读不了 {}：{error}", file.display())))?;
            let mut files = Vec::new();
            let mut pending = vec![directory.clone()];
            while let Some(dir) = pending.pop() {
                for entry in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        pending.push(path);
                    } else if path != file && files.len() < 500 {
                        files.push(path.strip_prefix(&directory).unwrap_or(&path).to_string_lossy().replace('\\', "/"));
                    }
                }
            }
            files.sort();
            Ok(json!({
                "name": skill["name"], "declaredName": skill["name"], "description": skill["description"],
                "directory": directory.to_string_lossy(), "file": file.to_string_lossy(), "content": content, "files": files,
            }))
        }
        "mcp.list" => Ok(json!({ "servers": mcp_servers() })),
        "mcp.update" | "mcp.saveServer" | "mcp.saveConfigText" | "mcp.importText" => {
            let mut servers = mcp_servers();
            let mut names = Vec::new();
            let outcome: Result<(), String> = (|| {
                match method {
                    "mcp.update" => {
                        let given = params["servers"].as_object().cloned().ok_or("要给 servers")?;
                        names.extend(given.keys().cloned());
                        servers.extend(given);
                    }
                    "mcp.saveServer" => {
                        let server = params["name"].as_str().map(str::trim).filter(|name| !name.is_empty()).ok_or("服务要有名字")?;
                        let config = params["config"].clone();
                        if !config.is_object() {
                            return Err("配置要是对象".into());
                        }
                        if let Some(old) = params["oldName"].as_str().filter(|old| *old != server) {
                            servers.remove(old);
                        }
                        servers.insert(server.to_string(), config);
                        names.push(server.to_string());
                    }
                    "mcp.saveConfigText" => {
                        let text = params["text"].as_str().unwrap_or("");
                        let value: Value = serde_json::from_str(text).map_err(|error| format!("不是 JSON：{error}"))?;
                        servers = value["mcpServers"].as_object().cloned().ok_or("要有 mcpServers 对象")?;
                    }
                    _ => {
                        let pasted = parse_mcp_text(params["text"].as_str().unwrap_or(""))?;
                        names.extend(pasted.keys().cloned());
                        servers.extend(pasted);
                    }
                }
                write_mcp_servers(servers.clone()).map_err(|(_, message)| message)
            })();
            if method == "mcp.update" {
                outcome.map_err(|message| (-32602, message))?;
                config_changed(&["mcp", "tools"]);
                return Ok(json!({}));
            }
            let listing = tools_listing();
            let mcp = mcp_state(&listing);
            if outcome.is_ok() {
                config_changed(&["mcp", "tools"]);
            }
            let error = outcome.err().unwrap_or_default();
            if method == "mcp.importText" {
                return Ok(json!({ "ok": error.is_empty(), "names": names, "error": error, "mcp": mcp }));
            }
            Ok(json!({ "error": error, "mcp": mcp }))
        }
        "mcp.remove" | "mcp.setDisabled" | "mcp.setAutoApprove" => {
            let server = name()?;
            let mut servers = mcp_servers();
            match method {
                "mcp.remove" => {
                    servers.remove(&server);
                }
                _ => {
                    // 只在项目那层配置的：全局这层写一条只改开关的（EDITH 后面盖前面）
                    let entry = servers.entry(server.clone()).or_insert_with(|| json!({}));
                    if method == "mcp.setDisabled" {
                        entry["disabled"] = json!(params["disabled"].as_bool().unwrap_or(false));
                    } else {
                        entry["autoApprove"] = json!(params["value"].as_bool().unwrap_or(false));
                    }
                }
            }
            write_mcp_servers(servers)?;
            config_changed(&["mcp", "tools"]);
            Ok(json!({}))
        }
        // EDITH 用到时才连 MCP 服务、配置变了下次用时重连
        "mcp.reconnect" | "mcp.reloadAll" | "capabilities.refreshAvailability" => {
            config_changed(&["mcp"]);
            Ok(json!({}))
        }
        other if other.starts_with("capabilities.") => {
            Err(not_allowed("在这里管能力提供方（EDITH 的能力写在 ~/.edith/capabilities.json）"))
        }
        other => Err(not_allowed(&format!("脚本工具（{other}）"))),
    }
}

// MARK: - 记住的事（memory.*：EDITH 的 ~/.edith/memory.json，同 EDITH 的 memory.rs）

fn memory_path() -> PathBuf {
    edith_home().join("memory.json")
}

fn memory_items() -> Vec<Value> {
    read_json_file(&memory_path())["items"].as_array().cloned().unwrap_or_default()
}

fn memory_label(kind: &str) -> &'static str {
    match kind {
        "fact" => "事实",
        "decision" => "决定",
        "correction" => "纠正",
        "experience" => "经验",
        "habit" => "习惯",
        "routine" => "惯例",
        _ => "偏好",
    }
}

/// 那台的本地时间文字（yyyy-MM-dd HH:mm）
fn local_time(text: &str) -> String {
    chrono::DateTime::parse_from_rfc3339(text)
        .map(|time| time.with_timezone(&chrono::Local).format("%Y-%m-%d %H:%M").to_string())
        .unwrap_or_default()
}

/// MemoryState 的一行
fn memory_row(item: &Value) -> Value {
    let field = |key: &str| item[key].as_str().unwrap_or("").to_string();
    let uses = item["uses"].as_u64().unwrap_or(0);
    let mut row = json!({
        "id": field("id"), "shortId": field("id"), "kind": field("kind"), "kindLabel": memory_label(&field("kind")), "text": field("text"),
        "project": field("project"), "pinned": item["pinned"] == true, "auto": field("source").starts_with("auto:"), "uses": uses,
        "judged": 0, "wins": 0, "losses": 0, "rate": if uses > 0 { format!("用了 {uses} 次") } else { String::new() },
        "lastUsed": local_time(&field("last_used_at")), "updatedAt": local_time(&field("updated_at")),
    });
    if field("state") == "archived" {
        row["archivedAt"] = json!(local_time(&field("archived_at")));
        row["archiveReason"] = json!(field("archive_reason"));
    }
    row
}

fn memory_state(recent: Option<usize>) -> Value {
    let items = memory_items();
    let (mut active, mut archived): (Vec<&Value>, Vec<&Value>) = items.iter().partition(|item| item["state"] != "archived");
    let (count, archived_count) = (active.len(), archived.len());
    if let Some(recent) = recent.filter(|recent| *recent > 0) {
        let newest = |list: &mut Vec<&Value>, key: &str| {
            list.sort_by(|a, b| b[key].as_str().cmp(&a[key].as_str()));
            list.truncate(recent);
        };
        newest(&mut active, "updated_at");
        newest(&mut archived, "archived_at");
    }
    let metrics =
        json!({ "episodes": 0, "tasks": 0, "firstPass": 0, "deliveries": 0, "followups": 0, "firstPassText": "", "followupText": "" });
    json!({
        "items": active.into_iter().map(memory_row).collect::<Vec<_>>(),
        "archived": archived.into_iter().map(memory_row).collect::<Vec<_>>(),
        "metrics": { "week": metrics, "month": metrics }, "byVersion": [], "pending": [],
        "count": count, "archivedCount": archived_count, "recent": recent.unwrap_or(0),
    })
}

/// memory.search：同 EDITH 的 search_memory（每个关键词对上算一分，分高的、新的在前，最多 20 条；不分项目）
fn memory_search(query: &str) -> Vec<Value> {
    let words: Vec<String> = query.split_whitespace().map(str::to_lowercase).collect();
    let mut scored: Vec<(usize, Value)> = memory_items()
        .into_iter()
        .filter(|item| item["state"] != "archived")
        .filter_map(|item| {
            let text = item["text"].as_str().unwrap_or("").to_lowercase();
            let score = words.iter().filter(|word| text.contains(word.as_str())).count();
            (score > 0).then_some((score, item))
        })
        .collect();
    scored.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1["updated_at"].as_str().cmp(&a.1["updated_at"].as_str())));
    scored.into_iter().take(20).map(|(_, item)| memory_row(&item)).collect()
}

fn memory_request(method: &str, params: &Value) -> Reply {
    if method == "memory.search" {
        let query = params["query"].as_str().unwrap_or("");
        return Ok(json!({ "query": query, "items": memory_search(query), "pending": [] }));
    }
    let recent = {
        let mut state = STATE.lock().unwrap();
        if method == "memory.get" {
            state.memory = Some(params["recent"].as_u64().unwrap_or(0) as usize);
        }
        state.memory.get_or_insert(0).to_owned()
    };
    if method == "memory.get" {
        return Ok(memory_state(Some(recent)));
    }
    let now = chrono::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false);
    let mut items = memory_items();
    if method == "memory.add" {
        let text = text_param(params, "text")?;
        let kind = params["kind"].as_str().filter(|kind| !kind.is_empty()).unwrap_or("preference");
        let id = loop {
            let id = format!("m{}", &uuid::Uuid::new_v4().simple().to_string()[..4]);
            if !items.iter().any(|item| item["id"] == json!(id)) {
                break id;
            }
        };
        items.push(json!({
            "id": id, "kind": kind, "text": text, "project": "", "pinned": false, "source": "user", "state": "active", "uses": 0,
            "created_at": now, "updated_at": now,
        }));
    } else {
        let id = text_param(params, "id")?;
        let index = items.iter().position(|item| item["id"] == json!(id)).ok_or((NOT_FOUND, format!("没有记忆 {id}")))?;
        let item = &mut items[index];
        match method {
            "memory.setText" => {
                item["text"] = json!(text_param(params, "text")?);
                // 用户改过的不再算自动学的，计数清零
                item["source"] = json!("user");
                item["uses"] = json!(0);
                item["updated_at"] = json!(now);
            }
            "memory.setPinned" => item["pinned"] = json!(params["pinned"].as_bool().unwrap_or(false)),
            "memory.archive" => {
                item["state"] = json!("archived");
                item["archived_at"] = json!(now);
                item["archive_reason"] = json!("手动归档");
            }
            "memory.restore" => {
                item["state"] = json!("active");
                item["updated_at"] = json!(now);
            }
            "memory.remove" => {
                items.remove(index);
            }
            other => return Err((-32601, format!("这一版的 EDITH 网关没有 {other}"))),
        }
    }
    write_json_file(&memory_path(), &json!({ "items": items }))?;
    let state = memory_state(Some(recent));
    event("memory", state.clone());
    Ok(state)
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
        (Some("approve.cancel"), None) => on_approve_cancel(&message["params"]),
        // 终端：接着的有输出（原样转）、开了关了（这个大脑的全部终端）
        (Some("terminal.output"), None) => event("term.output", message["params"].clone()),
        // 浏览器：看着的标签的画面、下载（原样转）、标签变了（这个大脑的全部标签）
        (Some("browser.frame"), None) | (Some("browser.download"), None) => event(method.unwrap_or(""), message["params"].clone()),
        (Some("browser"), None) => {
            {
                let mut state = STATE.lock().unwrap();
                for tab in message["params"]["tabs"].as_array().into_iter().flatten().filter_map(|tab| tab["id"].as_str()) {
                    state.browser_tabs.insert(tab.to_string(), brain.workspace.clone());
                }
                state.browsers.insert(brain.workspace.clone(), message["params"].clone());
            }
            event("browser", browser_state());
        }
        (Some("terminals"), None) => {
            let list = message["params"]["terminals"].as_array().cloned().unwrap_or_default();
            STATE.lock().unwrap().terminals.insert(brain.workspace.clone(), list);
            event("terminals", terminals_state());
        }
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
        state.terminals.remove(&brain.workspace);
        state.browsers.remove(&brain.workspace);
        let workspace = brain.workspace.clone();
        state.browser_tabs.retain(|_, owner| *owner != workspace);
        state.live.keys().cloned().collect()
    };
    event("terminals", terminals_state());
    event("browser", browser_state());
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
    // 排着的（别的客户端排的、连上之前排的）
    {
        let mut state = STATE.lock().unwrap();
        for session in listed.as_array().into_iter().flatten() {
            let id = session["id"].as_str().unwrap_or("").to_string();
            for entry in session["queue"].as_array().into_iter().flatten() {
                let turn = entry["turn"].as_str().unwrap_or("").to_string();
                if turn.is_empty() || state.queued_turns.values().any(|known| *known == turn) {
                    continue;
                }
                let item_id = uuid::Uuid::new_v4().to_string().to_uppercase();
                let item = json!({ "id": item_id, "text": entry["text"], "attachments": [], "createdAt": iso(SystemTime::now()) });
                state.queued.entry(id.clone()).or_default().push(item);
                state.queued_turns.insert(item_id, turn);
                state.queue_revision += 1;
            }
        }
    }
    // 它的浏览器标签（旧版的 EDITH 没有）
    if let Ok(listed) = brain.request("browser.state", json!({}), Duration::from_secs(10)) {
        let mut state = STATE.lock().unwrap();
        for tab in listed["tabs"].as_array().into_iter().flatten().filter_map(|tab| tab["id"].as_str()) {
            state.browser_tabs.insert(tab.to_string(), brain.workspace.clone());
        }
        state.browsers.insert(brain.workspace.clone(), listed);
    }
    // 它开着的终端（旧版的 EDITH 没有）
    if let Ok(listed) = brain.request("terminals.list", json!({}), Duration::from_secs(10)) {
        let list = listed["terminals"].as_array().cloned().unwrap_or_default();
        STATE.lock().unwrap().terminals.insert(brain.workspace.clone(), list);
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
        // 选目录时新建文件夹；runner.submit 认 whenBusy；会话的动静（客户端的助手看这台）、助手开没开
        "features": ["mkdir", "submitFrom", "activity", "assistant", "config", "memory", "memorySearch"],
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
            "secrets": secret_names(),
            "directories": directories_state(),
            "terminals": terminals_state_of(&state),
            "browser": browser_state_of(&state),
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

/// 交给大脑的这段话按什么来：界面上选的权限模式、思考档位、模型（EDITH 照着审批、发请求），附件里的图片（模型能看图时一起给它）
fn with_prefs(request: &mut Value, session: &str, attachments: &[&str]) {
    request["mode"] = json!(permission_mode(session));
    if let Some(thinking) = META.setting(session, "thinking") {
        request["thinking"] = json!(thinking);
    }
    let engine = engine_for(session);
    if engine["id"] != ENGINE_ID {
        request["engine"] = engine["id"].clone();
    }
    let images: Vec<&str> = attachments
        .iter()
        .copied()
        .filter(|path| {
            let lower = path.to_lowercase();
            [".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp"].iter().any(|extension| lower.ends_with(extension))
        })
        .collect();
    if !images.is_empty() {
        request["images"] = json!(images);
    }
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
    // 改了一条重发：这段先整个存成另一支，截到那条之前
    if let Some(replacing) = params["replacing"].as_str().filter(|id| !id.is_empty()) {
        branch_off(&brain, &session, replacing)?;
    }
    // 忙的时候：默认同输入框（补充进这一轮），whenBusy: queue 排到下一轮
    let queue = params["whenBusy"].as_str() == Some("queue");
    let urgency = if queue { "queue" } else { "interject" };
    let mut request = json!({ "session": session.id, "text": text, "urgency": urgency, "kind": "user" });
    with_prefs(&mut request, &session.id, &attachments);
    let reply = brain.request("sense", request, Duration::from_secs(30)).map_err(|error| (NOT_ALLOWED, error))?;
    META.set_draft(&session.id, "");
    if reply["disposition"] == "queued" {
        let item_id = uuid::Uuid::new_v4().to_string().to_uppercase();
        let item = json!({
            "id": item_id,
            "text": text,
            "attachments": attachments,
            "createdAt": iso(SystemTime::now()),
        });
        let mut state = STATE.lock().unwrap();
        state.queued.entry(session.id.clone()).or_default().push(item);
        state.queued_turns.insert(item_id, reply["turn"].as_str().unwrap_or("").to_string());
        state.queue_revision += 1;
    }
    changed(&session.id, true);
    Ok(json!({}))
}

fn busy(session: &Stored) -> Result<(), (i64, String)> {
    if STATE.lock().unwrap().generating.contains(&session.id) {
        return Err((NOT_ALLOWED, "这段会话在干活：先停下".into()));
    }
    Ok(())
}

fn request_on(brain: &Brain, method: &str, params: Value) -> Reply {
    brain.request(method, params, Duration::from_secs(30)).map_err(|error| (NOT_ALLOWED, error))
}

/// 改一条重发（runner.submit 带 replacing）：这段整个存成藏起来的另一支，主干截到那条之前（同 Friday 的 Conversation::fork：
/// 新的那条和原来那条同一个上级，切分支的箭头能切回去）
fn branch_off(brain: &Brain, session: &Stored, replacing: &str) -> Result<(), (i64, String)> {
    busy(session)?;
    let nodes = group(&session.id, &session.history, &Team::new());
    let index = (0..nodes.len())
        .find(|index| message_uuid(&session.id, *index) == upper(replacing))
        .ok_or((NOT_FOUND, "没有这条消息".to_string()))?;
    let keep = if index == 0 { 0 } else { nodes[index - 1].end };
    let archive = request_on(brain, "sessions.fork", json!({ "id": session.id, "hidden": true }))?;
    let archive = archive["id"].as_str().unwrap_or("").to_string();
    request_on(brain, "sessions.truncate", json!({ "id": session.id, "keep": keep }))?;
    let (main, mut paths) = META.branches(&session.id);
    let next = paths.iter().map(|(_, rank)| *rank).chain([main]).max().unwrap_or(0) + 1;
    paths.push((archive.clone(), main));
    META.set_branches(&session.id, next, &paths);
    META.move_with_branch(&session.id, &archive, None);
    Ok(())
}

/// 切到另一支（runner.selectBranch）：主干整个存成藏起来的一支，选中的那支换上来当主干（EDITH 接着它往下做）
fn select_branch(session: &Stored, child: &str) -> Reply {
    let (main, paths) = META.branches(&session.id);
    let nodes = group(&session.id, &session.history, &Team::new());
    if (0..nodes.len()).any(|index| message_uuid(&session.id, index) == child) || paths.is_empty() {
        return Ok(json!({}));
    }
    let hashes = chain(&session.history, &nodes);
    let picked = branch_paths(session, &paths, &hashes, &Team::new())
        .into_iter()
        .find(|path| path.ids.iter().any(|id| id == child))
        .ok_or((NOT_FOUND, "没有这一支".to_string()))?;
    busy(session)?;
    let brain = brain_for(&session.cwd).map_err(|error| (NOT_ALLOWED, error))?;
    let archive = request_on(&brain, "sessions.fork", json!({ "id": session.id, "hidden": true }))?;
    let archive = archive["id"].as_str().unwrap_or("").to_string();
    request_on(&brain, "sessions.copy", json!({ "id": session.id, "from": picked.archive.id }))?;
    let _ = request_on(&brain, "sessions.remove", json!({ "id": picked.archive.id }));
    let paths: Vec<(String, u64)> =
        paths.into_iter().map(|(id, rank)| if id == picked.archive.id { (archive.clone(), main) } else { (id, rank) }).collect();
    META.set_branches(&session.id, picked.rank, &paths);
    META.move_with_branch(&session.id, &archive, Some(&picked.archive.id));
    changed(&session.id, true);
    Ok(json!({}))
}

/// 重试（runner.retry，最后一条回复出错了才能）：从这一轮用户说的那句起重做——截到它之前、再发一遍
/// （消息 id 和原来的一样，出错的那条换成新的回复）
fn retry(session: &Stored) -> Reply {
    busy(session)?;
    failed_message(session).ok_or((NOT_ALLOWED, "最后一条回复没出错".to_string()))?;
    let index = session
        .history
        .iter()
        .rposition(|message| {
            let text = user_text(message);
            message["role"] == "user" && !is_framework(&text) && report_from(&text).is_none() && !is_interjection(&text)
        })
        .ok_or((NOT_ALLOWED, "找不到这一轮说的话".to_string()))?;
    let text = shown_user_text(&user_text(&session.history[index]));
    let brain = brain_for(&session.cwd).map_err(|error| (NOT_ALLOWED, error))?;
    request_on(&brain, "sessions.truncate", json!({ "id": session.id, "keep": index }))?;
    META.set_ended(&session.id, 0, "", false);
    // 那句附的图片（存档里记成 file://路径）照样带上
    let images: Vec<String> = session.history[index]["content"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|part| part["image_url"]["url"].as_str()?.strip_prefix("file://").map(str::to_string))
        .collect();
    let mut request = json!({ "session": session.id, "text": text, "urgency": "interject", "kind": "user" });
    with_prefs(&mut request, &session.id, &images.iter().map(String::as_str).collect::<Vec<_>>());
    request_on(&brain, "sense", request)?;
    changed(&session.id, true);
    Ok(json!({}))
}

/// 从某条消息分叉成一段新会话（conversation.fork）：那条和它之前的抄过去，标题加「（fork）」（同 Friday 的 ConversationStore::fork）
fn fork(params: &Value) -> Reply {
    let session = session_param(params, "id")?;
    let message = upper(&text_param(params, "messageID")?);
    if session.blank() {
        return Err((NOT_FOUND, "没有这条消息".into()));
    }
    let nodes = group(&session.id, &session.history, &Team::new());
    let mut source =
        (0..nodes.len()).find(|index| message_uuid(&session.id, *index) == message).map(|index| (session.id.clone(), nodes[index].end));
    // 在另一支上的消息：从那一支抄
    if source.is_none() {
        let (_, paths) = META.branches(&session.id);
        let hashes = chain(&session.history, &nodes);
        source = branch_paths(&session, &paths, &hashes, &Team::new()).into_iter().find_map(|path| {
            let index = path.ids.iter().position(|id| *id == message)?;
            Some((path.archive.id.clone(), path.nodes[index].end))
        });
    }
    let (from, keep) = source.ok_or((NOT_FOUND, "没有这条消息（正在写的这一轮做完再分叉）".to_string()))?;
    let brain = brain_for(&session.cwd).map_err(|error| (NOT_ALLOWED, error))?;
    let mut request = json!({ "id": from, "keep": keep });
    if let Some(new) = params["newID"].as_str().filter(|id| !id.is_empty()) {
        request["newID"] = json!(lower(new));
    }
    let created = request_on(&brain, "sessions.fork", request)?;
    let id = created["id"].as_str().unwrap_or("").to_string();
    let title = format!("{}（fork）", if session.title.is_empty() { "新对话" } else { &session.title });
    if let Some(path) = stored_session(&id).and_then(|stored| stored.path) {
        append_title(&path, &title);
    }
    for key in ["permissionMode", "thinking", "teamMode"] {
        if let Some(value) = META.setting(&session.id, key) {
            META.set_setting(&id, key, &value);
        }
    }
    changed(&id, true);
    Ok(json!({ "id": upper(&id) }))
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
            for key in ["permissionMode", "thinking", "engineID", "teamMode"] {
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
                // 交给大脑删（停下、存档、改动基线、子智能体的运行记录一起，分出去的各支也删）；旧版的 EDITH 没有 sessions.remove：停下后这里删
                let brain = brain_for(&session.cwd).ok();
                for (archive, _) in META.branches(&session.id).1 {
                    if let Some(brain) = &brain {
                        let _ = brain.request("sessions.remove", json!({ "id": archive }), Duration::from_secs(30));
                    }
                }
                let removed = brain
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
                let outcome = if approved { "approved" } else { "denied" };
                approvals_changed(&mut state, &pending.session, vec![json!({ "eventID": event_id, "outcome": outcome })]);
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
        // 记在这段会话上，下一句交给大脑时带上（权限模式、思考档位、模型；任务模式只记下）
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
        "conversation.fork" => fork(params),
        // 客户端的助手看这台的会话：快照 + 之后的 activity 事件；这台自己没有助手
        "activity.subscribe" => {
            let mut state = STATE.lock().unwrap();
            state.activity = true;
            Ok(activity_snapshot(&state))
        }
        "activity.unsubscribe" => {
            STATE.lock().unwrap().activity = false;
            Ok(json!({}))
        }
        "assistant.status" => Ok(json!({ "enabled": false, "reacting": false })),
        // 这台没有助手（EDITH 是大脑、助手在客户端那台上）：助手页看这台时说没开，不说版本太旧
        "assistant.subscribe" => Ok(json!({
            "state": { "enabled": false, "reacting": false, "config": { "enabled": false }, "engines": [] },
            "board": { "machines": [] }, "entries": [], "channels": [],
        })),
        // 客户端前台在看哪段（presence）只是告诉一声
        "assistant.unsubscribe" | "assistant.presence" => Ok(json!({})),
        "assistant.history" => Ok(json!({ "entries": [], "hasOlder": false })),
        "assistant.channels.list" => Ok(json!({ "channels": [] })),
        method if method.starts_with("assistant.") => Err((NOT_ALLOWED, "这台的 EDITH 没有助手：助手开在客户端那台上".into())),
        method if method.starts_with("term.") => terminal_request(method, params),
        method if method.starts_with("browser.") => browser_request(method, params),
        method
            if method.starts_with("config.")
                || method.starts_with("settings.")
                || method.starts_with("tools.")
                || method.starts_with("mcp.")
                || method.starts_with("capabilities.") =>
        {
            settings_request(method, params)
        }
        method if method.starts_with("memory.") => memory_request(method, params),
        "runner.retry" => retry(&session_param(params, "conversationID")?),
        "runner.selectBranch" => select_branch(&session_param(params, "conversationID")?, &upper(&text_param(params, "child")?)),
        // Friday 丢掉缓存的模型会话、下一轮按存档重建：EDITH 每轮本来就按存档来
        "runner.resetSession" => Ok(json!({})),
        "runner.removeQueued" => {
            let session = session_param(params, "conversationID")?;
            let item = upper(&text_param(params, "itemID")?);
            let turn = STATE.lock().unwrap().queued_turns.get(&item).cloned().ok_or((NOT_FOUND, "没有这句排着的".to_string()))?;
            let brain = connected_brain(&session).ok_or((NOT_FOUND, "这段会话没在干活".to_string()))?;
            brain
                .request("sessions.dequeue", json!({ "id": session.id, "turn": turn }), Duration::from_secs(10))
                .map_err(|error| (NOT_ALLOWED, error))?;
            {
                let mut state = STATE.lock().unwrap();
                state.queued_turns.remove(&item);
                if let Some(queue) = state.queued.get_mut(&session.id) {
                    queue.retain(|entry| entry["id"] != json!(item));
                }
                state.queue_revision += 1;
            }
            changed(&session.id, false);
            Ok(json!({}))
        }
        // EDITH 不会暂停队列（Friday 出错时暂停），继续就是照常
        "runner.resumeQueue" => Ok(json!({})),
        "agent.removeQueued" => {
            let card = upper(&text_param(params, "agentID")?);
            let item = upper(&text_param(params, "itemID")?);
            let found = STATE.lock().unwrap().agents.values().find(|agent| agent_card_uuid(agent.session(), agent.id()) == card).and_then(
                |agent| {
                    let inbox = agent.meta["inbox"]
                        .as_array()?
                        .iter()
                        .find(|entry| agent_inbox_uuid(agent.session(), agent.id(), entry["id"].as_str().unwrap_or("")) == item)?;
                    Some((agent.id().to_string(), agent.session().to_string(), inbox["id"].as_str().unwrap_or("").to_string()))
                },
            );
            let (agent, session, inbox) = found.ok_or((NOT_FOUND, "没有这句排着的".to_string()))?;
            let stored = stored_session(&session).ok_or((NOT_FOUND, "没有这段会话".to_string()))?;
            let brain = brain_for(&stored.cwd).map_err(|error| (NOT_ALLOWED, error))?;
            brain
                .request("agents.dequeue", json!({ "id": agent, "item": inbox }), Duration::from_secs(10))
                .map_err(|error| (NOT_ALLOWED, error))?;
            Ok(json!({}))
        }
        "runner.cancelQuotaWait" => Err(not_allowed("这个操作")),
        "secrets.set" | "secrets.remove" | "engines.update" | "engines.remove" | "engines.setSubAgentAllowed" => {
            engines_request(method, params)
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
