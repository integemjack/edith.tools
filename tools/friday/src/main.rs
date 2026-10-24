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
//! - **常驻的那份**（`edith-friday serve`，整台机器一个，像 edith-connect）：Friday 专有、要一直在的——终端、浏览器、智能体的浏览器工具、
//!   本地能力（Friday.Powers）和本地模型、定时任务、脚本工具、钩子、插件、能力提供方……
//!   它连着这台每个大脑（在那里是能力「friday」，网关是「friday-client」），大脑只给它开模型地址和钩子两个口子；网关第一次要用时拉起它，
//!   终端、浏览器的请求转给它。见「常驻的那份」一节。
//!
//! 能做：连上、看会话、新建 / 删 / 改名 / 置顶 / 草稿 / 分叉、选工作目录、发话（忙时补充或排队）、附件、流式看回复、上下文进度环、批准、停下、
//! 重试、改一条重发与切分支、子智能体、会话改动、会话上选的权限模式 / 思考档位 / 模型、模型和密钥、设置页、记忆、终端、浏览器、
//! 智能体的浏览器工具、会话的动静。
//! 别的方法回 -32601（没有）或错误 4（这一版的 EDITH 还不支持）；server.features 只报做了的，客户端就不去用别的。

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
    // 常驻那份的浏览器工具、定时任务、扩展自己的能力在大脑里叫 friday__*
    let name = match name.split_once("__") {
        Some((owner, action)) if owner.starts_with("friday") => action,
        _ => name,
    };
    Some(match name {
        "loop" => ("定时任务", "clock.arrow.circlepath"),
        "create_tool" => ("创建工具", "hammer"),
        "update_tool" => ("修改工具", "hammer"),
        "delete_tool" => ("删除工具", "trash"),
        "list_tools" => ("列出工具", "list.bullet"),
        "read_tool" => ("查看工具", "doc.text.magnifyingglass"),
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
        "browser_navigate" => ("打开网页", "globe"),
        "browser_read" => ("读网页", "globe"),
        "browser_act" => ("操作网页", "cursorarrow.click"),
        "browser_screenshot" => ("网页截图", "camera.viewfinder"),
        "browser_resize" => ("调整网页视口", "arrow.up.left.and.arrow.down.right"),
        "browser_tabs" => ("浏览器标签", "globe"),
        "browser_hand_over" => ("请你在浏览器里操作", "hand.raised"),
        "browser_run_js" => ("在网页里运行脚本", "curlybraces"),
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

/// 「/loop …」问答记进会话时，命令和回复之间的分隔（网关拆成一问一答）
const LOOP_REPLY: &str = "\n\n［/loop 的回复］\n";

/// 定时任务到点发来的那条（提示词 loop.run* 开头的「（定时任务 #n…」）：第几个
fn loop_number(text: &str) -> Option<u64> {
    let rest = text.strip_prefix("（定时任务 #")?;
    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
    digits.parse().ok()
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
    /// 定时任务到点发来的：「定时任务 #n」（Friday 消息上的 sentBy）
    sent_by: Option<String>,
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
                let shown = shown_user_text(&text);
                // 输入框里打的「/loop …」：记成一问一答（问是命令，答是回复）
                if let Some((command, reply)) = shown.split_once(LOOP_REPLY) {
                    nodes.push(Node {
                        role: "user",
                        blocks: vec![text_block(session, &format!("{index}"), "text", command)],
                        ..Default::default()
                    });
                    nodes.last_mut().unwrap().end = index + 1;
                    nodes.push(Node {
                        role: "assistant",
                        blocks: vec![text_block(session, &format!("{index}.loop"), "text", reply)],
                        ..Default::default()
                    });
                    nodes.last_mut().unwrap().end = index + 1;
                    continue;
                }
                let blocks = vec![text_block(session, &format!("{index}"), "text", &shown)];
                let (from_agent, interjected) = (report_from(&text), is_interjection(&text));
                let sent_by = loop_number(&shown).map(|number| format!("定时任务 #{number}"));
                nodes.push(Node { role: "user", blocks, from_agent, interjected, sent_by, ..Default::default() });
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
    if let Some(sent_by) = node.sent_by {
        message["sentBy"] = json!(sent_by);
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
    /// 开着设置 › 能力（powers.subscribe）、订了本地模型（models.subscribe，客户端连上就订）
    powers: bool,
    models: bool,
    /// 调过 loops.*（设置 › 定时任务）
    loops: bool,
    /// 常驻那份开着的终端（TerminalInfo 们）、浏览器（BrowserState；还没连上它时是 None）
    terminals: Vec<Value>,
    browser: Option<Value>,
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

/// TerminalsState：常驻那份的终端（会话 id 换成 Friday 的大写）
fn terminals_state_of(state: &State) -> Value {
    let mut all: Vec<Value> = state
        .terminals
        .iter()
        .map(|terminal| {
            let mut terminal = terminal.clone();
            terminal["conversationID"] = json!(upper(terminal["conversationID"].as_str().unwrap_or("")));
            terminal
        })
        .collect();
    all.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
    json!({ "terminals": all, "available": serve_mode() != "off" })
}

fn terminals_state() -> Value {
    terminals_state_of(&STATE.lock().unwrap())
}

fn terminal_request(method: &str, params: &Value) -> Reply {
    let id = text_param(params, "id")?;
    let hub = hub().map_err(|error| (NOT_ALLOWED, error))?;
    let forward = |target: &str, request: Value| hub.request(target, request, Duration::from_secs(30)).map_err(|error| (NOT_FOUND, error));
    if method == "term.open" {
        // 在这段会话的工作目录开（没发过话的空白会话也行：它记着工作目录）
        let session = session_param(params, "conversationID")?;
        let directory = params["workingDirectory"]
            .as_str()
            .filter(|dir| !dir.is_empty())
            .map(str::to_string)
            .or_else(|| (!session.cwd.is_empty()).then(|| session.cwd.clone()))
            .unwrap_or_else(default_directory);
        let mut request = json!({ "id": id, "session": session.id, "cwd": directory, "cols": params["cols"], "rows": params["rows"] });
        if let Some(command) = params["command"].as_str() {
            request["command"] = json!(command);
        }
        let info = forward("terminals.open", request)?;
        {
            let mut state = STATE.lock().unwrap();
            state.terminals.retain(|terminal| terminal["id"] != json!(id));
            state.terminals.push(info);
        }
        event("terminals", terminals_state());
        return Ok(json!({}));
    }
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
    let reply = forward(target, request)?;
    Ok(if method == "term.attach" { reply } else { json!({}) })
}

// MARK: - 浏览器（browser.*：常驻那份起的无头 Chrome，标签断了照常在）

/// BrowserState：常驻那份的标签（会话 id 换成 Friday 的大写）；还没连上它时先说能用（开的时候才知道）
fn browser_state_of(state: &State) -> Value {
    let mut value = state.browser.clone().unwrap_or_else(|| json!({ "available": serve_mode() != "off", "tabs": [] }));
    for tab in value["tabs"].as_array_mut().into_iter().flatten() {
        tab["conversationID"] = json!(upper(tab["conversationID"].as_str().unwrap_or("")));
    }
    value
}

fn browser_state() -> Value {
    browser_state_of(&STATE.lock().unwrap())
}

fn browser_request(method: &str, params: &Value) -> Reply {
    let hub = hub().map_err(|error| (NOT_ALLOWED, error))?;
    if method == "browser.open" {
        let session = session_param(params, "conversationID")?;
        let request = json!({ "session": session.id, "url": params["url"].as_str().unwrap_or("") });
        return hub.request("browser.open", request, Duration::from_secs(60)).map_err(|error| (NOT_ALLOWED, error));
    }
    let reply = match hub.request(method, params.clone(), Duration::from_secs(30)) {
        Ok(reply) => reply,
        // 找不到标签的不理（多半刚关了，同 Friday）
        Err(error) if error.starts_with("没有标签") => return Ok(json!({})),
        Err(error) => return Err((NOT_ALLOWED, error)),
    };
    // 用户在卡片上点了「好了」：替用户往这段会话说一句（照这段会话选的权限模式、模型），智能体接着做
    if method == "browser.handOverDone"
        && let (Some(session), Some(text)) = (reply["session"].as_str(), reply["text"].as_str())
    {
        submit(&json!({ "conversationID": upper(session), "text": text, "whenBusy": "queue" }))?;
    }
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
    // 常驻那份的脚本工具（自己一组，不在 descriptors 里重复列）
    let scripts = hub_scripts();
    let script_names: Vec<String> = scripts["scriptTools"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|tool| tool["name"].as_str())
        .map(|name| format!("friday__{name}"))
        .collect();
    let descriptors: Vec<Value> = listing["tools"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|tool| !tool["name"].as_str().is_some_and(|name| script_names.iter().any(|script| script == name)))
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
            // 常驻那份的浏览器工具、定时任务算内置的，扩展自己的能力算 meta（同 Friday）
            let origin = match name.split_once("__") {
                Some((owner, action)) if group == "capability" && owner.starts_with("friday") => {
                    if ["create_tool", "update_tool", "delete_tool", "list_tools", "read_tool"].contains(&action) {
                        "meta"
                    } else {
                        "builtin"
                    }
                }
                _ if group == "capability" => "capability",
                _ => group,
            };
            let mut descriptor = json!({
                "name": name, "title": title, "description": tool["description"], "symbol": symbol, "permission": permission,
                "origin": origin, "enabled": !disabled.contains(&name),
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
        "descriptors": descriptors, "scriptTools": scripts["scriptTools"], "toolsDirectory": scripts["toolsDirectory"], "skills": skills,
        "skillsDirectory": listing["skillsDirectory"].as_str().unwrap_or(""),
    })
}

/// 常驻那份的脚本工具（设置 › 工具）
fn hub_scripts() -> Value {
    if serve_mode() == "off" {
        return json!({ "scriptTools": [], "toolsDirectory": "" });
    }
    hub()
        .and_then(|hub| hub.request("scripts.state", json!({}), Duration::from_secs(10)))
        .unwrap_or_else(|_| json!({ "scriptTools": [], "toolsDirectory": "" }))
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

/// CapabilitiesState（设置 › 工具 › 能力提供方）：常驻那份的本机提供方
fn capabilities_state() -> Value {
    let empty = json!({ "entries": [], "packages": [], "problems": [], "jobs": [], "availability": {}, "optionSpecs": {}, "packageFor": {}, "providersDirectory": "" });
    if serve_mode() == "off" {
        return empty;
    }
    hub().and_then(|hub| hub.request("capabilities.state", json!({}), Duration::from_secs(10))).unwrap_or(empty)
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
        // 脚本工具（常驻那份的）：存、自测、回滚、建、删
        "tools.scriptVersions"
        | "tools.saveScript"
        | "tools.testScript"
        | "tools.rollback"
        | "tools.createScript"
        | "tools.deleteScript" => {
            let hub = hub().map_err(|error| (NOT_ALLOWED, error))?;
            let reply = hub.request(method, params.clone(), Duration::from_secs(660)).map_err(|error| hub_error(&error))?;
            if method != "tools.scriptVersions" {
                config_changed(&["tools"]);
            }
            Ok(reply)
        }
        "tools.setEnabled" => {
            let tool = name()?;
            // 脚本工具的开关记在它的清单里
            if serve_mode() != "off"
                && let Ok(hub) = hub()
                && hub.request("tools.setEnabled", params.clone(), Duration::from_secs(10)).is_ok_and(|reply| reply["handled"] == true)
            {
                config_changed(&["tools"]);
                return Ok(json!({}));
            }
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
        "mcp.reconnect" | "mcp.reloadAll" => {
            config_changed(&["mcp"]);
            Ok(json!({}))
        }
        // 能力提供方：常驻那份的本机提供方
        other if other.starts_with("capabilities.") => {
            let hub = hub().map_err(|error| (NOT_ALLOWED, error))?;
            let reply = hub.request(other, params.clone(), Duration::from_secs(60)).map_err(|error| hub_error(&error))?;
            config_changed(&["capabilities"]);
            Ok(reply)
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
                "name": "friday-client",
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
    // 常驻的那份也连上它（浏览器工具、钩子……第一轮就要用）
    hub_attach(entry);
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
        "features": features(),
    })
}

/// 做了的（客户端只用报了的）；本地能力、本地模型、用户音色在常驻的那份里
fn features() -> Vec<&'static str> {
    let mut list = vec!["mkdir", "submitFrom", "activity", "assistant", "config", "memory", "memorySearch"];
    if serve_mode() != "off" {
        list.extend(["powers", "models", "userVoices", "loops", "hooks", "plugins"]);
    }
    list
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
    // 先连上常驻的那份（终端、浏览器在它那里）和在跑的大脑：在干活的会话、在等的批准马上就有
    if serve_mode() != "off"
        && let Err(error) = hub()
    {
        log(&error);
    }
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
    // 「/loop …」不交给模型：常驻的那份建、列、停，问答记进会话（在干活时等这一轮做完再记）
    if loops::is_command(&text) && serve_mode() != "off" {
        let hub = hub().map_err(|error| (NOT_ALLOWED, error))?;
        let reply = hub
            .request("loops.command", json!({ "conversationID": session.id, "text": text, "by": "user" }), Duration::from_secs(30))
            .map_err(|error| (NOT_ALLOWED, error))?;
        let (brain, session) = materialize(&session)?;
        // 第一句就是 /loop：标题用这条命令（不然 EDITH 按记下的问答起标题）
        if session.title.is_empty()
            && let Some(path) = &session.path
        {
            append_title(path, &text.replace('\n', " ").chars().take(40).collect::<String>());
        }
        let note = format!("{text}{LOOP_REPLY}{}", reply["reply"].as_str().unwrap_or(""));
        brain
            .request("sense", json!({ "session": session.id, "text": note, "urgency": "note", "kind": "user" }), Duration::from_secs(30))
            .map_err(|error| (NOT_ALLOWED, error))?;
        META.set_draft(&session.id, "");
        changed(&session.id, true);
        // 问答记好了再跑第一次（间隔、自己定节奏的现在就跑）
        let _ = hub.request("loops.tick", json!({}), Duration::from_secs(10));
        return Ok(json!({}));
    }
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
            // 它的定时任务一起删
            if let Some(hub) = HUB.lock().unwrap().clone() {
                let _ = hub.request("loops.forget", json!({ "conversationID": session.id }), Duration::from_secs(10));
            }
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
        method if ["powers.", "models.", "loops.", "hooks.", "plugins."].iter().any(|prefix| method.starts_with(prefix)) => {
            hub_forward(method, params)
        }
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

// MARK: - 常驻的那份（edith-friday serve：整台机器一个，像 edith-connect）
//
// Friday 专有、要一直在的都在这里：内置终端、浏览器（连同智能体的浏览器工具），之后的本地模型、定时任务、钩子、插件……
// 它连着这台的每个大脑（`~/.edith/instances/` 里登记的，主令牌），在那里是能力「friday」：浏览器工具是它的动作，
// 用 PreToolUse 钩子告诉大脑这些动作怎么批准（EDITH 的能力协议 §6.7），一轮结束（done）关掉智能体开的标签；
// 报 resident，不算有人连着（大脑闲够了照样退）。网关（一个客户端一条）连它的控制口（`~/.edith/friday/serve.json`：地址、令牌），
// 终端、浏览器的请求转给它，它推来的输出、画面、标签变化转给客户端。没有网关连着、没在跑的终端、没在干活的大脑，闲半小时自己退。

/// 常驻那份发 HTTP 用的（下载本地能力、模型；走环境变量里的代理）
static HTTP: LazyLock<ureq::Agent> = LazyLock::new(|| {
    ureq::Agent::config_builder()
        .http_status_as_error(false)
        .timeout_connect(Some(Duration::from_secs(30)))
        .timeout_recv_response(Some(Duration::from_secs(180)))
        .build()
        .into()
});

fn http_agent() -> &'static ureq::Agent {
    &HTTP
}

/// 常驻那份自己的设置（~/.edith/friday/settings.json：下载源、本地生图 / 生视频用的模型、信任的项目钩子）：读、改、写回一把锁
fn update_serve_settings(change: impl FnOnce(&mut Map<String, Value>)) {
    static LOCK: Mutex<()> = Mutex::new(());
    let _guard = LOCK.lock().unwrap();
    let path = serve_dir().join("settings.json");
    let mut settings = read_json_file(&path).as_object().cloned().unwrap_or_default();
    change(&mut settings);
    let _ = write_json_file(&path, &Value::Object(settings));
}

/// 文件夹里的子文件夹名（按名字排）
fn sorted_names(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|entry| entry.path().is_dir())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

fn serve_dir() -> PathBuf {
    edith_home().join("friday")
}

fn serve_registry() -> PathBuf {
    serve_dir().join("serve.json")
}

fn serve_log(source: &str, text: &str) {
    let line = format!("{} [{source}] {text}\n", chrono::Local::now().format("%m-%d %H:%M:%S"));
    if let Ok(mut file) = std::fs::File::options().create(true).append(true).open(serve_dir().join("serve.log")) {
        let _ = file.write_all(line.as_bytes());
    }
}

/// 常驻的那份怎么用：on（默认）；off 不用（网关不起它，终端、浏览器用不了）；nobrains 不连大脑（测试用）
fn serve_mode() -> String {
    std::env::var("EDITH_FRIDAY_HUB").unwrap_or_default()
}

/// 连着常驻那份的网关
struct ServeClient {
    writer: Mutex<TcpStream>,
}

impl ServeClient {
    fn write(&self, message: &Value) -> bool {
        let mut writer = self.writer.lock().unwrap();
        writeln!(writer, "{message}").and_then(|_| writer.flush()).is_ok()
    }
}

static CLIENTS: LazyLock<Mutex<HashMap<String, Arc<ServeClient>>>> = LazyLock::new(|| Mutex::new(HashMap::new()));
static NEXT_CLIENT: AtomicU64 = AtomicU64::new(1);
/// 连着的大脑（工作目录 → 连接）、正在连的
static SERVED: LazyLock<Mutex<HashMap<String, Arc<Brain>>>> = LazyLock::new(|| Mutex::new(HashMap::new()));
static SERVING: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(|| Mutex::new(HashSet::new()));
/// 在干活的（工作目录, 会话）：闲着不退
static TURNS: LazyLock<Mutex<HashSet<(String, String)>>> = LazyLock::new(|| Mutex::new(HashSet::new()));

/// 推一条通知给这几个网关（终端的输出给开了、接着它的）
fn notify_clients(names: &[String], method: &str, params: Value) {
    let message = json!({ "jsonrpc": "2.0", "method": method, "params": params });
    let clients: Vec<Arc<ServeClient>> =
        CLIENTS.lock().unwrap().iter().filter(|(name, _)| names.contains(name)).map(|(_, client)| client.clone()).collect();
    for client in clients {
        client.write(&message);
    }
}

/// 推一条通知给全部网关（终端开了关了、标签变了）
fn notify_all_clients(method: &str, params: Value) {
    let message = json!({ "jsonrpc": "2.0", "method": method, "params": params });
    let clients: Vec<Arc<ServeClient>> = CLIENTS.lock().unwrap().values().cloned().collect();
    for client in clients {
        client.write(&message);
    }
}

/// 只读的浏览器工具（只读权限的一轮也能用）
const READ_ONLY_BROWSER: [&str; 4] = ["browser_read", "browser_screenshot", "browser_tabs", "browser_hand_over"];

/// 常驻那份在大脑里的动作：浏览器工具、定时任务、扩展自己的能力、启用了的脚本工具
fn serve_actions() -> Vec<Value> {
    let mut actions: Vec<Value> = browser_tools::definitions()
        .into_iter()
        .map(|definition| {
            let name = definition["name"].as_str().unwrap_or("").to_string();
            let function = &definition["spec"]["function"];
            json!({
                "name": name,
                "description": function["description"],
                "parameters": function["parameters"],
                "readOnly": READ_ONLY_BROWSER.contains(&name.as_str()),
                "timeoutSeconds": 180,
            })
        })
        .collect();
    // 定时任务（只给主智能体：子 Agent 调了退回）
    actions.push(json!({
        "name": "loop",
        "description": loops::TOOL_DESCRIPTION,
        "parameters": { "type": "object", "properties": { "command": { "type": "string", "description": loops::TOOL_COMMAND } }, "required": ["command"] },
        "timeoutSeconds": 30,
    }));
    actions.extend(scripts::meta_actions());
    actions.extend(caps::actions());
    actions.extend(scripts::actions());
    actions
}

/// 动作变了（脚本工具加了、改了、开关了）：连着的大脑都换一份
fn serve_update_actions() {
    let message = json!({ "jsonrpc": "2.0", "method": "capability.update", "params": { "actions": serve_actions() } });
    let brains: Vec<Arc<Brain>> = SERVED.lock().unwrap().values().cloned().collect();
    for brain in brains {
        brain.write(&message);
    }
}

/// 在大脑那里报的：动作（serve_actions）；钩子管 PreToolUse（替浏览器动作说怎么批准）；本地模型的地址
fn serve_initialize() -> Value {
    let actions = serve_actions();
    json!({
        "protocolVersion": "1",
        "name": "friday",
        "capability": {
            "title": "Friday",
            "version": VERSION,
            "description": "Friday 的内置浏览器（用户在浏览器卡片里看得见、能一起操作）、定时任务、自定义脚本工具",
        },
        "actions": actions,
        "receives": ["turn.started", "done"],
        "sessions": "all",
        "hooks": hooks::EVENTS,
        // 本地模型（power://llm）的真地址由这里给（能力协议 §6.6）
        "models": ["power://"],
        "resident": true,
    })
}

/// 大脑里的工具名（friday__browser_navigate）是不是自己的浏览器工具：是的话回动作名
fn own_action(tool: &str) -> Option<&str> {
    let (owner, action) = tool.split_once("__")?;
    let own = owner.starts_with("friday") && !owner.starts_with("friday-client");
    (own && (browser_tools::is_tool(action) || action == "loop")).then_some(action)
}

fn serve_hook(params: &Value) -> Value {
    if params["hook_event_name"] != "PreToolUse" {
        return json!({});
    }
    let tool = params["tool_name"].as_str().unwrap_or("");
    let Some(action) = own_action(tool) else { return json!({}) };
    // 建定时任务不用批准（同 Friday：它本来就是照用户说的做；智能体建的有上限）
    if action == "loop" {
        return json!({ "need": false });
    }
    let arguments = params["tool_input"].as_object().cloned().unwrap_or_default();
    json!({ "need": browser_tools::need(tool, action, &arguments, params["session_id"].as_str().unwrap_or("")) })
}

fn serve_act(params: &Value, brain: &Brain) -> Value {
    let action = params["action"].as_str().unwrap_or("");
    let arguments = params["arguments"].as_object().cloned().unwrap_or_default();
    let session = params["session"].as_str().unwrap_or("");
    if action == "loop" {
        if params["agent"].as_str().is_some_and(|agent| !agent.is_empty()) {
            return json!({ "text": "定时任务只有主智能体能建、能停：把这件事交回给它", "ok": false });
        }
        let command = arguments.get("command").and_then(Value::as_str).unwrap_or("").trim();
        let command = if loops::is_command(command) { command.to_string() } else { format!("/loop {command}") };
        return json!({ "text": loops::handle(&command, session, "agent", true), "ok": true });
    }
    // 能力（本机的提供方）、脚本工具、扩展自己的能力：参数里的相对路径按这段会话的工作目录
    if caps::is_action(action) || scripts::is_meta(action) || scripts::is_script(action) {
        let caller = find_session(session).map(|stored| stored.cwd).filter(|cwd| !cwd.is_empty());
        let (ok, text, media) = if caps::is_action(action) {
            caps::act(action, &arguments, caller.as_deref())
        } else {
            scripts::act(action, &arguments, caller.as_deref(), brain)
        };
        let media: Vec<Value> = media
            .into_iter()
            .map(|path| {
                let lower = path.to_lowercase();
                let mime = [
                    (".png", "image/png"),
                    (".jpg", "image/jpeg"),
                    (".jpeg", "image/jpeg"),
                    (".gif", "image/gif"),
                    (".webp", "image/webp"),
                    (".wav", "audio/wav"),
                    (".webm", "video/webm"),
                ]
                .iter()
                .find(|(extension, _)| lower.ends_with(extension))
                .map_or("application/octet-stream", |(_, mime)| *mime);
                json!({ "path": path, "mime": mime })
            })
            .collect();
        return json!({ "text": text, "ok": ok, "media": media });
    }
    match browser_tools::run(action, &arguments, session) {
        Some(outcome) => {
            let media: Vec<Value> = outcome.image.into_iter().map(|path| json!({ "path": path, "mime": "image/jpeg" })).collect();
            json!({ "text": outcome.text, "ok": outcome.ok, "media": media })
        }
        None => json!({ "text": format!("Friday 没有动作 {action}"), "ok": false }),
    }
}

fn serve_brain_message(brain: &Arc<Brain>, message: Value, ready: &mut Option<Sender<()>>) {
    let method = message.get("method").and_then(Value::as_str);
    let id = message.get("id").filter(|id| !id.is_null()).cloned();
    match (method, id) {
        (Some("initialize"), Some(id)) => {
            brain.write(&json!({ "jsonrpc": "2.0", "id": id, "result": serve_initialize() }));
        }
        (Some("initialized"), None) => {
            if let Some(sender) = ready.take() {
                let _ = sender.send(());
            }
        }
        (Some("act"), Some(id)) => {
            let brain = brain.clone();
            std::thread::spawn(move || {
                let result = serve_act(&message["params"], &brain);
                brain.write(&json!({ "jsonrpc": "2.0", "id": id, "result": result }));
            });
        }
        (Some("hook"), Some(id)) => {
            // 用户配的钩子可能跑几分钟：另起线程（这个线程还要读别的请求、回话）
            let brain = brain.clone();
            std::thread::spawn(move || {
                let params = &message["params"];
                let event = params["hook_event_name"].as_str().unwrap_or("");
                let mut reply = hooks::run(params).reply(event);
                // 自己的动作怎么批准（钩子拦下了就不用说了）
                if reply.get("permissionDecision").is_none_or(|decision| decision != "deny")
                    && let Some(need) = serve_hook(params).get("need")
                {
                    reply["need"] = need.clone();
                }
                brain.write(&json!({ "jsonrpc": "2.0", "id": id, "result": reply }));
            });
        }
        (Some("model.endpoint"), Some(id)) => {
            let brain = brain.clone();
            std::thread::spawn(move || {
                let reply = match models::endpoint(&message["params"]) {
                    Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
                    Err(error) => json!({ "jsonrpc": "2.0", "id": id, "error": { "code": -32603, "message": error } }),
                };
                brain.write(&reply);
            });
        }
        (Some("event"), None) => {
            let params = &message["params"];
            let session = params["session"].as_str().unwrap_or("").to_string();
            match params["type"].as_str() {
                Some("turn.started") => {
                    TURNS.lock().unwrap().insert((brain.workspace.clone(), session));
                }
                Some("done") => {
                    TURNS.lock().unwrap().remove(&(brain.workspace.clone(), session.clone()));
                    loops::on_done(&session, params["turn"].as_str().unwrap_or(""), &params["data"]);
                    // 智能体这一轮开的网页：关掉（带 keep 的、正交给用户的留着）
                    std::thread::spawn(move || browser::close_agent_tabs(&session));
                }
                _ => {}
            }
        }
        (Some("ping"), Some(id)) => {
            brain.write(&json!({ "jsonrpc": "2.0", "id": id, "result": {} }));
        }
        (Some(other), Some(id)) => {
            brain.write(&json!({ "jsonrpc": "2.0", "id": id, "error": { "code": -32601, "message": format!("Friday 没有 {other}") } }));
        }
        (None, Some(id)) => {
            if let Some(sender) = id.as_u64().and_then(|id| brain.pending.lock().unwrap().remove(&id)) {
                let _ = sender.send(match message.get("error") {
                    Some(error) => Err(error.clone()),
                    None => Ok(message.get("result").cloned().unwrap_or(Value::Null)),
                });
            }
        }
        _ => {}
    }
}

/// 连上一个大脑（已经连着的不再连）：主令牌，在那里是能力「friday」
fn serve_attach(entry: &Value, wait: Duration) -> Result<(), String> {
    let workspace = entry["workspace"].as_str().unwrap_or("").to_string();
    let address = entry["address"].as_str().filter(|address| !address.is_empty()).ok_or("没有大脑的地址")?.to_string();
    if serve_mode() == "nobrains" || SERVED.lock().unwrap().contains_key(&workspace) {
        return Ok(());
    }
    if !SERVING.lock().unwrap().insert(workspace.clone()) {
        // 别的线程正在连：等它连好
        let deadline = Instant::now() + wait;
        while Instant::now() < deadline && !SERVED.lock().unwrap().contains_key(&workspace) {
            std::thread::sleep(Duration::from_millis(50));
        }
        return Ok(());
    }
    let connected = (|| {
        let mut stream = TcpStream::connect(&address).map_err(|error| format!("连不上 {address}：{error}"))?;
        writeln!(stream, "{}", json!({ "jsonrpc": "2.0", "method": "auth", "params": { "token": entry["token"] } }))
            .map_err(|error| error.to_string())?;
        let reading = stream.try_clone().map_err(|error| error.to_string())?;
        let brain = Arc::new(Brain {
            workspace: workspace.clone(),
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
                    if let Ok(message) = serde_json::from_str::<Value>(&line) {
                        serve_brain_message(&brain, message, &mut ready_sender);
                    }
                }
                let mut served = SERVED.lock().unwrap();
                if served.get(&brain.workspace).is_some_and(|current| Arc::ptr_eq(current, &brain)) {
                    served.remove(&brain.workspace);
                }
                drop(served);
                TURNS.lock().unwrap().retain(|(owner, _)| *owner != brain.workspace);
                serve_log("brain", &format!("{} 的大脑断开了", brain.workspace));
            });
        }
        ready.recv_timeout(wait).map_err(|_| "大脑没握手".to_string())?;
        SERVED.lock().unwrap().insert(workspace.clone(), brain);
        serve_log("brain", &format!("连上了 {workspace} 的大脑"));
        Ok(())
    })();
    SERVING.lock().unwrap().remove(&workspace);
    connected
}

fn serve_request(client: &str, method: &str, params: &Value) -> Reply {
    let invalid = |message: String| (-32602, message);
    match method {
        "ping" => Ok(json!({})),
        "attach" => serve_attach(params, Duration::from_secs(15)).map(|_| json!({})).map_err(|error| (NOT_FOUND, error)),
        // 内置终端：伪终端跑 shell，网关断了照常跑
        "terminals.list" | "terminals.open" | "terminals.attach" | "terminals.detach" | "terminals.write" | "terminals.resize"
        | "terminals.close" => {
            let id = params.get("id").and_then(Value::as_str).unwrap_or("");
            let size = |key: &str, default: u64| params.get(key).and_then(Value::as_u64).unwrap_or(default).min(1000) as u16;
            let outcome = match method {
                "terminals.list" => Ok(json!({ "terminals": pty::list() })),
                "terminals.open" => {
                    let session = params.get("session").and_then(Value::as_str).unwrap_or("");
                    let cwd = params
                        .get("cwd")
                        .and_then(Value::as_str)
                        .filter(|cwd| !cwd.is_empty())
                        .map(PathBuf::from)
                        .unwrap_or_else(user_home);
                    let command = params.get("command").and_then(Value::as_str);
                    pty::open(id, session, &cwd, size("cols", 80), size("rows", 24), command, client)
                }
                "terminals.attach" => pty::attach(id, client),
                "terminals.detach" => pty::detach(id, client),
                "terminals.write" => {
                    let data = params.get("data").and_then(Value::as_str).unwrap_or("");
                    let bytes = base64::engine::general_purpose::STANDARD
                        .decode(data)
                        .map_err(|error| invalid(format!("data 不是 base64：{error}")))?;
                    pty::write(id, &bytes)
                }
                "terminals.resize" => pty::resize(id, size("cols", 80), size("rows", 24)),
                _ => pty::close(id),
            };
            outcome.map_err(|message| (NOT_FOUND, message))
        }
        // 内置浏览器：无头 Chrome，标签按会话开，网关断了照常在
        method if method.starts_with("browser.") => {
            let outcome = match method {
                "browser.state" => Ok(browser::state()),
                "browser.open" => browser::open(
                    params.get("session").and_then(Value::as_str).unwrap_or(""),
                    params.get("url").and_then(Value::as_str).unwrap_or(""),
                ),
                _ => browser::request(method, params, client),
            };
            outcome.map_err(|message| (NOT_FOUND, message))
        }
        method if method.starts_with("powers.") => powers_request(method, params),
        method if method.starts_with("models.") => models::request(client, method, params),
        method if method.starts_with("loops.") => loops::request(method, params),
        method if method == "scripts.state" || method.starts_with("tools.") => scripts::request(method, params),
        method if method.starts_with("hooks.") => hooks::request(method, params),
        method if method.starts_with("capabilities.") => caps::request(method, params),
        method if method.starts_with("plugins.") => plugins::request(method, params),
        other => Err((-32601, format!("常驻的那份没有 {other}"))),
    }
}

/// powers.*（设置 › 能力看着这台时）
fn powers_request(method: &str, params: &Value) -> Reply {
    let id = || params["id"].as_str().filter(|id| !id.is_empty()).map(str::to_string).ok_or((-32602, "缺少参数 id".to_string()));
    match method {
        "powers.state" => return Ok(powers::state()),
        "powers.refresh" => {
            std::thread::spawn(powers::refresh);
        }
        "powers.install" => powers::install(&id()?, params["target"].as_str().unwrap_or("")),
        "powers.cancel" => powers::cancel(&id()?),
        "powers.uninstall" => powers::uninstall(&id()?),
        "powers.setEnabled" => {
            let enabled = params["enabled"].as_bool().ok_or((-32602, "缺少参数 enabled（布尔）".to_string()))?;
            powers::set_enabled(&id()?, enabled);
        }
        "powers.setOption" => {
            let key = params["key"].as_str().filter(|key| !key.is_empty()).ok_or((-32602, "缺少参数 key".to_string()))?;
            powers::set_option(&id()?, key, &params["value"]);
        }
        "powers.start" => powers::start_power(&id()?),
        "powers.stop" => powers::stop(&id()?),
        // 用户音色：path 是先经 file.upload 传上来的文件（.gguf 导入，别的当录音克隆），做完连目录删掉
        "powers.importVoice" => {
            let path = PathBuf::from(params["path"].as_str().unwrap_or(""));
            let uploads = canonical(&edith_home().join("uploads"));
            let file = canonical(&path);
            if !file.starts_with(&uploads) || !file.is_file() {
                return Err((-32602, "只认 file.upload 传上来的文件".into()));
            }
            let name = params["name"].as_str().filter(|name| !name.is_empty()).ok_or((-32602, "缺少参数 name".to_string()))?;
            let outcome =
                powers::import_voice(&file, name, params["text"].as_str().unwrap_or(""), params["replace"].as_bool().unwrap_or(false));
            if let Some(dir) = file.parent().filter(|dir| *dir != uploads) {
                let _ = std::fs::remove_dir_all(dir);
            }
            let (voice, transcript) = outcome.map_err(|error| (-32603, error))?;
            return Ok(json!({ "voice": voice, "transcript": transcript }));
        }
        "powers.deleteVoice" => {
            powers::delete_voice(params["name"].as_str().unwrap_or("")).map_err(|error| (-32603, error))?;
        }
        other => return Err((-32601, format!("没有这个方法：{other}"))),
    }
    Ok(json!({}))
}

fn serve_client(stream: TcpStream, token: &str) {
    let Ok(reading) = stream.try_clone() else { return };
    let mut reader = BufReader::new(reading);
    let mut first = String::new();
    if reader.read_line(&mut first).is_err() {
        return;
    }
    let auth: Value = serde_json::from_str(&first).unwrap_or_default();
    if auth["method"] != "auth" || auth["params"]["token"].as_str() != Some(token) {
        return;
    }
    let name = format!("g{}", NEXT_CLIENT.fetch_add(1, Ordering::Relaxed));
    let client = Arc::new(ServeClient { writer: Mutex::new(stream) });
    CLIENTS.lock().unwrap().insert(name.clone(), client.clone());
    for line in reader.lines().map_while(Result::ok) {
        let Ok(message) = serde_json::from_str::<Value>(&line) else { continue };
        let id = message.get("id").cloned().unwrap_or(Value::Null);
        let method = message["method"].as_str().unwrap_or("").to_string();
        let params = message.get("params").cloned().unwrap_or(json!({}));
        let answer = move |client: &ServeClient, reply: Reply| {
            if id.is_null() {
                return;
            }
            client.write(&match reply {
                Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
                Err((code, message)) => json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } }),
            });
        };
        // 慢的（连大脑、起浏览器开标签）另起线程；别的按收到的顺序（打的字、鼠标键盘不能乱）
        let slow = ["attach", "browser.open", "powers.importVoice", "powers.deleteVoice", "models.delete", "powers.uninstall"];
        let tests = [
            "tools.saveScript",
            "tools.testScript",
            "tools.rollback",
            "tools.createScript",
            "plugins.inspectGithub",
            "plugins.inspectData",
            "plugins.update",
            "plugins.install",
        ];
        if slow.contains(&method.as_str()) || tests.contains(&method.as_str()) {
            let (client, name) = (client.clone(), name.clone());
            std::thread::spawn(move || answer(&client, serve_request(&name, &method, &params)));
        } else {
            answer(&client, serve_request(&name, &method, &params));
        }
    }
    CLIENTS.lock().unwrap().remove(&name);
    pty::forget(&name);
    browser::forget(&name);
}

/// 起来就一直开着（同一台只一份：拿不到锁的直接退）
fn serve() -> Result<(), String> {
    let dir = serve_dir();
    std::fs::create_dir_all(&dir).map_err(|error| error.to_string())?;
    let lock = std::fs::File::options()
        .create(true)
        .truncate(false)
        .write(true)
        .open(dir.join("serve.lock"))
        .map_err(|error| error.to_string())?;
    if lock.try_lock().is_err() {
        return Ok(());
    }
    let listener = std::net::TcpListener::bind("127.0.0.1:0").map_err(|error| error.to_string())?;
    let address = listener.local_addr().map_err(|error| error.to_string())?.to_string();
    let token = uuid::Uuid::new_v4().simple().to_string();
    let entry = json!({ "pid": std::process::id(), "version": VERSION, "address": address, "token": token,
        "started": chrono::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false) });
    let registered = serve_registry();
    std::fs::write(&registered, serde_json::to_string_pretty(&entry).unwrap()).map_err(|error| error.to_string())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&registered, std::fs::Permissions::from_mode(0o600));
    }
    serve_log("serve", &format!("起来了：{address}（{VERSION}）"));
    powers::start();
    models::start();
    loops::start();
    plugins::start();
    {
        let token = token.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming().map_while(Result::ok) {
                let token = token.clone();
                std::thread::spawn(move || serve_client(stream, &token));
            }
        });
    }
    // 这台的大脑：登记了的都连上（别的客户端、edith run 起的也是）
    std::thread::spawn(|| {
        loop {
            for entry in registry() {
                if let Err(error) = serve_attach(&entry, Duration::from_secs(5)) {
                    serve_log("brain", &format!("连不上 {} 的大脑：{error}", entry["workspace"].as_str().unwrap_or("")));
                }
            }
            std::thread::sleep(Duration::from_secs(2));
        }
    });
    let idle = Duration::from_secs(std::env::var("EDITH_FRIDAY_HUB_IDLE").ok().and_then(|text| text.parse().ok()).unwrap_or(1800));
    let mut idle_since = Instant::now();
    loop {
        std::thread::sleep(Duration::from_secs(1));
        if !CLIENTS.lock().unwrap().is_empty()
            || pty::any_running()
            || !TURNS.lock().unwrap().is_empty()
            || powers::busy()
            || models::busy()
            || loops::active()
        {
            idle_since = Instant::now();
        } else if idle_since.elapsed() >= idle {
            break;
        }
    }
    serve_log("serve", "闲着没人用，退出");
    pty::close_all();
    browser::stop();
    powers::stop_all();
    if read_json_file(&registered)["pid"] == json!(std::process::id()) {
        let _ = std::fs::remove_file(&registered);
    }
    drop(lock);
    Ok(())
}

/// 这个目录的大脑（常驻那份连着的；没起就在那里起）
fn serve_brain_for(directory: &str) -> Result<Arc<Brain>, String> {
    let key = workspace_key(directory);
    if let Some(brain) = SERVED.lock().unwrap().get(&key).cloned() {
        return Ok(brain);
    }
    let entry = match registry().into_iter().find(|entry| entry["workspace"].as_str() == Some(key.as_str())) {
        Some(entry) => entry,
        None => launch_brain(Path::new(&key))?,
    };
    serve_attach(&entry, Duration::from_secs(30))?;
    SERVED.lock().unwrap().get(&key).cloned().ok_or_else(|| format!("连不上 {key} 的大脑"))
}

/// 定时任务到点：交给那段会话所在目录的大脑（排在手上这一轮之后；照会话上选的权限模式、思考档位、模型），回这一轮的 id。
/// 会话不在了回 Err("")
fn fire_loop(conversation: &str, text: &str) -> Result<String, String> {
    let session = find_session(conversation).ok_or_else(String::new)?;
    let directory = if session.cwd.is_empty() { default_directory() } else { session.cwd.clone() };
    let brain = serve_brain_for(&directory)?;
    // 还没发过话的空白会话：先在 EDITH 里建出来
    if session.blank() {
        brain.request("sessions.create", json!({ "id": session.id, "cwd": directory }), Duration::from_secs(30))?;
        if !session.title.is_empty()
            && let Some(path) = stored_session(&session.id).and_then(|stored| stored.path)
        {
            append_title(&path, &session.title);
        }
        META.remove_blank(&session.id);
    }
    let mut request = json!({ "session": session.id, "text": text, "urgency": "queue", "kind": "loop" });
    with_prefs(&mut request, &session.id, &[]);
    let reply = brain.request("sense", request, Duration::from_secs(30))?;
    reply["turn"].as_str().filter(|turn| !turn.is_empty()).map(str::to_string).ok_or_else(|| "大脑没收下".to_string())
}

/// 会话的标题（设置 › 定时任务的「会话：标题」）
fn conversation_title(id: &str) -> String {
    find_session(id).map(|session| session.title).filter(|title| !title.is_empty()).unwrap_or_else(|| "新对话".into())
}

// MARK: - 网关连常驻的那份

static HUB: LazyLock<Mutex<Option<Arc<Brain>>>> = LazyLock::new(|| Mutex::new(None));
static HUB_STARTING: Mutex<()> = Mutex::new(());

/// 登记着、连得上的常驻那份
fn serve_entry() -> Option<Value> {
    let entry = read_json_file(&serve_registry());
    let address = entry["address"].as_str()?.parse().ok()?;
    TcpStream::connect_timeout(&address, Duration::from_millis(500)).is_ok().then_some(entry)
}

/// 在后台起常驻的那份（脱离这次 SSH：网关退了它还在）
fn spawn_serve() -> Result<(), String> {
    let exe = std::env::current_exe().map_err(|error| error.to_string())?;
    let mut process = std::process::Command::new(exe);
    process.arg("serve").stdin(std::process::Stdio::null()).stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null());
    #[cfg(unix)]
    unsafe {
        use std::os::unix::process::CommandExt;
        process.pre_exec(|| {
            libc::setsid();
            Ok(())
        });
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const DETACHED_PROCESS: u32 = 0x0000_0008;
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        process.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW);
    }
    process.spawn().map(|_| ()).map_err(|error| format!("起不了常驻的那份：{error}"))
}

/// 常驻那份推来的
fn on_hub_message(hub: &Arc<Brain>, message: Value) {
    let method = message.get("method").and_then(Value::as_str);
    match method {
        // 终端：接着的有输出（原样转）、开了关了（全部终端）
        Some("terminal.output") => event("term.output", message["params"].clone()),
        Some("terminals") => {
            STATE.lock().unwrap().terminals = message["params"]["terminals"].as_array().cloned().unwrap_or_default();
            event("terminals", terminals_state());
        }
        // 浏览器：看着的标签的画面、下载（原样转）、标签变了（全部标签）
        Some("browser.frame" | "browser.download") => event(method.unwrap_or(""), message["params"].clone()),
        // 智能体请用户在浏览器里做件事：客户端弹出那张卡片（会话 id 换成 Friday 的大写）
        Some("browser.handOver") => {
            let mut params = message["params"].clone();
            params["conversationID"] = json!(upper(params["conversationID"].as_str().unwrap_or("")));
            event("browser.handOver", params);
        }
        Some("browser") => {
            STATE.lock().unwrap().browser = Some(message["params"].clone());
            event("browser", browser_state());
        }
        // 本地能力、本地模型：订了的客户端收；下载 / 换上失败只给发起的那个
        Some("powers") if STATE.lock().unwrap().powers => event("powers", message["params"].clone()),
        Some("models") if STATE.lock().unwrap().models => event("models", message["params"].clone()),
        Some("loops") if STATE.lock().unwrap().loops => event("loops", message["params"].clone()),
        // 脚本工具、能力提供方变了：设置页重列（另起线程：重列要问常驻那份，回话得靠这个线程读）
        Some("scripts") => {
            std::thread::spawn(|| config_changed(&["tools"]));
        }
        Some("capabilities") => {
            std::thread::spawn(|| config_changed(&["capabilities"]));
        }
        Some("modelsFailed") => event("modelsFailed", message["params"].clone()),
        // 本地模型下好了：建了本地引擎、给会话换上了（会话上选的记在 friday.json）
        Some("engines.changed") => {
            event("engines", engines_state());
            if let Some(conversation) = message["params"]["conversationID"].as_str() {
                changed(&lower(conversation), true);
            }
        }
        Some(_) => {}
        None => {
            let id = message.get("id").and_then(Value::as_u64);
            if let Some(sender) = id.and_then(|id| hub.pending.lock().unwrap().remove(&id)) {
                let _ = sender.send(match message.get("error") {
                    Some(error) => Err(error.clone()),
                    None => Ok(message.get("result").cloned().unwrap_or(Value::Null)),
                });
            }
        }
    }
}

/// 连着的常驻那份（没起就拉起、连上，取一次终端和标签）
fn hub() -> Result<Arc<Brain>, String> {
    if let Some(hub) = HUB.lock().unwrap().clone() {
        return Ok(hub);
    }
    if serve_mode() == "off" {
        return Err("常驻的那份关掉了（EDITH_FRIDAY_HUB=off），终端、浏览器用不了".into());
    }
    let _starting = HUB_STARTING.lock().unwrap();
    if let Some(hub) = HUB.lock().unwrap().clone() {
        return Ok(hub);
    }
    let entry = match serve_entry() {
        Some(entry) => entry,
        None => {
            spawn_serve()?;
            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                if let Some(entry) = serve_entry() {
                    break entry;
                }
                if Instant::now() >= deadline {
                    return Err(format!("常驻的那份没起来（日志在 {}）", serve_dir().join("serve.log").display()));
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    };
    let address = entry["address"].as_str().unwrap_or("");
    let mut stream = TcpStream::connect(address).map_err(|error| format!("连不上常驻的那份：{error}"))?;
    writeln!(stream, "{}", json!({ "jsonrpc": "2.0", "method": "auth", "params": { "token": entry["token"] } }))
        .map_err(|error| error.to_string())?;
    let reading = stream.try_clone().map_err(|error| error.to_string())?;
    let hub = Arc::new(Brain {
        workspace: String::new(),
        writer: Mutex::new(stream),
        pending: Mutex::new(HashMap::new()),
        next: AtomicU64::new(1),
    });
    {
        let hub = hub.clone();
        std::thread::spawn(move || {
            for line in BufReader::new(reading).lines().map_while(Result::ok) {
                if let Ok(message) = serde_json::from_str::<Value>(&line) {
                    on_hub_message(&hub, message);
                }
            }
            // 断了（它退出了）：下次用时再起、再连
            let mut current = HUB.lock().unwrap();
            if current.as_ref().is_some_and(|current| Arc::ptr_eq(current, &hub)) {
                *current = None;
            }
        });
    }
    let terminals = hub.request("terminals.list", json!({}), Duration::from_secs(10)).unwrap_or_default();
    let browser = hub.request("browser.state", json!({}), Duration::from_secs(10)).ok();
    {
        let mut state = STATE.lock().unwrap();
        state.terminals = terminals["terminals"].as_array().cloned().unwrap_or_default();
        state.browser = browser;
    }
    *HUB.lock().unwrap() = Some(hub.clone());
    Ok(hub)
}

/// 转给常驻的那份（本地能力、本地模型）；订阅记在这个网关
fn hub_forward(method: &str, params: &Value) -> Reply {
    let hub = hub().map_err(|error| (NOT_ALLOWED, error))?;
    let (target, subscribe) = match method {
        "powers.subscribe" => ("powers.state", Some(("powers", true))),
        "powers.unsubscribe" => {
            return {
                STATE.lock().unwrap().powers = false;
                Ok(json!({}))
            };
        }
        "models.subscribe" => ("models.state", Some(("models", true))),
        "models.unsubscribe" => {
            return {
                STATE.lock().unwrap().models = false;
                Ok(json!({}))
            };
        }
        other => (other, None),
    };
    // 慢的：等装好的音色、删模型（friday-llm 先停掉）
    let slow = [
        "powers.importVoice",
        "models.delete",
        "powers.deleteVoice",
        "plugins.inspectGithub",
        "plugins.inspectData",
        "plugins.update",
        "plugins.install",
    ];
    let wait = if slow.contains(&method) { 660 } else { 30 };
    let reply = hub.request(target, params.clone(), Duration::from_secs(wait)).map_err(|error| hub_error(&error))?;
    // 调过 loops.* 的客户端之后收 loops 事件
    if method.starts_with("loops.") {
        STATE.lock().unwrap().loops = true;
    }
    if let Some((what, on)) = subscribe {
        let mut state = STATE.lock().unwrap();
        if what == "powers" {
            state.powers = on;
        } else {
            state.models = on;
        }
    }
    Ok(reply)
}

/// 常驻那份回的错误：没有这个模型回 3，别的原样
fn hub_error(error: &str) -> (i64, String) {
    if error.starts_with("这台机器上没有这个本地模型") || error.starts_with("没有定时任务") {
        (NOT_FOUND, error.to_string())
    } else {
        (-32603, error.to_string())
    }
}

/// 让常驻的那份马上连上这个大脑（新起的大脑第一轮就要它的钩子、浏览器工具，不等它下一次看登记）
fn hub_attach(entry: &Value) {
    if serve_mode() == "off" {
        return;
    }
    let attached = hub().and_then(|hub| {
        let params = json!({ "address": entry["address"], "token": entry["token"], "workspace": entry["workspace"] });
        hub.request("attach", params, Duration::from_secs(20))
    });
    if let Err(error) = attached {
        log(&format!("常驻的那份没连上 {} 的大脑：{error}", entry["workspace"].as_str().unwrap_or("")));
    }
}

// MARK: - 本地能力（Friday.Powers）

mod powers {
    //! 本地能力（照 Friday Qt 的 PowerManager，Friday 仓库的 qt/docs/POWERS.md）：按需下载的本地 GPU 程序（本地语音 voice、本地大模型 llm、
    //! 生图 / 生视频 diffusion、设备 devices），装在常驻那份所在的这台、用这台的显卡。索引是 Friday.Powers 的 index-v2.json
    //! （`FRIDAY_POWERS_INDEX` 可以指别的地址或本地文件），20 秒后取一次、之后每 6 小时；包下好比大小和 SHA-256、用系统的 tar 解开，
    //! 要的模型、共用运行库（cuda13）一起下。进程：`<程序> serve …`（参数模板来自清单），从标准输出读 `{"event":"listening","url":…}`，
    //! 关标准输入它自己退，闲 15 分钟停掉把显存还回去。目录 `~/.edith/friday/powers`（`FRIDAY_POWERS_DIR` 可以换）。

    use std::collections::{BTreeMap, HashMap, VecDeque};
    use std::io::{BufRead, BufReader, Read, Write};
    use std::path::{Path, PathBuf};
    use std::process::{Child, ChildStdin, Command, Stdio};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Condvar, LazyLock, Mutex};
    use std::time::{Duration, Instant};

    use serde_json::{Map, Value, json};
    use sha2::Digest;

    const INDEX: &str = "https://github.com/integemjack/Friday.Powers/releases/download/index/index-v2.json";
    /// 闲置这么久（没有请求）就停掉，把显存还回去
    const IDLE_STOP: Duration = Duration::from_secs(15 * 60);
    /// 起来后这么久没报 listening 算失败
    const START_TIMEOUT: Duration = Duration::from_secs(120);

    pub fn root() -> PathBuf {
        match std::env::var_os("FRIDAY_POWERS_DIR").filter(|dir| !dir.is_empty()) {
            Some(dir) => PathBuf::from(dir),
            None => crate::serve_dir().join("powers"),
        }
    }

    fn power_dir(id: &str) -> PathBuf {
        root().join(id)
    }

    pub fn models_dir(id: &str) -> PathBuf {
        power_dir(id).join("models")
    }

    fn runtimes_dir() -> PathBuf {
        root().join("runtimes")
    }

    fn downloads_dir() -> PathBuf {
        root().join(".downloads")
    }

    fn executable_name(name: &str) -> String {
        if cfg!(windows) { format!("{name}.exe") } else { name.to_string() }
    }

    fn simple(name: &str) -> bool {
        let mut chars = name.chars();
        chars.next().is_some_and(|first| first.is_ascii_alphanumeric())
            && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
    }

    fn text(value: &Value, key: &str) -> String {
        value[key].as_str().unwrap_or("").to_string()
    }

    fn strings(value: &Value) -> Vec<String> {
        value.as_array().into_iter().flatten().filter_map(Value::as_str).filter(|text| !text.is_empty()).map(str::to_string).collect()
    }

    fn size_of(value: &Value) -> u64 {
        value.as_f64().map(|size| size.max(0.0) as u64).unwrap_or(0)
    }

    pub fn human_size(bytes: u64) -> String {
        if bytes >= 1 << 30 { format!("{:.1} GB", bytes as f64 / (1u64 << 30) as f64) } else { format!("{} MB", (bytes >> 20).max(1)) }
    }

    // MARK: 清单

    #[derive(Clone)]
    struct ModelFile {
        id: String,
        file: String,
        url: String,
        sha256: String,
        size: u64,
        purpose: String,
    }

    #[derive(Clone)]
    struct Target {
        id: String,
        detail: String,
        url: String,
        sha256: String,
        size: u64,
        models: Vec<String>,
        capabilities: Vec<String>,
        runtime: String,
    }

    #[derive(Clone)]
    struct Package {
        url: String,
        sha256: String,
        size: u64,
    }

    #[derive(Clone)]
    struct SharedRuntime {
        id: String,
        title: String,
        version: String,
        packages: HashMap<String, Package>,
    }

    #[derive(Clone)]
    pub struct Spec {
        pub id: String,
        title: String,
        detail: String,
        version: String,
        executable: String,
        serve: Vec<String>,
        capabilities: Vec<String>,
        models: Vec<ModelFile>,
        targets: Vec<Target>,
        pub families: Value,
        options: Vec<Value>,
    }

    impl Spec {
        fn parse(value: &Value) -> Option<Spec> {
            let models = value["models"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|model| {
                    let file = ModelFile {
                        id: text(model, "id"),
                        file: text(model, "file"),
                        url: text(model, "url"),
                        sha256: text(model, "sha256").trim().to_lowercase(),
                        size: size_of(&model["size"]),
                        purpose: text(model, "purpose"),
                    };
                    // 文件名不能跳出 models 目录
                    let bad = file.file.contains("..") || file.file.starts_with('/') || file.file.contains(':');
                    (!file.id.is_empty() && !file.file.is_empty() && !file.url.is_empty() && !bad).then_some(file)
                })
                .collect();
            let targets = value["targets"]
                .as_object()
                .into_iter()
                .flatten()
                .filter_map(|(id, target)| {
                    let target = Target {
                        id: id.clone(),
                        detail: text(target, "detail"),
                        url: text(target, "url"),
                        sha256: text(target, "sha256").trim().to_lowercase(),
                        size: size_of(&target["size"]),
                        models: strings(&target["models"]),
                        capabilities: strings(&target["capabilities"]),
                        runtime: text(target, "runtime"),
                    };
                    (!target.url.is_empty() && !target.sha256.is_empty()).then_some(target)
                })
                .collect();
            let spec = Spec {
                id: text(value, "id"),
                title: text(value, "title"),
                detail: text(value, "detail"),
                version: text(value, "version"),
                executable: text(value, "executable"),
                serve: strings(&value["serve"]),
                capabilities: strings(&value["capabilities"]),
                models,
                targets,
                families: value["families"].clone(),
                options: value["options"].as_array().cloned().unwrap_or_default(),
            };
            // id、程序名拼进路径：只能是简单名字
            if !simple(&spec.id) || !simple(&spec.executable) || spec.version.is_empty() || spec.serve.is_empty() {
                return None;
            }
            Some(Spec { title: if spec.title.is_empty() { spec.id.clone() } else { spec.title.clone() }, ..spec })
        }

        fn target(&self, id: &str) -> Option<&Target> {
            self.targets.iter().find(|target| target.id == id)
        }

        fn models_for(&self, target: &Target) -> Vec<ModelFile> {
            self.models.iter().filter(|model| target.models.contains(&model.id)).cloned().collect()
        }

        fn capabilities_for(&self, target: &Target) -> Vec<String> {
            if target.capabilities.is_empty() { self.capabilities.clone() } else { target.capabilities.clone() }
        }
    }

    fn parse_runtimes(index: &Value) -> HashMap<String, SharedRuntime> {
        let mut runtimes = HashMap::new();
        for (id, item) in index["runtimes"].as_object().into_iter().flatten() {
            let packages: HashMap<String, Package> = item["packages"]
                .as_object()
                .into_iter()
                .flatten()
                .filter_map(|(platform, package)| {
                    let package = Package {
                        url: text(package, "url"),
                        sha256: text(package, "sha256").trim().to_lowercase(),
                        size: size_of(&package["size"]),
                    };
                    (!package.url.is_empty() && !package.sha256.is_empty()).then(|| (platform.clone(), package))
                })
                .collect();
            let version = text(item, "version");
            if !simple(id) || !simple(&version) || packages.is_empty() {
                continue;
            }
            let title = if text(item, "title").is_empty() { id.clone() } else { text(item, "title") };
            runtimes.insert(id.clone(), SharedRuntime { id: id.clone(), title, version, packages });
        }
        runtimes
    }

    // MARK: 平台

    /// 不用显卡的 Power（devices）的包：<系统>-<架构>
    pub fn runtime_key() -> String {
        let arch = match std::env::consts::ARCH {
            "x86_64" => "x64",
            "aarch64" => "arm64",
            other => other,
        };
        let system = if cfg!(windows) {
            "windows"
        } else if cfg!(target_os = "macos") {
            "macos"
        } else {
            "linux"
        };
        format!("{system}-{arch}")
    }

    /// 这台能用的平台包（加速的在前；同 Friday 的 PowerPlatform::candidates）
    fn candidates() -> Vec<String> {
        let arch = std::env::consts::ARCH;
        let mut list: Vec<&str> = Vec::new();
        if cfg!(windows) && arch == "x86_64" {
            list.push("windows-x64-vulkan");
            let system = std::env::var("SystemRoot").unwrap_or_else(|_| r"C:\Windows".into());
            if Path::new(&system).join("System32").join("nvcuda.dll").exists() {
                list.extend(["windows-x64-cuda13", "windows-x64-cuda"]);
            }
        } else if cfg!(target_os = "macos") && arch == "aarch64" {
            list.push("macos-arm64-metal");
        } else if cfg!(target_os = "linux") && arch == "x86_64" {
            list.push("linux-x64-vulkan");
            if Path::new("/proc/driver/nvidia/version").exists() {
                list.push("linux-x64-cuda");
            }
        } else if cfg!(target_os = "linux") && arch == "aarch64" {
            let tegra = std::fs::read_to_string("/etc/nv_tegra_release").unwrap_or_default();
            let first = tegra.lines().next().unwrap_or("");
            if first.contains("R36") {
                list.push("linux-arm64-jetson-orin");
            } else if !first.contains("R32") {
                list.push("linux-arm64-vulkan");
            }
        }
        let mut list: Vec<String> = list.into_iter().map(str::to_string).collect();
        list.push(runtime_key());
        list
    }

    fn platform_title(target: &str) -> String {
        match target {
            "windows-x64-vulkan" => "Windows（Vulkan）",
            "windows-x64-cuda13" | "windows-x64-cuda" => "Windows（NVIDIA CUDA）",
            "macos-arm64-metal" => "macOS（Metal）",
            "linux-x64-vulkan" => "Linux（Vulkan）",
            "linux-x64-cuda" => "Linux（NVIDIA CUDA）",
            "linux-arm64-jetson-orin" => "Jetson Orin",
            "linux-arm64-vulkan" => "ARM64 Linux（Vulkan）",
            "windows-x64" => "Windows x64",
            "macos-arm64" => "macOS（Apple 芯片）",
            "linux-x64" => "Linux x64",
            "linux-arm64" => "ARM64 Linux",
            other => other,
        }
        .to_string()
    }

    /// 平台包改过名：装着旧名字的换成新的
    fn successor(target: &str) -> Option<&'static str> {
        (target == "windows-x64-cuda").then_some("windows-x64-cuda13")
    }

    fn newer(a: &str, b: &str) -> bool {
        let parse = |text: &str| text.split('.').map(|part| part.trim().parse::<u64>().unwrap_or(0)).collect::<Vec<_>>();
        parse(a) > parse(b)
    }

    // MARK: 状态

    #[derive(Clone)]
    struct Installed {
        version: String,
        target: String,
        dir: PathBuf,
        executable: String,
        serve: Vec<String>,
        capabilities: Vec<String>,
        title: String,
        detail: String,
        enabled: bool,
        runtime_dir: String,
        options: Vec<Value>,
        option_values: Map<String, Value>,
    }

    impl Installed {
        fn from_json(value: &Value) -> Installed {
            Installed {
                version: text(value, "version"),
                target: text(value, "target"),
                dir: PathBuf::from(text(value, "dir")),
                executable: text(value, "executable"),
                serve: strings(&value["serve"]),
                capabilities: strings(&value["capabilities"]),
                title: text(value, "title"),
                detail: text(value, "detail"),
                enabled: value["enabled"].as_bool().unwrap_or(true),
                runtime_dir: text(value, "runtimeDir"),
                options: value["options"].as_array().cloned().unwrap_or_default(),
                option_values: value["optionValues"].as_object().cloned().unwrap_or_default(),
            }
        }

        fn to_json(&self) -> Value {
            let mut value = json!({
                "version": self.version, "target": self.target, "dir": self.dir.to_string_lossy(), "executable": self.executable,
                "serve": self.serve, "capabilities": self.capabilities, "title": self.title, "detail": self.detail, "enabled": self.enabled,
            });
            if !self.runtime_dir.is_empty() {
                value["runtimeDir"] = json!(self.runtime_dir);
            }
            if !self.options.is_empty() {
                value["options"] = json!(self.options);
            }
            if !self.option_values.is_empty() {
                value["optionValues"] = json!(self.option_values);
            }
            value
        }
    }

    #[derive(Clone, Copy, PartialEq, Default)]
    enum Status {
        #[default]
        Idle,
        Downloading,
        Starting,
        Running,
        Stopping,
        Failed,
    }

    struct Process {
        child: Child,
        stdin: Option<ChildStdin>,
    }

    struct Running {
        status: Status,
        progress: f64,
        progress_text: String,
        error: String,
        cancel: Option<Arc<AtomicBool>>,
        process: Option<Arc<Mutex<Process>>>,
        url: String,
        token: String,
        stderr_tail: VecDeque<String>,
        stopping: bool,
        restart_after_stop: bool,
        last_used: Instant,
        runtime_dir: String,
    }

    impl Default for Running {
        fn default() -> Self {
            Running {
                status: Status::Idle,
                progress: -1.0,
                progress_text: String::new(),
                error: String::new(),
                cancel: None,
                process: None,
                url: String::new(),
                token: String::new(),
                stderr_tail: VecDeque::new(),
                stopping: false,
                restart_after_stop: false,
                last_used: Instant::now(),
                runtime_dir: String::new(),
            }
        }
    }

    #[derive(Default)]
    struct Manager {
        index: Vec<Spec>,
        runtimes: HashMap<String, SharedRuntime>,
        installed: BTreeMap<String, Installed>,
        running: HashMap<String, Running>,
        refreshing: bool,
        index_error: String,
        platforms: Vec<String>,
    }

    static MANAGER: LazyLock<Mutex<Manager>> = LazyLock::new(|| Mutex::new(Manager::default()));
    /// 状态变了（等进程起来的在等它）
    static SETTLED: Condvar = Condvar::new();
    static DIRTY: AtomicBool = AtomicBool::new(false);

    /// 变了：80 毫秒内合成一次推给网关（订了的客户端收 powers）
    fn changed() {
        DIRTY.store(true, Ordering::Relaxed);
        SETTLED.notify_all();
    }

    fn save(manager: &Manager) {
        let powers: Map<String, Value> = manager.installed.iter().map(|(id, installed)| (id.clone(), installed.to_json())).collect();
        let _ = std::fs::create_dir_all(root());
        let _ = std::fs::write(root().join("state.json"), serde_json::to_vec_pretty(&json!({ "powers": powers })).unwrap_or_default());
    }

    fn runtime_dir_of(runtime: &SharedRuntime) -> PathBuf {
        runtimes_dir().join(&runtime.id).join(&runtime.version)
    }

    fn runtime_ready(dir: &Path, sha256: &str) -> bool {
        std::fs::read_to_string(dir.join(".sha256")).is_ok_and(|text| text.trim().eq_ignore_ascii_case(sha256))
    }

    fn model_ready(path: &Path, model: &ModelFile) -> bool {
        let Ok(meta) = std::fs::metadata(path) else { return false };
        if model.size > 0 && meta.len() != model.size {
            return false;
        }
        let marker = PathBuf::from(format!("{}.sha256", path.display()));
        std::fs::read_to_string(marker).is_ok_and(|text| text.trim().eq_ignore_ascii_case(&model.sha256))
    }

    impl Manager {
        fn spec(&self, id: &str) -> Option<&Spec> {
            self.index.iter().find(|spec| spec.id == id)
        }

        fn usable(&self, target: &Target) -> bool {
            target.runtime.is_empty()
                || self.runtimes.get(&target.runtime).is_some_and(|runtime| runtime.packages.contains_key(&runtime_key()))
        }

        fn default_target(&self, spec: &Spec) -> String {
            self.platforms
                .iter()
                .find(|platform| spec.target(platform).is_some_and(|target| self.usable(target)))
                .cloned()
                .unwrap_or_default()
        }

        fn runtime_download_size(&self, target: &Target) -> u64 {
            let Some(runtime) = self.runtimes.get(&target.runtime) else { return 0 };
            let Some(package) = runtime.packages.get(&runtime_key()) else { return 0 };
            if runtime_ready(&runtime_dir_of(runtime), &package.sha256) { 0 } else { package.size }
        }

        fn manifest(&self, id: &str) -> Option<Spec> {
            if let Some(spec) = self.spec(id) {
                return Some(spec.clone());
            }
            let installed = self.installed.get(id)?;
            Spec::parse(&read_json(&installed.dir.join("power.json"))?)
        }
    }

    fn read_json(path: &Path) -> Option<Value> {
        serde_json::from_slice(&std::fs::read(path).ok()?).ok()
    }

    /// 起来时：读装了的、缓存的索引，清掉没人用的运行库；20 秒后取索引、之后每 6 小时，每分钟看一次闲着的进程
    pub fn start() {
        {
            let mut manager = MANAGER.lock().unwrap();
            manager.platforms = candidates();
            if let Some(state) = read_json(&root().join("state.json")) {
                for (id, value) in state["powers"].as_object().into_iter().flatten() {
                    let installed = Installed::from_json(value);
                    // 目录被手动删了就当没装
                    if installed.dir.join(executable_name(&installed.executable)).is_file() {
                        manager.installed.insert(id.clone(), installed);
                    }
                }
            }
            if let Some(index) = read_json(&root().join("index.json")) {
                manager.index = index["powers"].as_array().into_iter().flatten().filter_map(Spec::parse).collect();
                manager.runtimes = parse_runtimes(&index);
            }
            cleanup_runtimes(&manager);
        }
        std::thread::spawn(|| {
            loop {
                std::thread::sleep(Duration::from_millis(80));
                if DIRTY.swap(false, Ordering::Relaxed) {
                    crate::notify_all_clients("powers", state());
                    crate::models::powers_changed();
                    crate::caps::powers_changed();
                }
            }
        });
        std::thread::spawn(|| {
            std::thread::sleep(Duration::from_secs(20));
            loop {
                refresh();
                std::thread::sleep(Duration::from_secs(6 * 3600));
            }
        });
        std::thread::spawn(|| {
            loop {
                std::thread::sleep(Duration::from_secs(60));
                let idle: Vec<String> = {
                    let manager = MANAGER.lock().unwrap();
                    manager
                        .running
                        .iter()
                        .filter(|(_, running)| {
                            running.process.is_some() && running.status == Status::Running && running.last_used.elapsed() > IDLE_STOP
                        })
                        .map(|(id, _)| id.clone())
                        .collect()
                };
                for id in idle {
                    crate::serve_log("powers", &format!("本地能力 {id} 闲置 15 分钟，先停掉"));
                    stop(&id);
                }
            }
        });
    }

    // MARK: 索引

    fn fetch_index() -> Result<Vec<u8>, String> {
        let location = std::env::var("FRIDAY_POWERS_INDEX").ok().filter(|location| !location.is_empty()).unwrap_or_else(|| INDEX.into());
        if !location.starts_with("http://") && !location.starts_with("https://") {
            let path = location.strip_prefix("file://").unwrap_or(&location);
            return std::fs::read(path).map_err(|_| format!("打不开 {path}"));
        }
        let get = |url: &str| -> Result<(u16, Vec<u8>), String> {
            let mut response = crate::http_agent().get(url).call().map_err(|error| format!("取本地能力列表失败：{error}"))?;
            let status = response.status().as_u16();
            let mut body = Vec::new();
            response.body_mut().as_reader().read_to_end(&mut body).map_err(|error| format!("取本地能力列表失败：{error}"))?;
            Ok((status, body))
        };
        let (mut status, mut body) = get(&location)?;
        // 还没有 index-v2.json 时退回老的 index.json
        if status == 404
            && let Some(base) = location.strip_suffix("index-v2.json")
        {
            (status, body) = get(&format!("{base}index.json"))?;
        }
        match status {
            404 => Err("还没有发布可用的本地能力".into()),
            200..300 => Ok(body),
            other => Err(format!("取本地能力列表失败：HTTP {other}")),
        }
    }

    /// 重新取索引（检查更新）：装了的有新版本就在后台更新
    pub fn refresh() {
        {
            let mut manager = MANAGER.lock().unwrap();
            if manager.refreshing {
                return;
            }
            manager.refreshing = true;
        }
        changed();
        let fetched = fetch_index().and_then(|body| {
            let index: Value = serde_json::from_slice(&body).map_err(|_| "本地能力列表格式不对".to_string())?;
            if !index.is_object() {
                return Err("本地能力列表格式不对".to_string());
            }
            Ok((body, index))
        });
        let updates: Vec<(String, String)> = {
            let mut manager = MANAGER.lock().unwrap();
            manager.refreshing = false;
            match fetched {
                Ok((body, index)) => {
                    manager.index_error.clear();
                    let _ = std::fs::create_dir_all(root());
                    let _ = std::fs::write(root().join("index.json"), body);
                    manager.index = index["powers"].as_array().into_iter().flatten().filter_map(Spec::parse).collect();
                    manager.runtimes = parse_runtimes(&index);
                    updates(&manager)
                }
                Err(error) => {
                    crate::serve_log("powers", &error);
                    manager.index_error = error;
                    Vec::new()
                }
            }
        };
        changed();
        for (id, target) in updates {
            install(&id, &target);
        }
    }

    /// 装着的、索引里有新版本（或用的运行库被删了）的：(id, 平台包)
    fn updates(manager: &Manager) -> Vec<(String, String)> {
        let mut list = Vec::new();
        for (id, installed) in &manager.installed {
            let Some(latest) = manager.spec(id) else { continue };
            let mut target = installed.target.clone();
            if latest.target(&target).is_none_or(|current| !manager.usable(current)) {
                let Some(next) = successor(&target) else { continue };
                if latest.target(next).is_none_or(|next| !manager.usable(next))
                    || !manager.platforms.iter().any(|platform| platform == next)
                {
                    continue;
                }
                target = next.to_string();
            }
            let broken = !installed.runtime_dir.is_empty() && !Path::new(&installed.runtime_dir).exists();
            let busy = manager.running.get(id).is_some_and(|running| running.status == Status::Downloading);
            if (newer(&latest.version, &installed.version) || broken) && !busy {
                crate::serve_log("powers", &format!("本地能力 {id} 有新版本 {}（现在 {}），后台更新", latest.version, installed.version));
                list.push((id.clone(), target));
            }
        }
        list
    }

    // MARK: 安装

    /// 下到 destination：http(s) 断点续传（先下到 .part），file:// 直接复制（本机测试用的索引）
    pub fn fetch(url: &str, destination: &Path, cancel: &AtomicBool, progress: &mut dyn FnMut(u64)) -> Result<(), String> {
        if let Some(dir) = destination.parent() {
            std::fs::create_dir_all(dir).map_err(|error| error.to_string())?;
        }
        let name = destination.file_name().map(|name| name.to_string_lossy().into_owned()).unwrap_or_default();
        if !url.starts_with("http://") && !url.starts_with("https://") {
            let source = url.strip_prefix("file://").unwrap_or(url);
            std::fs::copy(source, destination).map_err(|_| format!("复制 {source} 失败"))?;
            progress(std::fs::metadata(destination).map(|meta| meta.len()).unwrap_or(0));
            return Ok(());
        }
        let part = PathBuf::from(format!("{}.part", destination.display()));
        let mut attempt = 0;
        loop {
            let have = std::fs::metadata(&part).map(|meta| meta.len()).unwrap_or(0);
            let mut request = crate::http_agent().get(url).header("User-Agent", format!("Friday/{}", crate::VERSION));
            if have > 0 {
                request = request.header("Range", format!("bytes={have}-"));
            }
            let outcome = (|| -> Result<(), (bool, String)> {
                let mut response = request.call().map_err(|error| (true, format!("下载 {name} 失败：{error}")))?;
                let status = response.status().as_u16();
                if status == 416 {
                    return Ok(());
                }
                if !(200..300).contains(&status) {
                    return Err((status >= 500 || status == 429, format!("下载 {name} 失败：HTTP {status}")));
                }
                let append = status == 206 && have > 0;
                let mut file = std::fs::File::options()
                    .create(true)
                    .write(true)
                    .append(append)
                    .truncate(!append)
                    .open(&part)
                    .map_err(|error| (false, format!("写入 {name} 失败：{error}")))?;
                let mut done = if append { have } else { 0 };
                let mut reader = response.body_mut().as_reader();
                let mut buffer = vec![0u8; 256 * 1024];
                loop {
                    if cancel.load(Ordering::Relaxed) {
                        return Err((false, CANCELLED.into()));
                    }
                    let read = reader.read(&mut buffer).map_err(|error| (true, format!("下载 {name} 断了：{error}")))?;
                    if read == 0 {
                        break;
                    }
                    file.write_all(&buffer[..read]).map_err(|error| (false, format!("写入 {name} 失败：{error}")))?;
                    done += read as u64;
                    progress(done);
                }
                Ok(())
            })();
            match outcome {
                Ok(()) => {
                    std::fs::rename(&part, destination).map_err(|error| format!("放不进 {}：{error}", destination.display()))?;
                    return Ok(());
                }
                // 断了、服务端 5xx：等一会儿从 .part 接着下，最多再试 5 次
                Err((true, _)) if attempt < 5 => {
                    std::thread::sleep(Duration::from_secs([2, 5, 10, 20, 30][attempt]));
                    attempt += 1;
                }
                Err((_, message)) => return Err(message),
            }
        }
    }

    pub const CANCELLED: &str = "已取消";

    pub fn sha256_of(path: &Path, cancel: &AtomicBool) -> Result<String, String> {
        let mut file = std::fs::File::open(path).map_err(|error| format!("读不了 {}：{error}", path.display()))?;
        let mut hasher = sha2::Sha256::new();
        let mut buffer = vec![0u8; 8 << 20];
        loop {
            if cancel.load(Ordering::Relaxed) {
                return Err(CANCELLED.into());
            }
            let read = file.read(&mut buffer).map_err(|error| error.to_string())?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
        Ok(hasher.finalize().iter().map(|byte| format!("{byte:02x}")).collect())
    }

    fn verify(path: &Path, size: u64, sha256: &str, cancel: &AtomicBool) -> Result<(), String> {
        let actual_size = std::fs::metadata(path).map(|meta| meta.len()).unwrap_or(0);
        if (size > 0 && actual_size != size) || !sha256_of(path, cancel)?.eq_ignore_ascii_case(sha256) {
            let _ = std::fs::remove_file(path);
            let name = path.file_name().map(|name| name.to_string_lossy().into_owned()).unwrap_or_default();
            return Err(format!("{name} 校验不对（下载不完整或被改过），已删除，请重试"));
        }
        Ok(())
    }

    /// 解开 zip / tar.gz（Windows 10 起自带的 tar.exe 也认 zip）
    fn extract(archive: &Path, into: &Path) -> Result<(), String> {
        let tar = if cfg!(windows) {
            PathBuf::from(std::env::var("SystemRoot").unwrap_or_else(|_| r"C:\Windows".into())).join("System32").join("tar.exe")
        } else {
            PathBuf::from("tar")
        };
        let output =
            Command::new(tar).arg("-xf").arg(archive).arg("-C").arg(into).output().map_err(|error| format!("解压失败：{error}"))?;
        if !output.status.success() {
            return Err(format!("解压失败：{}", String::from_utf8_lossy(&output.stderr).trim()));
        }
        Ok(())
    }

    /// 解开的目录里找程序所在的目录（包里多一层同名目录时）；只认文件
    fn package_root(staging: &Path, executable: &str) -> Option<PathBuf> {
        if staging.join(executable).is_file() {
            return Some(staging.to_path_buf());
        }
        std::fs::read_dir(staging).ok()?.flatten().map(|entry| entry.path()).find(|path| path.is_dir() && path.join(executable).is_file())
    }

    fn random_token() -> String {
        uuid::Uuid::new_v4().simple().to_string()
    }

    /// 在这台上下载安装（target 不给用装着的 / 这台默认的）；装了的就是更新、换平台
    pub fn install(id: &str, requested: &str) {
        let job = {
            let mut manager = MANAGER.lock().unwrap();
            let Some(spec) = manager.spec(id).cloned() else { return };
            if manager.running.get(id).is_some_and(|running| running.status == Status::Downloading) {
                return;
            }
            let mut target_id = if !requested.is_empty() {
                requested.to_string()
            } else if let Some(installed) = manager.installed.get(id) {
                installed.target.clone()
            } else {
                manager.default_target(&spec)
            };
            if spec.target(&target_id).is_none()
                && let Some(next) = successor(&target_id)
            {
                target_id = next.to_string();
            }
            let target = spec.target(&target_id).cloned();
            let fits = target.as_ref().is_some_and(|target| manager.platforms.contains(&target.id) && manager.usable(target));
            let runtime = target.as_ref().and_then(|target| manager.runtimes.get(&target.runtime).cloned());
            let running = manager.running.entry(id.to_string()).or_default();
            if !fits {
                running.status = Status::Failed;
                running.error = format!("这台机器用不了「{}」", spec.title);
                drop(manager);
                changed();
                return;
            }
            let cancel = Arc::new(AtomicBool::new(false));
            running.status = Status::Downloading;
            running.progress = 0.0;
            running.progress_text = "准备下载".into();
            running.error.clear();
            running.cancel = Some(cancel.clone());
            (spec, target.unwrap(), runtime, cancel)
        };
        changed();
        std::thread::spawn(move || {
            let (spec, target, runtime, cancel) = job;
            let id = spec.id.clone();
            let outcome = install_job(&spec, &target, runtime.as_ref(), &cancel);
            let mut manager = MANAGER.lock().unwrap();
            match outcome {
                Ok(fresh) => finish_install(&mut manager, &id, fresh),
                Err(message) => {
                    if message != CANCELLED {
                        crate::serve_log("powers", &format!("安装本地能力 {id} 失败：{message}"));
                    }
                    let running = manager.running.entry(id).or_default();
                    running.status = if running.process.is_some() {
                        Status::Running
                    } else if message == CANCELLED {
                        Status::Idle
                    } else {
                        Status::Failed
                    };
                    running.progress = -1.0;
                    running.error = message;
                    running.cancel = None;
                }
            }
            drop(manager);
            changed();
        });
    }

    /// 一次只装一个共用运行库（几个 Power 一起装时只下一次）
    static RUNTIME_LOCK: Mutex<()> = Mutex::new(());

    fn install_job(spec: &Spec, target: &Target, runtime: Option<&SharedRuntime>, cancel: &AtomicBool) -> Result<Installed, String> {
        let id = &spec.id;
        let package = match runtime {
            Some(runtime) => Some(runtime.packages.get(&runtime_key()).cloned().ok_or(format!("这台机器没有「{}」的包", runtime.title))?),
            None if !target.runtime.is_empty() => return Err(format!("索引里没有「{}」", target.runtime)),
            None => None,
        };
        let mut runtime_path = runtime.map(runtime_dir_of).unwrap_or_default();
        let need_runtime = package.as_ref().is_some_and(|package| !runtime_ready(&runtime_path, &package.sha256));
        let missing: Vec<ModelFile> =
            spec.models_for(target).into_iter().filter(|model| !model_ready(&models_dir(id).join(&model.file), model)).collect();
        let total = target.size
            + if need_runtime { package.as_ref().map_or(0, |package| package.size) } else { 0 }
            + missing.iter().map(|model| model.size).sum::<u64>();
        let mut done = 0u64;
        let mut shown = -1.0;
        let mut report = |received: u64, what: &str, done: u64| {
            let progress = if total > 0 { ((done + received) as f64 / total as f64).min(1.0) } else { 0.0 };
            if progress - shown < 0.005 && progress < 1.0 {
                return;
            }
            shown = progress;
            let text = format!("{what}（{} / {}）", human_size(done + received), human_size(total));
            if let Some(running) = MANAGER.lock().unwrap().running.get_mut(id) {
                running.progress = progress;
                running.progress_text = text;
            }
            changed();
        };
        // 0. 共用运行库（解到 runtimes/<id>/<版本>/，.sha256 最后写：有它才算装好）
        if let (Some(runtime), Some(package)) = (runtime, package.as_ref()) {
            let _one = RUNTIME_LOCK.lock().unwrap();
            if !runtime_ready(&runtime_path, &package.sha256) {
                let archive = downloads_dir().join(package.url.rsplit('/').next().unwrap_or("runtime"));
                let label = format!("下载 {} {}", runtime.title, runtime.version);
                fetch(&package.url, &archive, cancel, &mut |received| report(received, &label, done))?;
                verify(&archive, package.size, &package.sha256, cancel)?;
                let staging = runtimes_dir().join(&runtime.id).join(".staging");
                let _ = std::fs::remove_dir_all(&staging);
                std::fs::create_dir_all(&staging).map_err(|error| error.to_string())?;
                extract(&archive, &staging)?;
                if runtime_path.exists() && std::fs::remove_dir_all(&runtime_path).is_err() {
                    runtime_path = PathBuf::from(format!("{}-{}", runtime_path.display(), &random_token()[..6]));
                }
                std::fs::rename(&staging, &runtime_path).map_err(|_| format!("放不进 {}", runtime_path.display()))?;
                std::fs::write(runtime_path.join(".sha256"), &package.sha256).map_err(|error| error.to_string())?;
                let _ = std::fs::remove_file(&archive);
            }
            if need_runtime {
                done += package.size;
            }
        }
        // 1. 平台包
        let archive = downloads_dir().join(target.url.rsplit('/').next().unwrap_or("package"));
        let what = format!("下载 {} {}", spec.title, spec.version);
        fetch(&target.url, &archive, cancel, &mut |received| report(received, &what, done))?;
        verify(&archive, target.size, &target.sha256, cancel)?;
        done += target.size;
        if let Some(running) = MANAGER.lock().unwrap().running.get_mut(id) {
            running.progress_text = "解压".into();
        }
        changed();
        let staging = power_dir(id).join(".staging");
        let _ = std::fs::remove_dir_all(&staging);
        std::fs::create_dir_all(&staging).map_err(|error| error.to_string())?;
        extract(&archive, &staging)?;
        let exe = executable_name(&spec.executable);
        let Some(package_root) = package_root(&staging, &exe) else {
            let _ = std::fs::remove_dir_all(&staging);
            return Err(format!("包里没有 {exe}"));
        };
        let mut destination = power_dir(id).join(format!("{}-{}", spec.version, target.id));
        if destination.exists() && std::fs::remove_dir_all(&destination).is_err() {
            destination = PathBuf::from(format!("{}-{}", destination.display(), &random_token()[..6]));
        }
        std::fs::rename(&package_root, &destination).map_err(|_| format!("放不进 {}", destination.display()))?;
        let _ = std::fs::remove_dir_all(&staging);
        let _ = std::fs::remove_file(&archive);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(destination.join(&exe), std::fs::Permissions::from_mode(0o755));
        }
        // 2. 模型（各版本共用）
        for model in &missing {
            let path = models_dir(id).join(&model.file);
            let label = format!("下载模型 {}", if model.purpose.is_empty() { &model.id } else { &model.purpose });
            fetch(&model.url, &path, cancel, &mut |received| report(received, &label, done))?;
            verify(&path, model.size, &model.sha256, cancel)?;
            let _ = std::fs::write(format!("{}.sha256", path.display()), &model.sha256);
            done += model.size;
        }
        Ok(Installed {
            version: spec.version.clone(),
            target: target.id.clone(),
            dir: destination,
            executable: spec.executable.clone(),
            serve: spec.serve.clone(),
            capabilities: spec.capabilities_for(target),
            title: spec.title.clone(),
            detail: spec.detail.clone(),
            enabled: true,
            runtime_dir: if runtime.is_some() { runtime_path.to_string_lossy().into_owned() } else { String::new() },
            options: spec.options.clone(),
            option_values: Map::new(),
        })
    }

    fn finish_install(manager: &mut Manager, id: &str, fresh: Installed) {
        let mut installed = fresh;
        if let Some(previous) = manager.installed.get(id) {
            installed.enabled = previous.enabled;
            installed.option_values = previous.option_values.clone();
        }
        crate::serve_log(
            "powers",
            &format!("本地能力 {id} {}（{}）已装好：{}", installed.version, installed.target, installed.dir.display()),
        );
        manager.installed.insert(id.to_string(), installed);
        save(manager);
        let running = manager.running.entry(id.to_string()).or_default();
        running.progress = -1.0;
        running.progress_text.clear();
        running.error.clear();
        running.cancel = None;
        if running.process.is_some() {
            // 正在跑旧版本：停掉换新的（下个请求按新版本拉起）
            running.status = Status::Running;
            stop_locked(manager, id, true);
        } else {
            running.status = Status::Idle;
            cleanup_versions(manager, id);
        }
        cleanup_runtimes(manager);
    }

    /// 旧版本目录（不是装着的、不是模型）删掉
    fn cleanup_versions(manager: &Manager, id: &str) {
        let Some(keep) = manager.installed.get(id).map(|installed| installed.dir.clone()) else { return };
        for entry in std::fs::read_dir(power_dir(id)).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.is_dir() && entry.file_name() != "models" && path != keep {
                let _ = std::fs::remove_dir_all(&path);
            }
        }
    }

    /// 没有装着的 Power、也没有在跑的进程用的运行库删掉（有在装的先不动）
    fn cleanup_runtimes(manager: &Manager) {
        if manager.running.values().any(|running| running.status == Status::Downloading) {
            return;
        }
        let mut keep: Vec<PathBuf> = manager
            .installed
            .values()
            .filter(|installed| !installed.runtime_dir.is_empty())
            .map(|installed| PathBuf::from(&installed.runtime_dir))
            .collect();
        keep.extend(
            manager
                .running
                .values()
                .filter(|running| running.process.is_some() && !running.runtime_dir.is_empty())
                .map(|running| PathBuf::from(&running.runtime_dir)),
        );
        for kind in std::fs::read_dir(runtimes_dir()).into_iter().flatten().flatten() {
            for version in std::fs::read_dir(kind.path()).into_iter().flatten().flatten() {
                if !keep.contains(&version.path()) && std::fs::remove_dir_all(version.path()).is_ok() {
                    crate::serve_log("powers", &format!("运行库 {} 没有能力在用了，已删除", version.path().display()));
                }
            }
            let _ = std::fs::remove_dir(kind.path());
        }
        let _ = std::fs::remove_dir(runtimes_dir());
    }

    pub fn cancel(id: &str) {
        if let Some(cancel) = MANAGER.lock().unwrap().running.get(id).and_then(|running| running.cancel.clone()) {
            cancel.store(true, Ordering::Relaxed);
        }
    }

    pub fn uninstall(id: &str) {
        cancel(id);
        let process = {
            let mut manager = MANAGER.lock().unwrap();
            let process = manager.running.remove(id).and_then(|running| running.process);
            manager.installed.remove(id);
            save(&manager);
            process
        };
        if let Some(process) = process {
            let mut process = process.lock().unwrap();
            process.stdin = None;
            let _ = process.child.kill();
            let _ = process.child.wait();
        }
        let _ = std::fs::remove_dir_all(power_dir(id));
        cleanup_runtimes(&MANAGER.lock().unwrap());
        crate::serve_log("powers", &format!("本地能力 {id} 已卸载"));
        changed();
    }

    pub fn set_enabled(id: &str, enabled: bool) {
        {
            let mut manager = MANAGER.lock().unwrap();
            let Some(installed) = manager.installed.get_mut(id) else { return };
            if installed.enabled == enabled {
                return;
            }
            installed.enabled = enabled;
            save(&manager);
            if !enabled {
                stop_locked(&mut manager, id, false);
            }
        }
        changed();
    }

    /// 放进环境变量的写法：整数不带小数点
    fn option_text(value: &Value) -> String {
        match value {
            Value::Number(number) => match number.as_f64() {
                Some(float) if float.fract() == 0.0 => format!("{}", float as i64),
                Some(float) => float.to_string(),
                None => number.to_string(),
            },
            Value::Bool(flag) => if *flag { "1" } else { "0" }.into(),
            Value::String(text) => text.clone(),
            _ => String::new(),
        }
    }

    fn option_value(option: &Value, values: &Map<String, Value>) -> Value {
        values.get(option["key"].as_str().unwrap_or("")).cloned().unwrap_or_else(|| option["default"].clone())
    }

    /// 改装好的这一版清单里的选项（只认 choices 里的值）；在跑的进程重新启动后生效
    pub fn set_option(id: &str, key: &str, value: &Value) {
        {
            let mut manager = MANAGER.lock().unwrap();
            let Some(installed) = manager.installed.get_mut(id) else { return };
            let Some(option) = installed.options.iter().find(|option| option["key"] == json!(key)).cloned() else { return };
            let chosen = option_text(value);
            let known = option["choices"].as_array().into_iter().flatten().any(|choice| option_text(&choice["value"]) == chosen);
            if !known || option_text(&option_value(&option, &installed.option_values)) == chosen {
                return;
            }
            if chosen == option_text(&option["default"]) {
                installed.option_values.remove(key);
            } else {
                installed.option_values.insert(key.to_string(), value.clone());
            }
            save(&manager);
            crate::serve_log("powers", &format!("本地能力 {id} 的选项 {key} 改成 {chosen}"));
            if manager.running.get(id).is_some_and(|running| running.process.is_some()) {
                stop_locked(&mut manager, id, false);
            }
        }
        changed();
    }

    // MARK: 进程

    pub fn start_power(id: &str) {
        let mut manager = MANAGER.lock().unwrap();
        if manager.running.get(id).is_some_and(|running| running.process.is_some())
            || !manager.installed.get(id).is_some_and(|installed| installed.enabled)
        {
            return;
        }
        start_locked(&mut manager, id);
        drop(manager);
        changed();
    }

    pub fn stop(id: &str) {
        stop_locked(&mut MANAGER.lock().unwrap(), id, false);
        changed();
    }

    fn start_locked(manager: &mut Manager, id: &str) {
        let Some(installed) = manager.installed.get(id).cloned() else { return };
        let token = random_token();
        let models = models_dir(id);
        let _ = std::fs::create_dir_all(&models);
        let mut arguments: Vec<String> = Vec::new();
        for argument in &installed.serve {
            // 「{library}」：模型库的目录（魔搭、HF 两个缓存），前面的参数名跟着每个目录重复一遍
            if argument == "{library}" {
                let flag = arguments.last().cloned();
                for (index, dir) in crate::models::roots().iter().enumerate() {
                    if index > 0
                        && let Some(flag) = &flag
                    {
                        arguments.push(flag.clone());
                    }
                    arguments.push(dir.to_string_lossy().into_owned());
                }
                continue;
            }
            arguments.push(argument.replace("{models}", &models.to_string_lossy()).replace("{token}", &token));
        }
        let mut command = Command::new(installed.dir.join(executable_name(&installed.executable)));
        command.args(&arguments).current_dir(&installed.dir).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
        if !installed.runtime_dir.is_empty() {
            // 共用运行库（cuBLAS 这类）：Windows 按 PATH、Linux 按 LD_LIBRARY_PATH 找动态库，放在最前面
            let name = if cfg!(windows) { "PATH" } else { "LD_LIBRARY_PATH" };
            let previous = std::env::var(name).unwrap_or_default();
            let separator = if cfg!(windows) { ";" } else { ":" };
            command.env(
                name,
                if previous.is_empty() { installed.runtime_dir.clone() } else { format!("{}{separator}{previous}", installed.runtime_dir) },
            );
        }
        for option in &installed.options {
            let name = option["env"].as_str().unwrap_or("");
            let chosen = option_value(option, &installed.option_values);
            if !name.is_empty() && !chosen.is_null() {
                command.env(name, option_text(&chosen));
            }
        }
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
            command.creation_flags(0x0800_0000 | 0x0000_0200);
        }
        let running = manager.running.entry(id.to_string()).or_default();
        running.token = token;
        running.url.clear();
        running.error.clear();
        running.stderr_tail.clear();
        running.stopping = false;
        running.runtime_dir = installed.runtime_dir.clone();
        crate::serve_log("powers", &format!("拉起本地能力 {id} {}", installed.version));
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(error) => {
                running.status = Status::Failed;
                running.error = format!("起不了本地能力：{error}");
                return;
            }
        };
        let stdin = child.stdin.take();
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        let process = Arc::new(Mutex::new(Process { child, stdin }));
        running.process = Some(process.clone());
        running.status = Status::Starting;
        let started = Instant::now();
        if let Some(stdout) = stdout {
            let id = id.to_string();
            let process = process.clone();
            std::thread::spawn(move || {
                for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                    let Ok(event) = serde_json::from_str::<Value>(line.trim()) else { continue };
                    if event["event"] != "listening" {
                        continue;
                    }
                    let mut manager = MANAGER.lock().unwrap();
                    if let Some(running) = manager.running.get_mut(&id)
                        && running.process.as_ref().is_some_and(|current| Arc::ptr_eq(current, &process))
                        && running.status == Status::Starting
                    {
                        running.url = event["url"].as_str().unwrap_or("").trim_end_matches('/').to_string();
                        running.status = Status::Running;
                        running.last_used = Instant::now();
                        crate::serve_log("powers", &format!("本地能力 {id} 已就绪：{}", running.url));
                    }
                    drop(manager);
                    changed();
                }
            });
        }
        if let Some(stderr) = stderr {
            let id = id.to_string();
            std::thread::spawn(move || {
                for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                    let line = line.trim().to_string();
                    if line.is_empty() {
                        continue;
                    }
                    if let Some(running) = MANAGER.lock().unwrap().running.get_mut(&id) {
                        running.stderr_tail.push_back(line);
                        while running.stderr_tail.len() > 30 {
                            running.stderr_tail.pop_front();
                        }
                    }
                }
            });
        }
        // 看它退出（起不来的两分钟后结束它）
        let id = id.to_string();
        std::thread::spawn(move || {
            let code = loop {
                std::thread::sleep(Duration::from_millis(200));
                let mut guard = process.lock().unwrap();
                match guard.child.try_wait() {
                    Ok(Some(status)) => break status.code().unwrap_or(-1),
                    Ok(None) => {}
                    Err(_) => break -1,
                }
                let starting = MANAGER.lock().unwrap().running.get(&id).is_some_and(|running| running.status == Status::Starting);
                if starting && started.elapsed() > START_TIMEOUT {
                    if let Some(running) = MANAGER.lock().unwrap().running.get_mut(&id) {
                        running.stderr_tail.push_back("两分钟没就绪".into());
                    }
                    let _ = guard.child.kill();
                }
            };
            finished(&id, &process, code);
        });
    }

    fn finished(id: &str, process: &Arc<Mutex<Process>>, code: i32) {
        let restart = {
            let mut manager = MANAGER.lock().unwrap();
            let Some(running) = manager.running.get_mut(id) else { return };
            if !running.process.as_ref().is_some_and(|current| Arc::ptr_eq(current, process)) {
                return;
            }
            running.process = None;
            running.url.clear();
            let expected = std::mem::take(&mut running.stopping);
            let mut error = String::new();
            if !expected {
                let mut tail: Vec<String> =
                    running.stderr_tail.iter().filter(|line| line.contains("error") || line.contains("失败")).cloned().collect();
                if tail.is_empty() {
                    tail = running.stderr_tail.iter().rev().take(3).rev().cloned().collect();
                }
                error = format!(
                    "本地能力进程退出了（退出码 {code}）{}",
                    if tail.is_empty() { String::new() } else { format!("：{}", tail.join(" ")) }
                );
                crate::serve_log("powers", &format!("{id} {error}"));
            }
            running.status = if expected { Status::Idle } else { Status::Failed };
            running.error = error;
            running.runtime_dir.clear();
            let restart = std::mem::take(&mut running.restart_after_stop);
            if expected {
                cleanup_versions(&manager, id);
                cleanup_runtimes(&manager);
            }
            restart && manager.installed.get(id).is_some_and(|installed| installed.enabled)
        };
        if restart {
            start_locked(&mut MANAGER.lock().unwrap(), id);
        }
        changed();
    }

    /// 停掉（restart：停了再按新的拉起）：关标准输入它自己退，3 秒没退就结束它
    fn stop_locked(manager: &mut Manager, id: &str, restart: bool) {
        let Some(running) = manager.running.get_mut(id) else { return };
        let Some(process) = running.process.clone() else {
            if restart {
                start_locked(manager, id);
            }
            return;
        };
        running.stopping = true;
        running.restart_after_stop = restart;
        running.status = Status::Stopping;
        process.lock().unwrap().stdin = None;
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_secs(3));
            let mut process = process.lock().unwrap();
            if matches!(process.child.try_wait(), Ok(None)) {
                let _ = process.child.kill();
            }
        });
    }

    /// 没在跑就拉起，等到就绪：(地址, 令牌)
    pub fn ensure_running(id: &str, timeout: Duration) -> Result<(String, String), String> {
        let deadline = Instant::now() + timeout;
        let mut manager = MANAGER.lock().unwrap();
        let title =
            manager.installed.get(id).map(|installed| installed.title.clone()).ok_or(format!("本地能力「{id}」还没安装（设置 › 能力）"))?;
        if !manager.installed[id].enabled {
            return Err(format!("本地能力「{title}」被关掉了（设置 › 能力）"));
        }
        if manager.running.get(id).is_none_or(|running| running.process.is_none()) {
            start_locked(&mut manager, id);
            changed();
        }
        loop {
            let running = manager.running.get_mut(id).ok_or("本地能力已停止")?;
            match (running.status, running.process.is_some()) {
                (Status::Running, true) => {
                    running.last_used = Instant::now();
                    return Ok((running.url.clone(), running.token.clone()));
                }
                (_, false) => return Err(if running.error.is_empty() { "本地能力已停止".into() } else { running.error.clone() }),
                _ => {}
            }
            let left = deadline.checked_duration_since(Instant::now()).ok_or(format!("本地能力「{title}」启动超时"))?;
            manager = SETTLED.wait_timeout(manager, left.min(Duration::from_millis(500))).unwrap().0;
        }
    }

    // MARK: 查询

    pub fn is_installed(id: &str) -> bool {
        MANAGER.lock().unwrap().installed.contains_key(id)
    }

    pub fn is_enabled(id: &str) -> bool {
        MANAGER.lock().unwrap().installed.get(id).is_some_and(|installed| installed.enabled)
    }

    pub fn has_process(id: &str) -> bool {
        MANAGER.lock().unwrap().running.get(id).is_some_and(|running| running.process.is_some())
    }

    pub fn is_refreshing() -> bool {
        MANAGER.lock().unwrap().refreshing
    }

    /// 清单（索引里的，没有就装着的 power.json）
    pub fn manifest(id: &str) -> Option<Spec> {
        MANAGER.lock().unwrap().manifest(id)
    }

    /// 装着的 Power 能不能做这件事（清单的 capabilities，如 voiceImport）
    pub fn can(id: &str, capability: &str) -> bool {
        MANAGER.lock().unwrap().installed.get(id).is_some_and(|installed| installed.capabilities.iter().any(|item| item == capability))
    }

    fn user_voices(id: &str) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(models_dir(id).join("user-voices"))
            .into_iter()
            .flatten()
            .flatten()
            .filter_map(|entry| {
                let name = entry.file_name().to_string_lossy().into_owned();
                // 点开头的是本地语音正在写的临时文件
                (!name.starts_with('.')).then(|| name.strip_suffix(".gguf").map(str::to_string)).flatten()
            })
            .collect();
        names.sort();
        names
    }

    fn voices(manager: &Manager, id: &str) -> Vec<String> {
        let Some(installed) = manager.installed.get(id) else { return Vec::new() };
        let mut names: Vec<String> = std::fs::read_dir(installed.dir.join("voices"))
            .into_iter()
            .flatten()
            .flatten()
            .filter_map(|entry| entry.file_name().to_string_lossy().strip_suffix(".gguf").map(str::to_string))
            .collect();
        names.sort();
        // 用户的排在后面；和包里的重名的不加载，也不列
        for name in user_voices(id) {
            if !names.contains(&name) {
                names.push(name);
            }
        }
        names
    }

    /// PowersState（同 Friday 的 RemoteServer::powersJson）
    pub fn state() -> Value {
        let manager = MANAGER.lock().unwrap();
        let mut ids: Vec<String> = manager.index.iter().map(|spec| spec.id.clone()).collect();
        for id in manager.installed.keys() {
            if !ids.contains(id) {
                ids.push(id.clone());
            }
        }
        let mut list = Vec::new();
        for id in ids {
            let latest = manager.spec(&id);
            let installed = manager.installed.get(&id);
            let empty = Running::default();
            let running = manager.running.get(&id).unwrap_or(&empty);
            let targets: Vec<Value> = latest
                .map(|latest| {
                    manager
                        .platforms
                        .iter()
                        .filter_map(|platform| latest.target(platform).filter(|target| manager.usable(target)))
                        .map(|target| {
                            json!({ "id": target.id, "title": platform_title(&target.id), "detail": target.detail,
                                "size": target.size + manager.runtime_download_size(target) })
                        })
                        .collect()
                })
                .unwrap_or_default();
            // 这台机器用不了的不列
            if installed.is_none() && targets.is_empty() {
                continue;
            }
            let target = match (installed, latest) {
                (Some(installed), _) => installed.target.clone(),
                (None, Some(latest)) => manager.default_target(latest),
                _ => String::new(),
            };
            let mut download_size = 0u64;
            if let Some(latest) = latest {
                let next = if installed.is_some() && latest.target(&target).is_none() {
                    successor(&target).unwrap_or("").to_string()
                } else {
                    target.clone()
                };
                if let Some(chosen) = latest.target(&next).filter(|chosen| manager.usable(chosen)) {
                    if installed.is_none_or(|installed| newer(&latest.version, &installed.version)) {
                        download_size += chosen.size + manager.runtime_download_size(chosen);
                    }
                    download_size += latest
                        .models_for(chosen)
                        .iter()
                        .filter(|model| !model_ready(&models_dir(&id).join(&model.file), model))
                        .map(|model| model.size)
                        .sum::<u64>();
                }
            }
            let options: Vec<Value> = installed
                .map(|installed| {
                    installed
                        .options
                        .iter()
                        .map(|option| {
                            let mut item = option.clone();
                            if let Some(object) = item.as_object_mut() {
                                object.remove("env");
                                object.insert("value".into(), option_value(option, &installed.option_values));
                            }
                            item
                        })
                        .collect()
                })
                .unwrap_or_default();
            let status = match running.status {
                Status::Downloading => "downloading",
                Status::Starting => "starting",
                Status::Running | Status::Stopping => "running",
                Status::Failed => "failed",
                Status::Idle if installed.is_some() => "installed",
                Status::Idle => "notInstalled",
            };
            let status_text = match status {
                "downloading" => running.progress_text.clone(),
                "starting" => "正在启动".into(),
                "running" if running.status == Status::Stopping => "正在停止".into(),
                "running" => "运行中".into(),
                "failed" => running.error.clone(),
                "installed" if installed.is_some_and(|installed| installed.enabled) => "已安装，用到时自动启动".into(),
                "installed" => "已关闭".into(),
                _ => "未安装".into(),
            };
            list.push(json!({
                "id": id,
                "title": latest.map(|latest| latest.title.clone()).or_else(|| installed.map(|installed| installed.title.clone())).unwrap_or_default(),
                "detail": latest.map(|latest| latest.detail.clone()).or_else(|| installed.map(|installed| installed.detail.clone())).unwrap_or_default(),
                "latestVersion": latest.map(|latest| latest.version.clone()).unwrap_or_default(),
                "installedVersion": installed.map(|installed| installed.version.clone()).unwrap_or_default(),
                "target": target,
                "targetTitle": platform_title(&target),
                "targets": targets,
                "status": status,
                "statusText": status_text,
                "progress": running.progress,
                "error": running.error,
                "enabled": installed.is_none_or(|installed| installed.enabled),
                "options": options,
                "updateAvailable": installed.is_some_and(|installed| latest.is_some_and(|latest| newer(&latest.version, &installed.version))),
                "capabilities": installed.map(|installed| installed.capabilities.clone()).or_else(|| latest.map(|latest| latest.capabilities.clone())).unwrap_or_default(),
                "downloadSize": download_size,
                "downloadSizeText": if download_size > 0 { human_size(download_size) } else { String::new() },
                "voices": voices(&manager, &id),
                "userVoices": if installed.is_some() { user_voices(&id) } else { Vec::new() },
            }));
        }
        json!({
            "powers": list,
            "refreshing": manager.refreshing,
            "indexError": manager.index_error,
            "platformTargets": manager.platforms,
            "available": true,
        })
    }

    // MARK: 用户音色（本地语音 0.5，Friday 的 POWERS.md §4.2）

    fn voice_error(status: u16, body: &[u8]) -> String {
        let message = serde_json::from_slice::<Value>(body)
            .ok()
            .and_then(|value| value["error"]["message"].as_str().map(str::to_string))
            .unwrap_or_default();
        if !message.is_empty() {
            message
        } else if status == 404 || status == 405 {
            "本地语音要更新到 0.5 才能导入、克隆音色（设置 › 能力）".into()
        } else {
            format!("本地语音没做成（HTTP {status}）：{}", String::from_utf8_lossy(&body[..body.len().min(300)]))
        }
    }

    /// 导入（.gguf）或克隆（录音）一个用户音色：(音色名, 听写出来的底稿)
    pub fn import_voice(path: &Path, name: &str, text: &str, replace: bool) -> Result<(String, String), String> {
        let gguf = path.extension().is_some_and(|extension| extension.eq_ignore_ascii_case("gguf"));
        let wanted = if gguf { "voiceImport" } else { "voiceCloning" };
        if !is_installed("voice") {
            return Err("还没装本地语音（设置 › 能力）".into());
        }
        if !is_enabled("voice") {
            return Err("本地语音关着（设置 › 能力）".into());
        }
        if !can("voice", wanted) {
            return Err(format!("本地语音要更新到 0.5 才能{}（设置 › 能力）", if gguf { "导入音色" } else { "克隆音色" }));
        }
        let data = std::fs::read(path).map_err(|_| format!("打不开文件：{}", path.display()))?;
        let (url, token) = ensure_running("voice", START_TIMEOUT)?;
        let boundary = format!("Friday-{}", random_token());
        let mut body: Vec<u8> = Vec::new();
        let mut field = |key: &str, value: &[u8]| {
            body.extend_from_slice(format!("--{boundary}\r\nContent-Disposition: form-data; name=\"{key}\"\r\n\r\n").as_bytes());
            body.extend_from_slice(value);
            body.extend_from_slice(b"\r\n");
        };
        field("name", name.as_bytes());
        if replace {
            field("replace", b"true");
        }
        if !gguf && !text.trim().is_empty() {
            field("text", text.trim().as_bytes());
        }
        let (file, kind) = if gguf { ("voice.gguf", "application/octet-stream") } else { ("audio.wav", "audio/wav") };
        body.extend_from_slice(
            format!("--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"{file}\"\r\nContent-Type: {kind}\r\n\r\n")
                .as_bytes(),
        );
        body.extend_from_slice(&data);
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
        let endpoint = format!("{url}{}", if gguf { "/v1/voice/import" } else { "/v1/voice/clone" });
        let mut response = crate::http_agent()
            .post(&endpoint)
            .header("Content-Type", format!("multipart/form-data; boundary={boundary}"))
            .header("Authorization", format!("Bearer {token}"))
            .send(&body[..])
            .map_err(|error| format!("本地语音没做成：{error}"))?;
        let status = response.status().as_u16();
        let mut reply = Vec::new();
        let _ = response.body_mut().as_reader().read_to_end(&mut reply);
        changed();
        if !(200..300).contains(&status) {
            return Err(voice_error(status, &reply));
        }
        let object: Value = serde_json::from_slice(&reply).unwrap_or_default();
        Ok((object["voice"].as_str().unwrap_or(name).to_string(), object["transcript"].as_str().unwrap_or("").to_string()))
    }

    /// 删掉一个用户音色（内置的删不掉）
    pub fn delete_voice(name: &str) -> Result<(), String> {
        if name.is_empty() || name.contains('/') || name.contains('\\') || name.starts_with('.') {
            return Err(format!("没有这个音色：{name}"));
        }
        let running = MANAGER
            .lock()
            .unwrap()
            .running
            .get("voice")
            .is_some_and(|running| running.status == Status::Running && running.process.is_some());
        if !running {
            // 没在跑：直接删文件（下次拉起就没有它了）
            let path = models_dir("voice").join("user-voices").join(format!("{name}.gguf"));
            let result = if !path.exists() {
                Err(format!("没有这个音色：{name}"))
            } else {
                std::fs::remove_file(&path).map_err(|_| format!("删不掉：{}", path.display()))
            };
            changed();
            return result;
        }
        let (url, token) = ensure_running("voice", START_TIMEOUT)?;
        let encoded: String =
            name.bytes()
                .map(|byte| {
                    if byte.is_ascii_alphanumeric() || b"-_.~".contains(&byte) {
                        (byte as char).to_string()
                    } else {
                        format!("%{byte:02X}")
                    }
                })
                .collect();
        let mut response = crate::http_agent()
            .delete(&format!("{url}/v1/voice/{encoded}"))
            .header("Authorization", format!("Bearer {token}"))
            .call()
            .map_err(|error| format!("本地语音没做成：{error}"))?;
        let status = response.status().as_u16();
        let mut reply = Vec::new();
        let _ = response.body_mut().as_reader().read_to_end(&mut reply);
        changed();
        if (200..300).contains(&status) { Ok(()) } else { Err(voice_error(status, &reply)) }
    }

    /// 常驻的那份退出：停掉全部进程
    pub fn stop_all() {
        let processes: Vec<Arc<Mutex<Process>>> = MANAGER
            .lock()
            .unwrap()
            .running
            .values_mut()
            .filter_map(|running| {
                running.stopping = true;
                running.process.take()
            })
            .collect();
        for process in processes {
            let mut process = process.lock().unwrap();
            process.stdin = None;
            std::thread::sleep(Duration::from_millis(200));
            if matches!(process.child.try_wait(), Ok(None)) {
                let _ = process.child.kill();
            }
            let _ = process.child.wait();
        }
    }

    /// 有在下的、在跑的（常驻的那份闲着不退）
    pub fn busy() -> bool {
        MANAGER.lock().unwrap().running.values().any(|running| running.status == Status::Downloading || running.process.is_some())
    }
}

// MARK: - 本地模型（模型库里的 GGUF，在 friday-llm 里跑）

mod models {
    //! 本地模型（照 Friday Qt 的 LocalModels / ModelLibrary / Downloader / ModelScopeDownloader / HubAdapters / GGUF，Friday 仓库的
    //! qt/docs/POWERS.md §8、REMOTE.md §6「本地模型」）：模型库是魔搭 / Hugging Face 自己的缓存布局（`MODELSCOPE_CACHE`、`HF_HUB_CACHE`、
    //! `HF_HOME` 可以换），和这台的 Friday 桌面端共用。`models.use` 没下的先下（friday-llm 没装的先装），下好了在 EDITH 的 config.json
    //! 里建一个本地引擎（baseURL `power://llm`、model「组织/仓库:量化档」），给那段会话换上（没给会话 = 新对话默认）。
    //! 大脑每段请求前问 `model.endpoint`（能力协议 §6.6）：拉起 friday-llm，回它的地址、令牌和 `x_friday`（模型文件、会话、优先级）。

    use std::collections::{HashMap, HashSet};
    use std::io::Read;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, LazyLock, Mutex};
    use std::time::{Duration, SystemTime};

    use regex::Regex;
    use serde_json::{Map, Value, json};
    use sha2::Digest;

    use crate::powers;

    // MARK: 目录

    fn home() -> PathBuf {
        crate::user_home()
    }

    fn env_path(name: &str) -> Option<PathBuf> {
        std::env::var_os(name).filter(|value| !value.is_empty()).map(PathBuf::from)
    }

    fn modelscope_root() -> PathBuf {
        env_path("MODELSCOPE_CACHE").map(|dir| dir.join("models")).unwrap_or_else(|| home().join(".cache/modelscope/hub/models"))
    }

    fn huggingface_root() -> PathBuf {
        env_path("HF_HUB_CACHE")
            .or_else(|| env_path("HF_HOME").map(|dir| dir.join("hub")))
            .unwrap_or_else(|| home().join(".cache/huggingface/hub"))
    }

    /// 模型库的目录（魔搭、HF 两个缓存；friday-llm 的 --models）
    pub fn roots() -> Vec<PathBuf> {
        vec![modelscope_root(), huggingface_root()]
    }

    #[derive(Clone, Copy, PartialEq, Debug)]
    enum Source {
        ModelScope,
        HuggingFace,
    }

    impl Source {
        fn parse(text: &str) -> Option<Source> {
            match text {
                "modelScope" => Some(Source::ModelScope),
                "huggingFace" => Some(Source::HuggingFace),
                _ => None,
            }
        }

        fn raw(self) -> &'static str {
            match self {
                Source::ModelScope => "modelScope",
                Source::HuggingFace => "huggingFace",
            }
        }

        fn other(self) -> Source {
            match self {
                Source::ModelScope => Source::HuggingFace,
                Source::HuggingFace => Source::ModelScope,
            }
        }

        /// 仓库在本机的目录（HF 是 refs/main 指着的快照，没有就第一个）
        fn local_directory(self, repo: &str) -> PathBuf {
            match self {
                Source::ModelScope => modelscope_root().join(repo),
                Source::HuggingFace => {
                    let root = hf_repo_dir(repo);
                    let snapshots = root.join("snapshots");
                    let commit = std::fs::read_to_string(root.join("refs/main")).unwrap_or_default().trim().to_string();
                    if !commit.is_empty() && snapshots.join(&commit).is_dir() {
                        return snapshots.join(commit);
                    }
                    let mut entries: Vec<PathBuf> = std::fs::read_dir(&snapshots)
                        .into_iter()
                        .flatten()
                        .flatten()
                        .map(|entry| entry.path())
                        .filter(|path| path.is_dir())
                        .collect();
                    entries.sort();
                    entries.into_iter().next().unwrap_or_else(|| snapshots.join(if commit.is_empty() { "main".into() } else { commit }))
                }
            }
        }
    }

    fn hf_repo_dir(repo: &str) -> PathBuf {
        huggingface_root().join(format!("models--{}", repo.replace('/', "--")))
    }

    /// 默认的下载源（`~/.edith/friday/settings.json` 的 downloadSource，没有用魔搭）
    fn default_source() -> Source {
        crate::read_json_file(&crate::serve_dir().join("settings.json"))["downloadSource"]
            .as_str()
            .and_then(Source::parse)
            .unwrap_or(Source::ModelScope)
    }

    // MARK: id

    fn repo_of(id: &str) -> String {
        id.split_once(':').map_or(id, |(repo, _)| repo).trim().to_string()
    }

    fn quant_of(id: &str) -> String {
        id.split_once(':').map(|(_, quant)| quant.trim().to_uppercase()).unwrap_or_default()
    }

    fn make_id(repo: &str, quant: &str) -> String {
        if quant.is_empty() { repo.to_string() } else { format!("{repo}:{quant}") }
    }

    fn safe_relative(path: &str) -> bool {
        !path.is_empty()
            && !path.starts_with('/')
            && !path.chars().any(|c| c == '\\' || c == ':' || (c as u32) < 0x20 || c as u32 == 0x7f)
            && path.split('/').all(|segment| !segment.is_empty() && segment != "." && segment != "..")
    }

    fn valid_repo(repo: &str) -> bool {
        safe_relative(repo) && repo.matches('/').count() == 1 && !repo.contains(' ')
    }

    // MARK: GGUF 文件名（照 llama.cpp common/download.cpp）

    #[derive(Clone, Debug)]
    struct RemoteFile {
        path: String,
        size: u64,
        sha256: String,
    }

    struct SplitInfo {
        prefix: String,
        tag: String,
        index: u32,
        count: u32,
    }

    static SPLIT: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)^(.+)-([0-9]{5})-of-([0-9]{5})$").unwrap());
    static TAG: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)[-.]([A-Z0-9_]+)$").unwrap());
    static QUANT: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"^(?:I?Q[1-8](?:_[0-9A-Z]+)*|TQ[1-3]_[0-9]|F(?:P)?(?:16|32|64)|BF16|FP8|MXFP4(?:_MOE)?|NVFP4)$").unwrap()
    });

    fn file_name_of(path: &str) -> &str {
        path.rsplit('/').next().unwrap_or(path)
    }

    fn is_gguf(path: &str) -> bool {
        path.to_lowercase().ends_with(".gguf")
    }

    fn split_info(path: &str) -> SplitInfo {
        let mut info = SplitInfo { prefix: String::new(), tag: String::new(), index: 1, count: 1 };
        if !is_gguf(path) {
            return info;
        }
        info.prefix = path[..path.len() - 5].to_string();
        if let Some(split) = SPLIT.captures(&info.prefix.clone()) {
            info.index = split[2].parse().unwrap_or(1);
            info.count = split[3].parse().unwrap_or(1);
            info.prefix = split[1].to_string();
        }
        if let Some(tag) = TAG.captures(&info.prefix) {
            info.tag = tag[1].to_uppercase();
        }
        info
    }

    fn quant_from_file(path: &str) -> String {
        let tag = split_info(file_name_of(path)).tag;
        if QUANT.is_match(&tag) { tag } else { String::new() }
    }

    fn bits_of_tag(tag: &str) -> i64 {
        let digits: String = tag.chars().skip_while(|c| !c.is_ascii_digit()).take_while(char::is_ascii_digit).collect();
        digits.parse().unwrap_or(0)
    }

    fn is_mmproj(path: &str) -> bool {
        file_name_of(path).to_lowercase().contains("mmproj")
    }

    fn is_model_file(path: &str) -> bool {
        if !is_gguf(path) {
            return false;
        }
        let name = file_name_of(path).to_lowercase();
        !["mmproj", "imatrix", "mtp-", "eagle3-", "dflash-", "dspark-"].iter().any(|keyword| name.contains(keyword))
    }

    fn is_first_piece(path: &str) -> bool {
        let info = split_info(path);
        !(info.count > 1 && info.index != 1)
    }

    /// 同 prefix、同分片数的全部分片（按序号）
    fn split_files(files: &[RemoteFile], file: &RemoteFile) -> Vec<RemoteFile> {
        let split = split_info(&file.path);
        if split.count <= 1 {
            return vec![file.clone()];
        }
        let mut found: Vec<(u32, RemoteFile)> = files
            .iter()
            .filter_map(|candidate| {
                let info = split_info(&candidate.path);
                (info.count == split.count && info.prefix == split.prefix).then(|| (info.index, candidate.clone()))
            })
            .collect();
        found.sort_by_key(|(index, _)| *index);
        found.dedup_by_key(|(index, _)| *index);
        found.into_iter().map(|(_, file)| file).collect()
    }

    fn mmproj_rank(path: &str) -> i32 {
        match split_info(path).tag.as_str() {
            "F16" | "FP16" => 0,
            "BF16" => 1,
            "F32" | "FP32" => 2,
            _ => 3,
        }
    }

    /// 视觉投影：在主模型目录或它的上级、位数最接近的（同 llama.cpp 的 find_best_mmproj）
    fn best_mmproj(files: &[RemoteFile], model_path: &str) -> Option<RemoteFile> {
        let model_parts: Vec<&str> = model_path.split('/').collect();
        let model_dirs = model_parts.len() - 1;
        let model_bits = bits_of_tag(&split_info(model_path).tag);
        let mut best: Option<(RemoteFile, usize, i64, i32)> = None;
        for file in files.iter().filter(|file| is_gguf(&file.path) && is_mmproj(&file.path)) {
            let parts: Vec<&str> = file.path.split('/').collect();
            let dirs = parts.len() - 1;
            if dirs > model_dirs || (0..dirs).any(|index| parts[index] != model_parts[index]) {
                continue;
            }
            let diff = (bits_of_tag(&split_info(&file.path).tag) - model_bits).abs();
            let rank = mmproj_rank(&file.path);
            let better = match &best {
                None => true,
                Some((_, best_dirs, best_diff, best_rank)) => {
                    dirs > *best_dirs
                        || (dirs == *best_dirs && diff < *best_diff)
                        || (dirs == *best_dirs && diff == *best_diff && rank < *best_rank)
                }
            };
            if better {
                best = Some((file.clone(), dirs, diff, rank));
            }
        }
        best.map(|(file, ..)| file)
    }

    #[derive(Default, Clone)]
    struct Selection {
        model_file: String,
        model_files: Vec<String>,
        model_bytes: u64,
        quant: String,
        mmproj_file: String,
        mmproj_bytes: u64,
    }

    fn select_file(files: &[RemoteFile], model_path: &str) -> Option<Selection> {
        let file = files.iter().find(|file| file.path == model_path)?;
        let group = split_files(files, file);
        let model_file = group.first().map_or(file.path.clone(), |first| first.path.clone());
        let mut selection = Selection {
            quant: quant_from_file(&model_file),
            model_files: group.iter().map(|file| file.path.clone()).collect(),
            model_bytes: group.iter().map(|file| file.size).sum(),
            model_file,
            ..Default::default()
        };
        if let Some(mmproj) = best_mmproj(files, &selection.model_file) {
            selection.mmproj_file = mmproj.path;
            selection.mmproj_bytes = mmproj.size;
        }
        Some(selection)
    }

    /// 按量化档挑主模型（不给时 Q4_K_M → Q4_0 → Q5_K_M → Q8_0，都没有挑最小的）
    fn select(files: &[RemoteFile], quant: &str) -> Option<Selection> {
        let quant = quant.trim().to_uppercase();
        let candidates: Vec<&RemoteFile> = files.iter().filter(|file| is_model_file(&file.path) && is_first_piece(&file.path)).collect();
        let find = |tag: &str| -> Option<&RemoteFile> {
            candidates.iter().copied().find(|file| split_info(&file.path).tag == tag).or_else(|| {
                let pattern = Regex::new(&format!("(?i){}[.-]", regex::escape(tag))).ok()?;
                candidates.iter().copied().find(|file| pattern.is_match(&file.path))
            })
        };
        let chosen = if !quant.is_empty() {
            find(&quant)
        } else {
            ["Q4_K_M", "Q4_0", "Q5_K_M", "Q8_0"].iter().find_map(|tag| find(tag)).or_else(|| {
                candidates.iter().copied().min_by_key(|file| split_files(files, file).iter().map(|file| file.size).sum::<u64>())
            })
        }?;
        select_file(files, &chosen.path)
    }

    /// GGUF 头里的 general.architecture（读到为止）
    fn architecture(path: &Path) -> Option<String> {
        let mut file = std::io::BufReader::with_capacity(1 << 20, std::fs::File::open(path).ok()?);
        let mut read = |length: usize| -> Option<Vec<u8>> {
            let mut buffer = vec![0u8; length];
            file.read_exact(&mut buffer).ok()?;
            Some(buffer)
        };
        let u32_of = |bytes: Vec<u8>| u32::from_le_bytes(bytes.try_into().unwrap_or([0; 4]));
        let u64_of = |bytes: Vec<u8>| u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8]));
        if read(4)? != b"GGUF" {
            return None;
        }
        let version = u32_of(read(4)?);
        if version < 2 {
            return None;
        }
        let _tensors = u64_of(read(8)?);
        let count = u64_of(read(8)?).min(100_000);
        fn scalar(kind: u32) -> usize {
            match kind {
                0 | 1 | 7 => 1,
                2 | 3 => 2,
                4..=6 => 4,
                10..=12 => 8,
                _ => 0,
            }
        }
        for _ in 0..count {
            let key_length = u64_of(read(8)?) as usize;
            if key_length > 4096 {
                return None;
            }
            let key = String::from_utf8_lossy(&read(key_length)?).into_owned();
            let kind = u32_of(read(4)?);
            match kind {
                8 => {
                    let length = u64_of(read(8)?) as usize;
                    if length > 1 << 30 {
                        return None;
                    }
                    let value = read(length)?;
                    if key == "general.architecture" {
                        return Some(String::from_utf8_lossy(&value).into_owned());
                    }
                }
                9 => {
                    let element = u32_of(read(4)?);
                    let items = u64_of(read(8)?);
                    if element == 8 {
                        for _ in 0..items {
                            let length = u64_of(read(8)?) as usize;
                            read(length)?;
                        }
                    } else if scalar(element) > 0 {
                        read(scalar(element) * items as usize)?;
                    } else {
                        return None;
                    }
                }
                other if scalar(other) > 0 => {
                    read(scalar(other))?;
                }
                _ => return None,
            }
        }
        None
    }

    // MARK: 模型库（本机的）

    #[derive(Clone, PartialEq)]
    struct LocalModel {
        id: String,
        source: Source,
        directory: PathBuf,
        files: Vec<PathBuf>,
        model_file: PathBuf,
        mmproj_file: Option<PathBuf>,
        size_bytes: u64,
        model_type: String,
        modified: SystemTime,
    }

    impl LocalModel {
        fn name(&self) -> String {
            static SHARD: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)-[0-9]{5}-of-[0-9]{5}$").unwrap());
            let file = self.model_file.file_name().map(|name| name.to_string_lossy().into_owned()).unwrap_or_default();
            let base = file.strip_suffix(".gguf").or_else(|| file.strip_suffix(".GGUF")).unwrap_or(&file);
            let base = SHARD.replace(base, "").into_owned();
            if base.is_empty() { self.id.rsplit('/').next().unwrap_or(&self.id).to_string() } else { base }
        }

        /// LocalModelInfo（Friday 的 RemoteProtocol::localModelToJson）
        fn to_json(&self, kind: &str) -> Value {
            let modified: chrono::DateTime<chrono::Utc> = self.modified.into();
            json!({
                "id": self.id,
                "source": self.source.raw(),
                "name": self.name(),
                "quant": quant_of(&self.id),
                "sizeBytes": self.size_bytes,
                "modelType": self.model_type,
                "hasVision": self.mmproj_file.is_some(),
                "kind": kind,
                "modelFile": self.model_file.to_string_lossy(),
                "mmprojFile": self.mmproj_file.as_ref().map(|path| path.to_string_lossy().into_owned()),
                "directory": self.directory.to_string_lossy(),
                "modified": modified.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            })
        }
    }

    /// 目录里的文件（相对路径；跳过点开头的、正在下的 .part / 没校验的 .incomplete）
    fn scan_directory(directory: &Path) -> Vec<RemoteFile> {
        let mut result = Vec::new();
        let mut stack = vec![directory.to_path_buf()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
                let path = entry.path();
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.starts_with('.') {
                    continue;
                }
                // HF 快照里是指向 blobs 的符号链接：按目标算
                let Ok(meta) = std::fs::metadata(&path) else { continue };
                if meta.is_dir() {
                    stack.push(path);
                } else if !name.ends_with(".part") && !name.ends_with(".incomplete") {
                    let relative =
                        path.strip_prefix(directory).map(|relative| relative.to_string_lossy().replace('\\', "/")).unwrap_or_default();
                    result.push(RemoteFile { path: relative, size: meta.len(), sha256: String::new() });
                }
            }
        }
        result.sort_by(|a, b| a.path.cmp(&b.path));
        result
    }

    /// 还在下的（有 .part / .incomplete 的）
    fn partial_files(directory: &Path) -> HashSet<String> {
        let mut result = HashSet::new();
        let mut stack = vec![directory.to_path_buf()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                let relative =
                    path.strip_prefix(directory).map(|relative| relative.to_string_lossy().replace('\\', "/")).unwrap_or_default();
                for suffix in [".incomplete.part", ".part", ".incomplete"] {
                    if let Some(stripped) = relative.strip_suffix(suffix) {
                        result.insert(stripped.to_string());
                        break;
                    }
                }
            }
        }
        result
    }

    /// 一个仓库目录里的本地模型：每个量化档（分片要齐全、没有在下的）一个
    fn inspect(repo: &str, source: Source, weights: &Path, remove_at: &Path) -> Vec<LocalModel> {
        let files = scan_directory(weights);
        if files.is_empty() {
            return Vec::new();
        }
        let partial = partial_files(weights);
        let models: Vec<&RemoteFile> = files.iter().filter(|file| is_model_file(&file.path) && is_first_piece(&file.path)).collect();
        let mmprojs: Vec<PathBuf> =
            files.iter().filter(|file| is_gguf(&file.path) && is_mmproj(&file.path)).map(|file| weights.join(&file.path)).collect();
        let mut seen = HashSet::new();
        let mut result = Vec::new();
        for candidate in &models {
            let Some(selection) = select_file(&files, &candidate.path) else { continue };
            if selection.model_files.len() as u32 != split_info(&selection.model_file).count.max(1) {
                continue; // 分片不齐
            }
            if selection.model_files.iter().any(|path| partial.contains(path)) {
                continue; // 还在下载
            }
            let id = make_id(repo, &selection.quant);
            if !seen.insert(id.clone()) {
                continue;
            }
            let mut model_files: Vec<PathBuf> = selection.model_files.iter().map(|path| weights.join(path)).collect();
            let mmproj = (!selection.mmproj_file.is_empty()).then(|| weights.join(&selection.mmproj_file));
            // 仓库里唯一的量化档：删除时连视觉投影一起删
            if models.len() == 1 {
                model_files.extend(mmprojs.iter().cloned());
            }
            let model_file = weights.join(&selection.model_file);
            let modified = model_files
                .iter()
                .chain(mmproj.iter())
                .filter_map(|path| std::fs::metadata(path).and_then(|meta| meta.modified()).ok())
                .max()
                .unwrap_or(SystemTime::UNIX_EPOCH);
            result.push(LocalModel {
                id,
                source,
                directory: remove_at.to_path_buf(),
                model_type: architecture(&model_file).unwrap_or_default(),
                model_file,
                mmproj_file: mmproj,
                files: model_files,
                size_bytes: selection.model_bytes + selection.mmproj_bytes,
                modified,
            });
        }
        result
    }

    fn scan_local() -> Vec<LocalModel> {
        let mut result = Vec::new();
        // 魔搭：<根>/<组织>/<仓库>
        let root = modelscope_root();
        for org in sorted_dirs(&root) {
            for repo in sorted_dirs(&root.join(&org)) {
                let dir = root.join(&org).join(&repo);
                result.extend(inspect(&format!("{org}/{repo}"), Source::ModelScope, &dir, &dir));
            }
        }
        // Hugging Face：<根>/models--组织--仓库/snapshots/<提交>
        for entry in sorted_dirs(&huggingface_root()) {
            let Some(rest) = entry.strip_prefix("models--") else { continue };
            let Some((org, repo)) = rest.split_once("--") else { continue };
            let id = format!("{org}/{repo}");
            let snapshot = Source::HuggingFace.local_directory(&id);
            if snapshot.is_dir() {
                result.extend(inspect(&id, Source::HuggingFace, &snapshot, &hf_repo_dir(&id)));
            }
        }
        result.sort_by_key(|model| std::cmp::Reverse(model.modified));
        result
    }

    fn sorted_dirs(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .into_iter()
            .flatten()
            .flatten()
            .filter(|entry| entry.path().is_dir())
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| !name.starts_with('.'))
            .collect();
        names.sort();
        names
    }

    /// id 带量化档时要完全一致；不带时同一个仓库的任一量化档都算
    fn id_matches(model: &LocalModel, id: &str) -> bool {
        if !repo_of(&model.id).eq_ignore_ascii_case(&repo_of(id)) {
            return false;
        }
        let quant = quant_of(id);
        quant.is_empty() || quant_of(&model.id) == quant
    }

    /// 本机的这一份（魔搭优先）
    fn local_copy(id: &str) -> Option<LocalModel> {
        let repo = repo_of(id);
        if !valid_repo(&repo) {
            return None;
        }
        let ms = modelscope_root().join(&repo);
        if let Some(found) = inspect(&repo, Source::ModelScope, &ms, &ms).into_iter().find(|model| id_matches(model, id)) {
            return Some(found);
        }
        let snapshot = Source::HuggingFace.local_directory(&repo);
        if !snapshot.is_dir() {
            return None;
        }
        inspect(&repo, Source::HuggingFace, &snapshot, &hf_repo_dir(&repo)).into_iter().find(|model| id_matches(model, id))
    }

    // MARK: 下载（魔搭 / Hugging Face 的接口）

    fn hf_endpoint() -> String {
        std::env::var("HF_ENDPOINT")
            .ok()
            .map(|value| value.trim().trim_end_matches('/').to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "https://huggingface.co".into())
    }

    fn hf_token() -> String {
        std::env::var("HF_TOKEN").unwrap_or_default().trim().to_string()
    }

    fn get_json(url: &str, bearer: &str) -> Result<(u16, Value, String), String> {
        let mut request = crate::http_agent().get(url).header("User-Agent", format!("Friday/{}", crate::VERSION));
        if !bearer.is_empty() {
            request = request.header("Authorization", format!("Bearer {bearer}"));
        }
        let mut response = request.call().map_err(|error| error.to_string())?;
        let status = response.status().as_u16();
        let link = response.headers().get("Link").and_then(|value| value.to_str().ok()).unwrap_or("").to_string();
        let mut body = Vec::new();
        response.body_mut().as_reader().read_to_end(&mut body).map_err(|error| error.to_string())?;
        Ok((status, serde_json::from_slice(&body).unwrap_or(Value::Null), link))
    }

    /// 仓库里的文件（魔搭的 master 分支、HF 的 main）
    fn list_files(source: Source, repo: &str) -> Result<Vec<RemoteFile>, String> {
        if !valid_repo(repo) {
            return Err(format!("模型 id 不对：{repo}"));
        }
        let mut result = Vec::new();
        match source {
            Source::ModelScope => {
                let url = format!("https://modelscope.cn/api/v1/models/{repo}/repo/files?Revision=master&Recursive=true");
                let (status, body, _) = get_json(&url, "")?;
                let files = body["Data"]["Files"].as_array().cloned();
                let Some(files) = files.filter(|_| status == 200) else {
                    let reason = body["Message"]
                        .as_str()
                        .filter(|message| !message.is_empty())
                        .map(str::to_string)
                        .unwrap_or_else(|| format!("HTTP {status}"));
                    return Err(format!("魔搭上找不到模型 {repo}（{reason}）"));
                };
                for file in files.iter().filter(|file| file["Type"] == "blob") {
                    let remote = RemoteFile {
                        path: file["Path"].as_str().unwrap_or("").to_string(),
                        size: file["Size"].as_u64().unwrap_or(0),
                        sha256: file["Sha256"].as_str().unwrap_or("").to_lowercase(),
                    };
                    if safe_relative(&remote.path) {
                        result.push(remote);
                    }
                }
            }
            Source::HuggingFace => {
                static NEXT: LazyLock<Regex> = LazyLock::new(|| Regex::new(r#"<([^>]+)>;\s*rel="next""#).unwrap());
                let mut url = format!("{}/api/models/{repo}/tree/main?recursive=true", hf_endpoint());
                for _ in 0..100 {
                    let (status, body, link) = get_json(&url, &hf_token())?;
                    if status != 200 {
                        let detail = body["error"].as_str().map(str::to_string).unwrap_or_else(|| format!("HTTP {status}"));
                        return Err(format!("Hugging Face 上找不到模型 {repo}（{detail}）"));
                    }
                    for item in body.as_array().into_iter().flatten().filter(|item| item["type"] == "file") {
                        let mut file = RemoteFile {
                            path: item["path"].as_str().unwrap_or("").to_string(),
                            size: item["size"].as_u64().unwrap_or(0),
                            sha256: String::new(),
                        };
                        if item["lfs"].is_object() {
                            file.sha256 = item["lfs"]["oid"].as_str().unwrap_or("").to_lowercase();
                            file.size = item["lfs"]["size"].as_u64().unwrap_or(file.size);
                        }
                        if safe_relative(&file.path) {
                            result.push(file);
                        }
                    }
                    match NEXT.captures(&link) {
                        Some(next) => url = next[1].to_string(),
                        None => break,
                    }
                }
            }
        }
        Ok(result)
    }

    /// 下仓库里的这几个文件（已经有、大小对的跳过；有 SHA-256 的先下到 .incomplete、校验过再改名）。progress：这几个文件已下的字节
    fn download(source: Source, repo: &str, wanted: &[String], cancel: &AtomicBool, progress: &mut dyn FnMut(u64)) -> Result<(), String> {
        let files = list_files(source, repo)?;
        let selected: Vec<RemoteFile> =
            files.iter().filter(|file| wanted.iter().any(|path| path.eq_ignore_ascii_case(&file.path))).cloned().collect();
        if selected.is_empty() {
            return Err(format!("仓库 {repo} 里没有要下的文件"));
        }
        let directory = match source {
            Source::ModelScope => modelscope_root().join(repo),
            Source::HuggingFace => {
                // 快照目录：提交 sha 不好拿时沿用本地的 refs/main 或 main（Friday 的 HubDownloader 用第一跳的 X-Repo-Commit）
                let root = hf_repo_dir(repo);
                let commit = std::fs::read_to_string(root.join("refs/main"))
                    .ok()
                    .map(|text| text.trim().to_string())
                    .filter(|commit| safe_relative(commit) && !commit.contains('/'))
                    .unwrap_or_else(|| "main".into());
                let _ = std::fs::create_dir_all(root.join("refs"));
                let _ = std::fs::write(root.join("refs/main"), &commit);
                root.join("snapshots").join(commit)
            }
        };
        let mut completed = 0u64;
        for file in &selected {
            let destination = directory.join(&file.path);
            if std::fs::metadata(&destination).is_ok_and(|meta| meta.len() == file.size) {
                completed += file.size;
                progress(completed);
                continue;
            }
            let url = match source {
                Source::ModelScope => format!("https://modelscope.cn/models/{repo}/resolve/master/{}", file.path),
                Source::HuggingFace => format!("{}/{repo}/resolve/main/{}", hf_endpoint(), file.path),
            };
            let base = completed;
            let staging =
                if file.sha256.is_empty() { destination.clone() } else { PathBuf::from(format!("{}.incomplete", destination.display())) };
            powers::fetch(&url, &staging, cancel, &mut |written| progress(base + written))?;
            if !file.sha256.is_empty() {
                let actual = powers::sha256_of(&staging, cancel)?;
                if !actual.eq_ignore_ascii_case(&file.sha256) {
                    let _ = std::fs::remove_file(&staging);
                    return Err(format!("{} 下载后校验失败（SHA-256 不一致），已删除，请重试", file_name_of(&file.path)));
                }
                let _ = std::fs::remove_file(&destination);
                std::fs::rename(&staging, &destination).map_err(|error| format!("无法把 {} 改名：{error}", staging.display()))?;
            }
            completed = base + file.size;
            progress(completed);
        }
        Ok(())
    }

    // MARK: 生图 / 生视频的模型家族（friday-diffusion 清单的 families）

    #[derive(Clone)]
    struct Component {
        role: String,
        repo: String,
        file: String,
        size: u64,
    }

    #[derive(Clone)]
    struct Family {
        id: String,
        kind: String,
        /// 主模型在 x_friday.files 里的键（缺省 diffusion_model）
        main_role: String,
        /// 能按参考图改图（FLUX.2 klein、Qwen-Image-Edit 这类）
        edits: bool,
        patterns: Vec<Regex>,
        recommended_repo: String,
        recommended_file: String,
        components: Vec<Component>,
    }

    fn families() -> Vec<Family> {
        let Some(spec) = powers::manifest("diffusion") else { return Vec::new() };
        spec.families
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|item| {
                let kind = item["kind"].as_str().unwrap_or("").to_string();
                let patterns: Vec<Regex> = item["match"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .filter(|text| !text.is_empty())
                    .filter_map(|text| Regex::new(&format!("(?i){text}")).ok())
                    .collect();
                let components: Option<Vec<Component>> = item["components"]
                    .as_object()
                    .into_iter()
                    .flatten()
                    .map(|(role, component)| {
                        let component = Component {
                            role: role.clone(),
                            repo: component["repo"].as_str().unwrap_or("").to_string(),
                            file: component["file"].as_str().unwrap_or("").to_string(),
                            size: component["size"].as_f64().unwrap_or(0.0) as u64,
                        };
                        (!component.repo.is_empty() && safe_relative(&component.file)).then_some(component)
                    })
                    .collect();
                let family = Family {
                    id: item["id"].as_str().unwrap_or("").to_string(),
                    main_role: item["main"].as_str().unwrap_or("diffusion_model").to_string(),
                    edits: item["edits"].as_bool().unwrap_or(false),
                    recommended_repo: item["recommended"]["repo"].as_str().unwrap_or("").to_string(),
                    recommended_file: item["recommended"]["file"].as_str().unwrap_or("").to_string(),
                    components: components?,
                    patterns,
                    kind,
                };
                (item["id"].is_string() && matches!(family.kind.as_str(), "image" | "video") && !family.patterns.is_empty())
                    .then_some(family)
            })
            .collect()
    }

    fn family_for(id: &str) -> Option<Family> {
        let repo = repo_of(id);
        families().into_iter().find(|family| family.patterns.iter().any(|pattern| pattern.is_match(&repo) || pattern.is_match(id)))
    }

    /// 认不出家族的生图 / 生视频仓库、配套文件、向量模型：不能拿来对话
    fn looks_non_chat(name: &str) -> bool {
        static DIFFUSION: LazyLock<Regex> = LazyLock::new(|| {
            Regex::new(r"comfy|stable[-_]?diffusion|sdxl|\bsd[-_]?3|flux|wan[-_]?2|ltx[-_]?(video|2)|hunyuan[-_]?video|z[-_]?image|qwen[-_]?image|hidream|chroma|cogvideo|mochi|minimax[-_]?h|kolors|pixart|lumina|umt5|t5[-_]?xxl|t5[-_]?encoder|vae").unwrap()
        });
        let lower = name.to_lowercase();
        DIFFUSION.is_match(&lower) || ["embedding", "rerank", "-embed"].iter().any(|marker| lower.contains(marker))
    }

    fn kind_of(id: &str, task: &str) -> String {
        if let Some(family) = family_for(id) {
            return family.kind;
        }
        if looks_non_chat(&repo_of(id)) {
            return "other".into();
        }
        if task == "image-text-to-text" || task == "multimodal" { "multimodal".into() } else { "text".into() }
    }

    /// 配套文件在本机哪里（两个缓存都看）
    fn component_path(component: &Component) -> Option<PathBuf> {
        [Source::ModelScope, Source::HuggingFace]
            .into_iter()
            .map(|source| source.local_directory(&component.repo).join(&component.file))
            .find(|path| std::fs::metadata(path).is_ok_and(|meta| meta.is_file() && (component.size == 0 || meta.len() == component.size)))
    }

    /// 这台本地生图 / 生视频用的模型（模型库里「使用」的那个；~/.edith/friday/settings.json）
    pub fn selected(what: &str) -> String {
        let settings = crate::read_json_file(&crate::serve_dir().join("settings.json"));
        settings[if what == "video" { "localVideoModel" } else { "localImageModel" }].as_str().unwrap_or("").to_string()
    }

    /// 交给 friday-diffusion 的 x_friday：{family, files: {角色: 路径}}；文件没下齐是 None
    pub fn diffusion_request(model: &str) -> Option<Value> {
        let family = family_for(model)?;
        let local = local_copy(model)?;
        let mut files = Map::new();
        files.insert(family.main_role.clone(), json!(local.model_file.to_string_lossy()));
        for component in &family.components {
            files.insert(component.role.clone(), json!(component_path(component)?.to_string_lossy()));
        }
        Some(json!({ "family": family.id, "files": files }))
    }

    /// 这个模型的家族能按参考图改图
    pub fn family_edits(model: &str) -> bool {
        family_for(model).is_some_and(|family| family.edits)
    }

    fn is_complete(id: &str, kind: &str) -> bool {
        if local_copy(id).is_none() {
            return false;
        }
        if kind != "image" && kind != "video" {
            return true;
        }
        family_for(id).is_some_and(|family| family.components.iter().all(|component| component_path(component).is_some()))
    }

    // MARK: 状态

    /// 下好了要做的：给会话换上（use），失败了告诉发起的网关（modelsFailed）
    struct Waiter {
        client: String,
        activate: bool,
        conversation: Option<String>,
    }

    struct Task {
        kind: String,
        source: Source,
        running: bool,
        error: String,
        progress: f64,
        completed: u64,
        total: u64,
        cancel: Arc<AtomicBool>,
        waiters: Vec<Waiter>,
        retry_waiters: Vec<Waiter>,
    }

    #[derive(Default)]
    struct Models {
        tasks: HashMap<String, Task>,
        order: Vec<String>,
        /// 在等下完的大模型的会话 → 模型 id
        waiting: HashMap<String, String>,
        local: Vec<LocalModel>,
        scanned: bool,
        chat_available: Option<bool>,
    }

    static MODELS: LazyLock<Mutex<Models>> = LazyLock::new(|| Mutex::new(Models::default()));
    static DIRTY: AtomicBool = AtomicBool::new(false);

    fn changed() {
        DIRTY.store(true, Ordering::Relaxed);
    }

    /// 起来时：后台扫一遍模型库，80 毫秒合并一次推 models
    pub fn start() {
        std::thread::spawn(|| {
            rescan();
            loop {
                std::thread::sleep(Duration::from_millis(80));
                if DIRTY.swap(false, Ordering::Relaxed) {
                    crate::notify_all_clients("models", state());
                }
            }
        });
    }

    fn rescan() {
        let found = scan_local();
        let mut models = MODELS.lock().unwrap();
        models.scanned = true;
        if models.local != found {
            models.local = found;
            changed();
        }
    }

    fn chat_available() -> bool {
        powers::is_installed("llm") && powers::is_enabled("llm")
    }

    /// 本地能力变了：本地大模型能不能用变了时，本地引擎跟着开 / 关（config.json 里 power://llm 的 disabled）
    pub fn powers_changed() {
        let available = chat_available();
        let previous = MODELS.lock().unwrap().chat_available.replace(available);
        if previous == Some(available) {
            return;
        }
        let path = crate::config_path();
        let mut config = crate::read_json_file(&path);
        let mut touched = false;
        for engine in config["engines"].as_array_mut().into_iter().flatten() {
            if engine["baseURL"].as_str().is_some_and(|base| base.starts_with("power://llm"))
                && engine["disabled"].as_bool().unwrap_or(false) == available
            {
                engine["disabled"] = json!(!available);
                touched = true;
            }
        }
        if touched && crate::write_json_file(&path, &config).is_ok() {
            crate::notify_all_clients("engines.changed", json!({}));
        }
        changed();
    }

    fn display_name(id: &str) -> String {
        let repo = repo_of(id);
        let name = repo.rsplit('/').next().unwrap_or(&repo);
        let name = if name.to_uppercase().ends_with("-GGUF") { &name[..name.len() - 5] } else { name };
        let quant = quant_of(id);
        if quant.is_empty() { name.to_string() } else { format!("{name} {quant}") }
    }

    fn engine_id_for(id: &str) -> String {
        let hash: String = sha2::Sha256::digest(id.to_lowercase().as_bytes()).iter().take(6).map(|byte| format!("{byte:02x}")).collect();
        format!("local-{hash}")
    }

    /// 下好了的大模型：在 config.json 里建（或更新）它的本地引擎，回引擎 id；生图 / 生视频记成这台本地生图 / 生视频用的
    fn activate(id: &str, kind: &str) -> String {
        let local = local_copy(id);
        let model_id = local.as_ref().map_or(id.to_string(), |local| local.id.clone());
        if kind == "image" || kind == "video" {
            crate::update_serve_settings(|settings| {
                settings.insert(if kind == "image" { "localImageModel" } else { "localVideoModel" }.into(), json!(model_id));
            });
            crate::caps::powers_changed();
            return String::new();
        }
        let engine_id = engine_id_for(&model_id);
        let engine = json!({
            "id": engine_id,
            "title": format!("本地 · {}", display_name(&model_id)),
            "baseURL": "power://llm",
            "apiKeySecret": "none",
            "model": model_id,
            "supportsVision": local.as_ref().is_some_and(|local| local.mmproj_file.is_some()),
            "supportsTools": true,
            // friday-llm 认顶层的 enable_thinking，模板支持思考时生效
            "sendsEnableThinking": true,
            "thinking": "toggle",
            "maxConcurrency": 4,
            "disabled": !chat_available(),
        });
        let path = crate::config_path();
        let mut config = crate::read_json_file(&path);
        if !config.is_object() {
            config = json!({});
        }
        if !config["engines"].is_array() {
            config["engines"] = json!([]);
        }
        let engines = config["engines"].as_array_mut().unwrap();
        match engines.iter_mut().find(|item| item["id"] == json!(engine_id)) {
            Some(existing) => *existing = engine,
            None => engines.push(engine),
        }
        let _ = crate::write_json_file(&path, &config);
        engine_id
    }

    /// 给会话换上（没给会话 = 新对话默认）：会话上选的记在 friday.json（同网关的 runner.setEngine），网关们推给客户端
    fn apply(engine_id: &str, conversation: Option<&str>) {
        match conversation {
            Some(conversation) => crate::META.set_setting(conversation, "engineID", engine_id),
            None => {
                let path = crate::config_path();
                let mut config = crate::read_json_file(&path);
                if config.is_object() {
                    config["defaultEngine"] = json!(engine_id);
                    let _ = crate::write_json_file(&path, &config);
                }
            }
        }
        crate::notify_all_clients("engines.changed", json!({ "conversationID": conversation }));
    }

    fn ensure_power(id: &str) {
        if !powers::is_installed(id) {
            powers::install(id, "");
        } else if !powers::is_enabled(id) {
            powers::set_enabled(id, true);
        }
    }

    /// 开始下（在下的只加等的人）
    fn download_model(id: &str, kind: &str, source: Source, waiter: Option<Waiter>) {
        let start = {
            let mut models = MODELS.lock().unwrap();
            let added = !models.tasks.contains_key(id);
            if added {
                models.order.push(id.to_string());
            }
            let task = models.tasks.entry(id.to_string()).or_insert_with(|| Task {
                kind: kind.to_string(),
                source,
                running: false,
                error: String::new(),
                progress: 0.0,
                completed: 0,
                total: 0,
                cancel: Arc::new(AtomicBool::new(false)),
                waiters: Vec::new(),
                retry_waiters: Vec::new(),
            });
            if let Some(waiter) = waiter {
                task.waiters.push(waiter);
            }
            if task.running {
                false
            } else {
                task.kind = kind.to_string();
                task.source = source;
                task.running = true;
                task.error.clear();
                task.progress = 0.0;
                task.completed = 0;
                task.total = 0;
                task.cancel = Arc::new(AtomicBool::new(false));
                task.retry_waiters.clear();
                true
            }
        };
        changed();
        if !start {
            return;
        }
        let (id, kind) = (id.to_string(), kind.to_string());
        let cancel = MODELS.lock().unwrap().tasks[&id].cancel.clone();
        std::thread::spawn(move || {
            let outcome = download_job(&id, &kind, source, &cancel);
            finish(&id, outcome);
        });
    }

    fn download_job(id: &str, kind: &str, source: Source, cancel: &AtomicBool) -> Result<(), String> {
        let diffusion = kind == "image" || kind == "video";
        let family = if diffusion {
            Some(family_for(id).ok_or("认不出这是哪一类生图 / 生视频模型（本地生图 / 生视频还不支持它）")?)
        } else {
            None
        };
        let repo = repo_of(id);
        let quant = quant_of(id);
        let files = list_files(source, &repo)?;
        let mut selection = None;
        if let Some(family) = &family
            && quant.is_empty()
            && repo.eq_ignore_ascii_case(&family.recommended_repo)
            && !family.recommended_file.is_empty()
        {
            selection = select_file(&files, &family.recommended_file);
        }
        let selection = selection.or_else(|| select(&files, &quant)).ok_or_else(|| {
            if quant.is_empty() {
                format!("仓库 {repo} 里没有 GGUF 模型文件")
            } else {
                format!("仓库 {repo} 里没有 {quant} 这一档")
            }
        })?;
        let mut wanted = selection.model_files.clone();
        // 生图 / 生视频模型的仓库里有 mmproj 也不要
        if family.is_none() && !selection.mmproj_file.is_empty() {
            wanted.push(selection.mmproj_file.clone());
        }
        let missing: Vec<Component> = family
            .as_ref()
            .map(|family| family.components.iter().filter(|component| component_path(component).is_none()).cloned().collect())
            .unwrap_or_default();
        let main_bytes = selection.model_bytes + if family.is_none() { selection.mmproj_bytes } else { 0 };
        let total = main_bytes + missing.iter().map(|component| component.size).sum::<u64>();
        let mut last = std::time::Instant::now() - Duration::from_secs(1);
        let mut report = |done: u64| {
            if last.elapsed() < Duration::from_millis(200) && done < total {
                return;
            }
            last = std::time::Instant::now();
            if let Some(task) = MODELS.lock().unwrap().tasks.get_mut(id) {
                task.completed = done.min(total);
                task.total = total;
                task.progress = if total > 0 { (done as f64 / total as f64).min(1.0) } else { 0.0 };
            }
            changed();
        };
        download(source, &repo, &wanted, cancel, &mut |done| report(done))?;
        let mut base = main_bytes;
        for component in &missing {
            let wanted = vec![component.file.clone()];
            // 这个源上没有：换另一个源试试（配套文件多半两边都有）
            if let Err(error) = download(source, &component.repo, &wanted, cancel, &mut |done| report(base + done)) {
                if error == powers::CANCELLED {
                    return Err(error);
                }
                download(source.other(), &component.repo, &wanted, cancel, &mut |done| report(base + done))?;
            }
            base += component.size;
        }
        Ok(())
    }

    fn finish(id: &str, outcome: Result<(), String>) {
        let waiters = {
            let mut models = MODELS.lock().unwrap();
            let Some(task) = models.tasks.get_mut(id) else { return };
            let waiters = std::mem::take(&mut task.waiters);
            match &outcome {
                Err(error) if error != powers::CANCELLED => {
                    task.running = false;
                    task.error = error.clone();
                    task.retry_waiters = waiters
                        .iter()
                        .map(|waiter| Waiter {
                            client: waiter.client.clone(),
                            activate: waiter.activate,
                            conversation: waiter.conversation.clone(),
                        })
                        .collect();
                }
                // 下好了、取消了：不再列出
                _ => {
                    models.tasks.remove(id);
                    models.order.retain(|item| item != id);
                }
            }
            waiters
        };
        rescan();
        changed();
        let kind = kind_of(id, "");
        for waiter in waiters {
            match &outcome {
                Ok(()) if waiter.activate => {
                    let engine = activate(id, &kind);
                    // 这段会话已经改选了别的：只建引擎，不换回来
                    let still = waiter
                        .conversation
                        .as_ref()
                        .is_none_or(|conversation| MODELS.lock().unwrap().waiting.get(conversation).is_some_and(|wanted| wanted == id));
                    if let Some(conversation) = &waiter.conversation
                        && still
                    {
                        MODELS.lock().unwrap().waiting.remove(conversation);
                    }
                    if still && !engine.is_empty() {
                        apply(&engine, waiter.conversation.as_deref());
                    }
                    changed();
                }
                Ok(()) => {}
                Err(error) if error == powers::CANCELLED => {
                    if let Some(conversation) = &waiter.conversation {
                        MODELS.lock().unwrap().waiting.remove(conversation);
                    }
                    changed();
                }
                Err(error) => {
                    crate::notify_clients(std::slice::from_ref(&waiter.client), "modelsFailed", json!({ "id": id, "message": error }))
                }
            }
        }
    }

    /// 用起来：没下的在这台上下（要用的 Power 没装的先装），下好了大模型给会话换上
    fn use_model(client: &str, id: &str, kind: &str, source: Source, conversation: Option<String>) {
        let diffusion = kind == "image" || kind == "video";
        ensure_power(if diffusion { "diffusion" } else { "llm" });
        let conversation = if diffusion { None } else { conversation };
        if let Some(conversation) = &conversation {
            MODELS.lock().unwrap().waiting.insert(conversation.clone(), id.to_string());
            changed();
        }
        if is_complete(id, kind) {
            let engine = activate(id, kind);
            if let Some(conversation) = &conversation {
                MODELS.lock().unwrap().waiting.remove(conversation);
            }
            if !engine.is_empty() {
                apply(&engine, conversation.as_deref());
            }
            changed();
            return;
        }
        download_model(id, kind, source, Some(Waiter { client: client.to_string(), activate: true, conversation }));
    }

    /// 强制删一个本地模型：friday-llm 还在就停掉、等它放开文件再删，删不掉隔半秒再试，最多 15 秒
    fn remove(model: &LocalModel) -> Result<(), String> {
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        loop {
            if powers::has_process("llm") {
                powers::stop("llm");
                while powers::has_process("llm") && std::time::Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(200));
                }
            }
            let mut failed = Vec::new();
            let mut reason = String::new();
            let mut remove_file = |path: &Path| {
                if std::fs::symlink_metadata(path).is_err() {
                    return;
                }
                if let Err(error) = std::fs::remove_file(path) {
                    failed.push(path.file_name().map(|name| name.to_string_lossy().into_owned()).unwrap_or_default());
                    if reason.is_empty() {
                        reason = error.to_string();
                    }
                }
            };
            for path in &model.files {
                // HF 的快照里是指向 blobs 的符号链接：连同目标一起删
                if let Ok(target) = std::fs::read_link(path) {
                    let target = if target.is_absolute() { target } else { path.parent().map(|dir| dir.join(&target)).unwrap_or(target) };
                    remove_file(path);
                    remove_file(&target);
                } else {
                    remove_file(path);
                }
                for suffix in [".part", ".incomplete", ".incomplete.part"] {
                    remove_file(&PathBuf::from(format!("{}{suffix}", path.display())));
                }
            }
            if failed.is_empty() {
                // 仓库里没有别的模型文件了：整个目录一起删
                if scan_directory(&model.directory).iter().all(|file| !is_model_file(&file.path)) {
                    let _ = std::fs::remove_dir_all(&model.directory);
                }
                rescan();
                return Ok(());
            }
            if std::time::Instant::now() >= deadline {
                rescan();
                return Err(format!("无法删除 {}（{reason}）。可能有别的程序占着这个文件：关掉它再删", failed.join("、")));
            }
            std::thread::sleep(Duration::from_millis(500));
        }
    }

    // MARK: 状态与请求

    fn physical_memory() -> u64 {
        #[cfg(target_os = "macos")]
        {
            let mut size: u64 = 0;
            let mut length = std::mem::size_of::<u64>();
            let name = c"hw.memsize";
            if unsafe { libc::sysctlbyname(name.as_ptr(), (&mut size as *mut u64).cast(), &mut length, std::ptr::null_mut(), 0) } == 0 {
                return size;
            }
            0
        }
        #[cfg(target_os = "linux")]
        {
            std::fs::read_to_string("/proc/meminfo")
                .ok()
                .and_then(|text| {
                    text.lines()
                        .find(|line| line.starts_with("MemTotal:"))
                        .and_then(|line| line.split_whitespace().nth(1)?.parse::<u64>().ok())
                })
                .map(|kib| kib * 1024)
                .unwrap_or(0)
        }
        #[cfg(windows)]
        {
            #[repr(C)]
            struct MemoryStatus {
                length: u32,
                load: u32,
                total_phys: u64,
                avail_phys: u64,
                total_page: u64,
                avail_page: u64,
                total_virtual: u64,
                avail_virtual: u64,
                avail_extended: u64,
            }
            unsafe extern "system" {
                fn GlobalMemoryStatusEx(buffer: *mut MemoryStatus) -> i32;
            }
            let mut status = MemoryStatus {
                length: std::mem::size_of::<MemoryStatus>() as u32,
                load: 0,
                total_phys: 0,
                avail_phys: 0,
                total_page: 0,
                avail_page: 0,
                total_virtual: 0,
                avail_virtual: 0,
                avail_extended: 0,
            };
            if unsafe { GlobalMemoryStatusEx(&mut status) } != 0 { status.total_phys } else { 0 }
        }
        #[cfg(not(any(target_os = "macos", target_os = "linux", windows)))]
        {
            0
        }
    }

    /// 独立显卡（显存从大到小）：NVIDIA 用 nvidia-smi，Linux 上 amdgpu 看 sysfs
    fn gpus() -> Vec<Value> {
        static CACHED: LazyLock<Vec<(String, u64)>> = LazyLock::new(|| {
            let minimum = 1u64 << 30;
            let mut list: Vec<(String, u64)> = Vec::new();
            if let Ok(output) =
                std::process::Command::new("nvidia-smi").args(["--query-gpu=name,memory.total", "--format=csv,noheader,nounits"]).output()
                && output.status.success()
            {
                for line in String::from_utf8_lossy(&output.stdout).lines() {
                    if let Some((name, memory)) = line.rsplit_once(',') {
                        let memory = memory.trim().parse::<u64>().unwrap_or(0) * 1024 * 1024;
                        if memory >= minimum {
                            list.push((name.trim().to_string(), memory));
                        }
                    }
                }
            }
            #[cfg(target_os = "linux")]
            for card in std::fs::read_dir("/sys/class/drm").into_iter().flatten().flatten() {
                let name = card.file_name().to_string_lossy().into_owned();
                if !name.starts_with("card") || name.contains('-') {
                    continue;
                }
                let memory = std::fs::read_to_string(card.path().join("device/mem_info_vram_total"))
                    .ok()
                    .and_then(|text| text.trim().parse::<u64>().ok())
                    .unwrap_or(0);
                if memory >= minimum {
                    let product = std::fs::read_to_string(card.path().join("device/product_name")).unwrap_or_default().trim().to_string();
                    list.push((product, memory));
                }
            }
            list.sort_by_key(|item| std::cmp::Reverse(item.1));
            list
        });
        CACHED.iter().map(|(name, memory)| json!({ "name": name, "memory": memory })).collect()
    }

    /// ModelsState
    pub fn state() -> Value {
        let models = MODELS.lock().unwrap();
        let local: Vec<Value> = models
            .local
            .iter()
            .map(|model| model.to_json(&kind_of(&model.id, if model.mmproj_file.is_some() { "multimodal" } else { "" })))
            .collect();
        let downloads: Vec<Value> = models
            .order
            .iter()
            .filter_map(|id| models.tasks.get(id).map(|task| (id, task)))
            .map(|(id, task)| {
                json!({ "id": id, "name": display_name(id), "kind": task.kind, "running": task.running, "progress": task.progress,
                    "completed": task.completed, "total": task.total, "error": task.error })
            })
            .collect();
        let waiting: Vec<Value> =
            models.waiting.iter().map(|(conversation, id)| json!({ "conversationID": crate::upper(conversation), "id": id })).collect();
        json!({
            "available": true,
            "machine": { "memory": physical_memory(), "gpus": gpus() },
            "chatAvailable": chat_available(),
            "local": local,
            "downloads": downloads,
            "waiting": waiting,
        })
    }

    fn text<'a>(params: &'a Value, key: &str) -> Result<&'a str, (i64, String)> {
        params[key].as_str().filter(|text| !text.is_empty()).ok_or((-32602, format!("缺少参数 {key}")))
    }

    /// models.*（客户端看着这台时：模型菜单的「本地」、模型库）
    pub fn request(client: &str, method: &str, params: &Value) -> crate::Reply {
        let source = || params["source"].as_str().and_then(Source::parse).unwrap_or_else(default_source);
        match method {
            "models.state" => Ok(state()),
            "models.refresh" => {
                std::thread::spawn(rescan);
                // 生图 / 生视频的家族在 diffusion 的清单里：还没取到索引就取一次
                if families().is_empty() && !powers::is_refreshing() {
                    std::thread::spawn(powers::refresh);
                }
                Ok(json!({}))
            }
            "models.download" => {
                let id = text(params, "id")?;
                let kind = params["kind"].as_str().filter(|kind| !kind.is_empty()).map(str::to_string).unwrap_or_else(|| kind_of(id, ""));
                download_model(id, &kind, source(), Some(Waiter { client: client.to_string(), activate: false, conversation: None }));
                Ok(json!({}))
            }
            "models.use" => {
                let id = text(params, "id")?.to_string();
                let conversation = params["conversationID"].as_str().filter(|text| !text.is_empty()).map(crate::lower);
                let kind = match params["kind"].as_str().filter(|kind| !kind.is_empty()) {
                    Some(kind) => kind.to_string(),
                    None => {
                        let vision = local_copy(&id).is_some_and(|local| local.mmproj_file.is_some());
                        kind_of(&id, if vision { "multimodal" } else { "" })
                    }
                };
                let (client, source) = (client.to_string(), source());
                std::thread::spawn(move || use_model(&client, &id, &kind, source, conversation));
                Ok(json!({}))
            }
            "models.cancel" => {
                let id = text(params, "id")?;
                let mut models = MODELS.lock().unwrap();
                match models.tasks.get(id) {
                    Some(task) if task.running => task.cancel.store(true, Ordering::Relaxed),
                    Some(_) => {
                        models.tasks.remove(id);
                        models.order.retain(|item| item != id);
                        models.waiting.retain(|_, wanted| wanted != id);
                        changed();
                    }
                    None => {}
                }
                Ok(json!({}))
            }
            "models.retry" => {
                let id = text(params, "id")?;
                let retry = {
                    let mut models = MODELS.lock().unwrap();
                    models
                        .tasks
                        .get_mut(id)
                        .filter(|task| !task.running)
                        .map(|task| (task.kind.clone(), task.source, std::mem::take(&mut task.retry_waiters)))
                };
                if let Some((kind, source, waiters)) = retry {
                    download_model(id, &kind, source, None);
                    if let Some(task) = MODELS.lock().unwrap().tasks.get_mut(id) {
                        task.waiters.extend(waiters);
                    }
                }
                Ok(json!({}))
            }
            "models.dismiss" => {
                let id = text(params, "id")?;
                let mut models = MODELS.lock().unwrap();
                if models.tasks.get(id).is_some_and(|task| !task.running) {
                    models.tasks.remove(id);
                    models.order.retain(|item| item != id);
                    models.waiting.retain(|_, wanted| wanted != id);
                    changed();
                }
                Ok(json!({}))
            }
            "models.release" => {
                let conversation = crate::lower(text(params, "conversationID")?);
                if MODELS.lock().unwrap().waiting.remove(&conversation).is_some() {
                    changed();
                }
                Ok(json!({}))
            }
            "models.delete" => {
                let id = text(params, "id")?;
                let wanted = params["source"].as_str().and_then(Source::parse);
                if !MODELS.lock().unwrap().scanned {
                    rescan();
                }
                let found = MODELS
                    .lock()
                    .unwrap()
                    .local
                    .iter()
                    .find(|model| model.id == id && wanted.is_none_or(|source| model.source == source))
                    .cloned();
                let model = found.ok_or((crate::NOT_FOUND, format!("这台机器上没有这个本地模型：{id}")))?;
                remove(&model).map(|_| json!({})).map_err(|error| (-32603, error))
            }
            other => Err((-32601, format!("没有这个方法：{other}"))),
        }
    }

    /// model.endpoint（能力协议 §6.6）：power://llm 的真地址——拉起 friday-llm，回地址、令牌、x_friday（模型文件、会话、优先级）
    pub fn endpoint(params: &Value) -> Result<Value, String> {
        let base = params["baseURL"].as_str().unwrap_or("");
        let power = base.strip_prefix("power://").map(|rest| rest.trim_end_matches('/')).unwrap_or("");
        if power != "llm" {
            return Err(format!("Friday 不认识这个模型地址：{base}"));
        }
        let model = params["model"].as_str().unwrap_or("");
        let local = local_copy(model).ok_or(format!("这台机器上没有本地模型 {model}（模型库里下载它）"))?;
        let (url, token) = powers::ensure_running("llm", Duration::from_secs(150))?;
        let agent = params["agent"].as_str().filter(|agent| !agent.is_empty());
        let session = agent.or_else(|| params["session"].as_str()).unwrap_or("");
        let mut hints = Map::new();
        hints.insert("model_path".into(), json!(local.model_file.to_string_lossy()));
        hints.insert(
            "mmproj_path".into(),
            json!(local.mmproj_file.as_ref().map(|path| path.to_string_lossy().into_owned()).unwrap_or_default()),
        );
        if !session.is_empty() {
            hints.insert("session".into(), json!(session));
        }
        hints.insert("priority".into(), json!(if agent.is_some() { 100 } else { 1000 }));
        hints.insert("main".into(), json!(agent.is_none() && !session.is_empty()));
        Ok(json!({ "baseURL": format!("{url}/v1"), "apiKey": token, "body": { "x_friday": hints } }))
    }

    /// 有在下的（常驻的那份闲着不退）
    pub fn busy() -> bool {
        MODELS.lock().unwrap().tasks.values().any(|task| task.running)
    }
}

// MARK: - 定时任务（/loop）

mod loops {
    //! 定时任务（照 Friday 的 LoopCommand / LoopScheduler，Friday 仓库的 qt/docs/LOOPS.md）：都用「/loop [时间] 内容」建——用户在会话里打的
    //! （网关不交给模型，转到这里，问答记进会话），主智能体用 `loop` 工具建的（这里的动作，标成智能体建的，最多 5 个、每个一天最多 24 次）。
    //! 到点把内容（提示词 loop.run*）交给那段会话的大脑（排在手上这一轮之后；上一次没做完就跳过），这一轮的回答（done）就是这次的结果；
    //! 自己定节奏的按回复里单独一行的 `/loop next 20m` 定下次，智能体建的写 `/loop done` 就结束（用户建的只有用户能停）。
    //! 错过的 12 小时内补一次。存在 `~/.edith/friday/loops.json`；有没暂停的任务时常驻的那份不退。

    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{LazyLock, Mutex};
    use std::time::Duration;

    use chrono::{DateTime, Datelike, Local, NaiveDate, NaiveTime, TimeZone, Utc};
    use regex::{Captures, Regex};
    use serde_json::{Value, json};

    const MAX_PER_OWNER: usize = 5;
    const MAX_RUNS_PER_DAY: usize = 24;
    const CATCH_UP_HOURS: i64 = 12;
    const SELF_DEFAULT_MINUTES: i64 = 30;
    const RUN_TIMEOUT_HOURS: i64 = 6;
    const HISTORY_LIMIT: usize = 20;

    // 提示词（同 Friday / EDITH 的 prompts-framework.md 的 loop.run*）
    const RUN: &str = "（定时任务 #{{number}}（{{schedule}}）第 {{count}} 次到点，照下面的做。这件事已经不用再做了，就在回复最后单独一行写 /loop done。）\n{{last}}\n{{content}}";
    const RUN_SELF: &str = "（定时任务 #{{number}} 第 {{count}} 次：盯着下面这件事，节奏你自己定。做完这一次，在回复最后单独一行写 /loop next 多久（比如 /loop next 20m、/loop next 2h，最短 1 分钟、最长 24 小时）定下次什么时候再来；事情办完了、不用再盯了就写 /loop done。不写的话半小时后再来。）\n{{last}}\n{{content}}";
    const RUN_USER: &str = "（定时任务 #{{number}}（{{schedule}}）第 {{count}} 次到点，照下面的做。这是用户建的定时任务，停不停由用户定，你只管把这一次做好。）\n{{last}}\n{{content}}";
    const RUN_SELF_USER: &str = "（定时任务 #{{number}} 第 {{count}} 次：盯着下面这件事，节奏你自己定。做完这一次，在回复最后单独一行写 /loop next 多久（比如 /loop next 20m、/loop next 2h，最短 1 分钟、最长 24 小时）定下次什么时候再来；不写的话半小时后再来。这是用户建的定时任务，停不停由用户定：事情办完了就简短说一声，把下次定远一点（比如 /loop next 24h）。）\n{{last}}\n{{content}}";
    const RUN_LAST: &str = "（上一次的结果：{{result}}）";

    /// 工具 loop 的说明（同 prompts-tools.md 的 tool.loop）
    pub const TOOL_DESCRIPTION: &str = "建、列、停定时任务和循环任务。command 是一条「/loop …」命令，和用户在输入框里打的一样：\n- /loop 内容：节奏你自己定（每次做完在回复最后写 /loop next 20m 定下次，办完了写 /loop done）\n- /loop 10m 内容、/loop 每2小时 内容：固定间隔\n- /loop 每天9:00 内容、/loop 工作日9:00 内容、/loop 每周一10:00 内容：定点\n- /loop 明天8:00 内容、/loop 30分钟后 内容：只做一次\n- /loop 列出来；/loop stop 3、/loop stop 全部 停掉（只能停智能体建的；用户自己建的只有用户能停）\n用户让你「每隔一会儿看看」「每天早上…」「明天提醒我…」「盯着…直到…」时用，不要只口头答应。到点时内容作为一条消息交回给你（在会话里建的发给这段会话）。你建的标成智能体建的，最多 5 个，每个一天最多跑 24 次。";
    pub const TOOL_COMMAND: &str = "一条完整的「/loop …」命令，比如「/loop 每天9:00 看一下昨晚的 CI，挂了就修」";

    fn fill(template: &str, pairs: &[(&str, String)]) -> String {
        let mut text = template.to_string();
        for (key, value) in pairs {
            text = text.replace(&format!("{{{{{key}}}}}"), value);
        }
        // 空的参数整行去掉（同 Friday 的提示词渲染）
        text.lines().filter(|line| !line.trim().is_empty()).collect::<Vec<_>>().join("\n")
    }

    fn brief(text: &str, limit: usize) -> String {
        let line = text.split_whitespace().collect::<Vec<_>>().join(" ");
        if line.chars().count() > limit { format!("{}…", line.chars().take(limit - 1).collect::<String>()) } else { line }
    }

    // MARK: 触发方式

    const EVERY_DAY: u32 = 0x7f;
    const WORKDAYS: u32 = 0x1f;
    const WEEKEND: u32 = 0x60;
    const DAY_NAMES: [char; 7] = ['一', '二', '三', '四', '五', '六', '日'];

    #[derive(Clone, PartialEq, Debug)]
    enum Trigger {
        /// 自己定节奏：跑完由它在回复里写下次
        Itself,
        /// 固定间隔（分钟）
        Interval(i64),
        /// 每周这几天（bit0 = 周一）的这个钟点（本地，分钟）
        Weekly { days: u32, minute_of_day: u32 },
        /// 只做一次
        Once(DateTime<Utc>),
    }

    fn local_at(date: NaiveDate, minute: u32) -> Option<DateTime<Utc>> {
        let time = NaiveTime::from_hms_opt(minute / 60, minute % 60, 0)?;
        Local.from_local_datetime(&date.and_time(time)).earliest().map(|at| at.with_timezone(&Utc))
    }

    impl Trigger {
        fn next_after(&self, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
            match self {
                Trigger::Itself => None,
                Trigger::Interval(minutes) => Some(after + chrono::Duration::minutes(*minutes)),
                Trigger::Once(at) => (*at > after).then_some(*at),
                Trigger::Weekly { days, minute_of_day } => {
                    if *days == 0 {
                        return None;
                    }
                    let local = after.with_timezone(&Local);
                    (0..=7).find_map(|offset| {
                        let date = local.date_naive() + chrono::Duration::days(offset);
                        let weekday = date.weekday().num_days_from_monday();
                        if days & (1 << weekday) == 0 {
                            return None;
                        }
                        local_at(date, *minute_of_day).filter(|candidate| *candidate > after)
                    })
                }
            }
        }

        fn describe(&self) -> String {
            match self {
                Trigger::Itself => "自己定节奏".into(),
                Trigger::Interval(minutes) if minutes % 1440 == 0 => {
                    if *minutes == 1440 {
                        "每天一次".into()
                    } else {
                        format!("每 {} 天", minutes / 1440)
                    }
                }
                Trigger::Interval(minutes) if minutes % 60 == 0 => format!("每 {} 小时", minutes / 60),
                Trigger::Interval(minutes) => format!("每 {minutes} 分钟"),
                Trigger::Weekly { days, minute_of_day } => {
                    let when = match *days {
                        EVERY_DAY => "每天".to_string(),
                        WORKDAYS => "工作日".to_string(),
                        WEEKEND => "周末".to_string(),
                        _ => format!(
                            "每周{}",
                            (0..7)
                                .filter(|index| days & (1 << index) != 0)
                                .map(|index| DAY_NAMES[index].to_string())
                                .collect::<Vec<_>>()
                                .join("、")
                        ),
                    };
                    format!("{when} {}", clock(*minute_of_day))
                }
                Trigger::Once(at) => format!("{} 一次", at.with_timezone(&Local).format("%-m月%-d日 %H:%M")),
            }
        }

        fn to_json(&self) -> Value {
            match self {
                Trigger::Itself => json!({ "kind": "self" }),
                Trigger::Interval(minutes) => json!({ "kind": "interval", "minutes": minutes }),
                Trigger::Weekly { days, minute_of_day } => json!({ "kind": "weekly", "days": days, "minuteOfDay": minute_of_day }),
                Trigger::Once(at) => json!({ "kind": "once", "at": iso(at) }),
            }
        }

        fn from_json(value: &Value) -> Option<Trigger> {
            match value["kind"].as_str()? {
                "self" => Some(Trigger::Itself),
                "interval" => value["minutes"].as_i64().filter(|minutes| *minutes > 0).map(Trigger::Interval),
                "weekly" => {
                    let days = value["days"].as_u64().filter(|days| *days > 0)? as u32;
                    Some(Trigger::Weekly { days: days & EVERY_DAY, minute_of_day: value["minuteOfDay"].as_u64().unwrap_or(0) as u32 })
                }
                "once" => parse_time(&value["at"]).map(Trigger::Once),
                _ => None,
            }
        }
    }

    fn clock(minute_of_day: u32) -> String {
        format!("{}:{:02}", minute_of_day / 60, minute_of_day % 60)
    }

    fn iso(at: &DateTime<Utc>) -> String {
        at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
    }

    fn parse_time(value: &Value) -> Option<DateTime<Utc>> {
        DateTime::parse_from_rfc3339(value.as_str()?).ok().map(|at| at.with_timezone(&Utc))
    }

    /// 给人看的时刻：今天的「14:20」、明天的「明天 14:20」、别的「10月15日 14:20」
    fn when(at: &DateTime<Utc>, now: DateTime<Utc>) -> String {
        let local = at.with_timezone(&Local);
        let today = now.with_timezone(&Local).date_naive();
        if local.date_naive() == today {
            local.format("%H:%M").to_string()
        } else if local.date_naive() == today + chrono::Duration::days(1) {
            format!("明天 {}", local.format("%H:%M"))
        } else {
            local.format("%-m月%-d日 %H:%M").to_string()
        }
    }

    // MARK: 「/loop …」

    enum Command {
        Create(Trigger, String),
        List,
        Stop { number: i64, all: bool },
        Next(i64),
        Done,
    }

    const UNITS: &str =
        "秒钟|秒|seconds|second|secs|sec|s|分钟|分|minutes|minute|mins|min|m|个小时|小时|个钟头|钟头|hours|hour|hrs|hr|h|天|days|day|d";
    const TIME: &str = r"(早上|早晨|上午|中午|下午|傍晚|晚上|凌晨)?\s*(\d{1,2})(?:[:：](\d{2})|\s*点(?:\s*(半)|\s*(\d{1,2})\s*分?)?)";

    fn pattern(text: &str) -> Regex {
        Regex::new(&format!("(?is){text}")).unwrap()
    }

    fn unit_seconds(unit: &str) -> f64 {
        let unit = unit.to_lowercase();
        if unit.starts_with('秒') || unit.starts_with('s') {
            1.0
        } else if unit.starts_with('分') || unit.starts_with('m') {
            60.0
        } else if unit.contains('时') || unit.contains("钟头") || unit.starts_with('h') {
            3600.0
        } else {
            86400.0
        }
    }

    /// 时间段那几组（时段、时、分、半、分）→ 当天的分钟数
    fn minute_of_day(captures: &Captures, first: usize, forced: &str) -> Option<u32> {
        let period = captures.get(first).map(|period| period.as_str()).unwrap_or(forced);
        let mut hour: u32 = captures.get(first + 1)?.as_str().parse().ok()?;
        let minute: u32 = if let Some(minute) = captures.get(first + 2) {
            minute.as_str().parse().ok()?
        } else if captures.get(first + 3).is_some() {
            30
        } else if let Some(minute) = captures.get(first + 4) {
            minute.as_str().parse().ok()?
        } else {
            0
        };
        match period {
            "下午" | "傍晚" | "晚上" if hour < 12 => hour += 12,
            "中午" if hour < 11 => hour += 12,
            "凌晨" if hour == 12 => hour = 0,
            _ => {}
        }
        (hour <= 23 && minute <= 59).then_some(hour * 60 + minute)
    }

    fn day_mask(text: &str) -> u32 {
        text.chars()
            .filter_map(|c| DAY_NAMES.iter().position(|day| *day == if c == '天' { '日' } else { c }))
            .fold(0, |mask, index| mask | (1 << index))
    }

    /// 「10m」「2小时」「1.5h」→ 秒
    fn duration(text: &str) -> Option<i64> {
        static DURATION: LazyLock<Regex> = LazyLock::new(|| pattern(&format!(r"^(\d+(?:\.\d+)?)\s*({UNITS})$")));
        let captures = DURATION.captures(text.trim())?;
        let seconds = captures[1].parse::<f64>().ok()? * unit_seconds(&captures[2]);
        (seconds > 0.0 && seconds <= 366.0 * 86400.0).then(|| seconds.ceil() as i64)
    }

    pub fn is_command(text: &str) -> bool {
        static HEAD: LazyLock<Regex> = LazyLock::new(|| pattern(r"^\s*/loop(?:\s|$)"));
        HEAD.is_match(text)
    }

    fn created(trigger: Trigger, content: &str) -> Result<Command, String> {
        let content = content.trim();
        if content.is_empty() {
            return Err("时间后面要写让它做什么".into());
        }
        Ok(Command::Create(trigger, content.to_string()))
    }

    fn parse(text: &str, now: DateTime<Utc>) -> Result<Command, String> {
        static HEAD: LazyLock<Regex> = LazyLock::new(|| pattern(r"^\s*/loop"));
        static LIST: LazyLock<Regex> = LazyLock::new(|| pattern(r"^(?:list|ls|列表|列出来|看看|全部)?$"));
        static STOP: LazyLock<Regex> =
            LazyLock::new(|| pattern(r"^(?:stop|cancel|停|停掉|停止|取消|删|删掉|删除)\s*(?:#?(\d+)|(全部|所有|all))$"));
        static NEXT: LazyLock<Regex> = LazyLock::new(|| pattern(r"^(?:next|下次|下一次)\s*(.+)$"));
        static DONE: LazyLock<Regex> = LazyLock::new(|| pattern(r"^(?:done|完成|结束|办完了)$"));
        static EVERY: LazyLock<Regex> =
            LazyLock::new(|| pattern(&format!(r"^(?:每隔|每|every\s*)\s*(\d+(?:\.\d+)?)\s*({UNITS})(?:一次|钟)?(\s*)(.*)$")));
        static LATER: LazyLock<Regex> = LazyLock::new(|| pattern(&format!(r"^(\d+(?:\.\d+)?)\s*({UNITS})(\s+|$)(.*)$")));
        static LATER_WORD: LazyLock<Regex> = LazyLock::new(|| {
            pattern(&format!(r"^(?:(\d+(?:\.\d+)?)\s*({UNITS})(?:后|以后|之后)|in\s+(\d+(?:\.\d+)?)\s*({UNITS}))\s*(.*)$"))
        });
        static WEEKLY: LazyLock<Regex> = LazyLock::new(|| {
            pattern(&format!(
                r"^(每天|每日|天天|daily|every\s*day|每个?工作日|工作日|weekdays|每个?周末|周末|weekends|(?:每周|每星期|每个星期|周|星期)([一二三四五六日天](?:\s*[、,，和]?\s*[一二三四五六日天])*))\s*{TIME}\s*(.*)$"
            ))
        });
        static DATED: LazyLock<Regex> = LazyLock::new(|| {
            pattern(&format!(
                r"^(今天|今晚|明天|明早|明晚|后天|(\d{{4}})[-/.](\d{{1,2}})[-/.](\d{{1,2}})|(\d{{1,2}})[-/](\d{{1,2}})|(\d{{1,2}})月(\d{{1,2}})[日号]?)\s*{TIME}\s*(.*)$"
            ))
        });
        static BARE: LazyLock<Regex> = LazyLock::new(|| pattern(&format!(r"^{TIME}\s*(.*)$")));
        let rest = HEAD.replace(text, "").trim().to_string();
        let local = now.with_timezone(&Local);
        if LIST.is_match(&rest) {
            return Ok(Command::List);
        }
        if let Some(captures) = STOP.captures(&rest) {
            return Ok(Command::Stop {
                number: captures.get(1).and_then(|number| number.as_str().parse().ok()).unwrap_or(0),
                all: captures.get(2).is_some(),
            });
        }
        if let Some(captures) = NEXT.captures(&rest) {
            return duration(&captures[1]).map(Command::Next).ok_or_else(|| "「/loop next」后面要写多久，比如 /loop next 20m".to_string());
        }
        if DONE.is_match(&rest) {
            return Ok(Command::Done);
        }
        let minutes = |number: &str, unit: &str| ((number.parse::<f64>().unwrap_or(0.0) * unit_seconds(unit) / 60.0).ceil() as i64).max(1);
        // 「每 10 分钟」「every 2h」：单位后面紧跟着英文字母的不算（10min… 是 10 分钟，10mail 不是）
        if let Some(captures) = EVERY.captures(&rest)
            && (!captures[3].is_empty() || !captures[4].starts_with(|c: char| c.is_ascii_alphabetic()))
        {
            if captures[1].parse::<f64>().unwrap_or(0.0) <= 0.0 {
                return Err("间隔要大于 0".into());
            }
            return created(Trigger::Interval(minutes(&captures[1], &captures[2])), &captures[4]);
        }
        if let Some(captures) = LATER_WORD.captures(&rest) {
            let (number, unit) = match captures.get(1) {
                Some(number) => (number.as_str().to_string(), captures[2].to_string()),
                None => (captures[3].to_string(), captures[4].to_string()),
            };
            let seconds = number.parse::<f64>().unwrap_or(0.0) * unit_seconds(&unit);
            if seconds <= 0.0 {
                return Err("时间要大于 0".into());
            }
            return created(Trigger::Once(now + chrono::Duration::seconds(seconds.ceil() as i64)), &captures[5]);
        }
        if let Some(captures) = LATER.captures(&rest) {
            if captures[1].parse::<f64>().unwrap_or(0.0) <= 0.0 {
                return Err("间隔要大于 0".into());
            }
            return created(Trigger::Interval(minutes(&captures[1], &captures[2])), &captures[4]);
        }
        let wrong_time = || "时间不对：几点要在 0–23，几分在 0–59".to_string();
        if let Some(captures) = WEEKLY.captures(&rest) {
            let which = captures[1].to_lowercase();
            let days = if which.contains("工作日") || which == "weekdays" {
                WORKDAYS
            } else if which.contains("周末") || which == "weekends" {
                WEEKEND
            } else if let Some(days) = captures.get(2) {
                day_mask(days.as_str())
            } else {
                EVERY_DAY
            };
            let minute = minute_of_day(&captures, 3, "").ok_or_else(wrong_time)?;
            return created(Trigger::Weekly { days, minute_of_day: minute }, &captures[8]);
        }
        if let Some(captures) = DATED.captures(&rest) {
            let day = &captures[1];
            let mut date = local.date_naive();
            let mut period = "";
            match day {
                "今晚" => period = "晚上",
                "明天" | "明早" => date += chrono::Duration::days(1),
                "明晚" => {
                    date += chrono::Duration::days(1);
                    period = "晚上";
                }
                "后天" => date += chrono::Duration::days(2),
                _ => {}
            }
            let number = |index: usize| captures.get(index).and_then(|value| value.as_str().parse::<u32>().ok());
            let mut guess_year = false;
            let parsed = if let (Some(year), Some(month), Some(day)) = (number(2), number(3), number(4)) {
                NaiveDate::from_ymd_opt(year as i32, month, day)
            } else if let (Some(month), Some(day)) = (number(5).or(number(7)), number(6).or(number(8))) {
                guess_year = true;
                NaiveDate::from_ymd_opt(local.year(), month, day)
            } else {
                Some(date)
            };
            let date = parsed.ok_or("日期不对")?;
            let minute = minute_of_day(&captures, 9, period).ok_or_else(wrong_time)?;
            let mut at = local_at(date, minute).ok_or("日期不对")?;
            if at <= now && guess_year {
                at = date.with_year(date.year() + 1).and_then(|next| local_at(next, minute)).unwrap_or(at);
            }
            if at <= now {
                return Err("这个时间已经过了".into());
            }
            return created(Trigger::Once(at), &captures[14]);
        }
        if let Some(captures) = BARE.captures(&rest) {
            let minute = minute_of_day(&captures, 1, "").ok_or_else(wrong_time)?;
            let mut at = local_at(local.date_naive(), minute).ok_or("日期不对")?;
            if at <= now {
                at = local_at(local.date_naive() + chrono::Duration::days(1), minute).ok_or("日期不对")?;
            }
            return created(Trigger::Once(at), &captures[6]);
        }
        created(Trigger::Itself, &rest)
    }

    /// 一次跑完的回复里单独一行的「/loop next 20m」「/loop done」（有几个取最后一个）
    fn directive(reply: &str) -> Option<Command> {
        static LINE: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"(?im)^[\s`*>_-]*(/loop\s+(?:next|下次)\s*[^`*\n]+?|/loop\s+done)[\s`*_]*$").unwrap());
        LINE.captures_iter(reply)
            .filter_map(|captures| parse(&captures[1], Utc::now()).ok())
            .filter(|command| matches!(command, Command::Next(_) | Command::Done))
            .last()
    }

    fn without_directives(reply: &str) -> String {
        static LINE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?im)^[\s`*>_-]*/loop\s+(?:next|下次|done)\b[^\n]*$\n?").unwrap());
        LINE.replace_all(reply, "").trim().to_string()
    }

    fn usage() -> &'static str {
        "用法：/loop [时间] 要做的事\n· 不写时间：它自己定多久再来（/loop 盯着 CI，过了告诉我）\n· 间隔：/loop 10m …、/loop 每2小时 …\n· 定点：/loop 每天9:00 …、/loop 工作日9:00 …、/loop 每周一10:00 …\n· 一次：/loop 明天8:00 …、/loop 10-15 14:00 …、/loop 30分钟后 …\n· /loop 列出来，/loop stop 3 停掉 #3，/loop stop 全部"
    }

    // MARK: 任务

    #[derive(Clone)]
    struct Task {
        id: String,
        number: i64,
        trigger: Trigger,
        content: String,
        /// 会话 id（小写）
        conversation: String,
        /// user / agent
        created_by: String,
        created_at: DateTime<Utc>,
        next_run: Option<DateTime<Utc>>,
        last_run: Option<DateTime<Utc>>,
        run_started_at: Option<DateTime<Utc>>,
        /// 正在跑的这次在大脑里是哪一轮
        run_turn: String,
        paused: bool,
        run_count: i64,
        last_result: String,
        history: Vec<Value>,
        run_times: Vec<DateTime<Utc>>,
    }

    impl Task {
        fn running(&self) -> bool {
            self.run_started_at.is_some()
        }

        /// 智能体建的、自己定节奏的：一天最多跑 24 次
        fn capped(&self) -> bool {
            self.created_by != "user" || self.trigger == Trigger::Itself
        }

        /// 用户建的只有用户能停
        fn agent_can_stop(&self) -> bool {
            self.created_by != "user"
        }

        fn line(&self, now: DateTime<Utc>) -> String {
            let mut parts = vec![format!("#{}", self.number), self.trigger.describe(), brief(&self.content, 60)];
            if self.created_by != "user" {
                parts.push("智能体建的".into());
            }
            if self.paused {
                parts.push("暂停了".into());
            } else if self.running() {
                parts.push("正在做".into());
            } else if let Some(next) = &self.next_run {
                parts.push(format!("下次 {}", when(next, now)));
            }
            if !self.last_result.is_empty() {
                parts.push(format!("上次：{}", brief(&self.last_result, 60)));
            }
            parts.join(" · ")
        }

        fn to_json(&self) -> Value {
            let mut value = json!({
                "id": self.id, "number": self.number, "trigger": self.trigger.to_json(), "content": self.content,
                "target": "conversation", "conversationID": crate::upper(&self.conversation), "createdBy": self.created_by,
                "createdAt": iso(&self.created_at), "runCount": self.run_count, "history": self.history,
            });
            for (key, at) in [("nextRun", &self.next_run), ("lastRun", &self.last_run), ("runStartedAt", &self.run_started_at)] {
                if let Some(at) = at {
                    value[key] = json!(iso(at));
                }
            }
            if !self.run_turn.is_empty() {
                value["runTurn"] = json!(self.run_turn);
            }
            if self.paused {
                value["paused"] = json!(true);
            }
            if !self.last_result.is_empty() {
                value["lastResult"] = json!(self.last_result);
            }
            if !self.run_times.is_empty() {
                value["runTimes"] = json!(self.run_times.iter().map(iso).collect::<Vec<_>>());
            }
            value
        }

        fn from_json(value: &Value) -> Option<Task> {
            let text = |key: &str| value[key].as_str().unwrap_or("").to_string();
            let task = Task {
                id: text("id"),
                number: value["number"].as_i64().unwrap_or(0),
                trigger: Trigger::from_json(&value["trigger"])?,
                content: text("content"),
                conversation: crate::lower(&text("conversationID")),
                created_by: if text("createdBy").is_empty() { "user".into() } else { text("createdBy") },
                created_at: parse_time(&value["createdAt"]).unwrap_or_else(Utc::now),
                next_run: parse_time(&value["nextRun"]),
                last_run: parse_time(&value["lastRun"]),
                run_started_at: parse_time(&value["runStartedAt"]),
                run_turn: text("runTurn"),
                paused: value["paused"].as_bool().unwrap_or(false),
                run_count: value["runCount"].as_i64().unwrap_or(0),
                last_result: text("lastResult"),
                history: value["history"].as_array().cloned().unwrap_or_default(),
                run_times: value["runTimes"].as_array().into_iter().flatten().filter_map(parse_time).collect(),
            };
            (!task.id.is_empty() && !task.content.is_empty() && !task.conversation.is_empty()).then_some(task)
        }

        fn record(&mut self, at: DateTime<Utc>, status: &str, result: &str) {
            self.history.push(json!({ "at": iso(&at), "status": status, "result": result }));
            let excess = self.history.len().saturating_sub(HISTORY_LIMIT);
            self.history.drain(..excess);
        }

        fn runs_in_last_day(&self, now: DateTime<Utc>) -> usize {
            self.run_times.iter().filter(|at| (now - **at).num_seconds() < 86400).count()
        }

        /// 到点发出去的那条
        fn run_text(&self) -> String {
            let last = if !self.last_result.is_empty() && self.run_count > 1 {
                fill(RUN_LAST, &[("result", brief(&self.last_result, 300))])
            } else {
                String::new()
            };
            let template = match (self.trigger == Trigger::Itself, self.agent_can_stop()) {
                (true, true) => RUN_SELF,
                (true, false) => RUN_SELF_USER,
                (false, true) => RUN,
                (false, false) => RUN_USER,
            };
            fill(
                template,
                &[
                    ("number", self.number.to_string()),
                    ("schedule", self.trigger.describe()),
                    ("count", self.run_count.to_string()),
                    ("last", last),
                    ("content", self.content.clone()),
                ],
            )
        }
    }

    #[derive(Default)]
    struct Loops {
        tasks: Vec<Task>,
        next_number: i64,
        loaded: bool,
    }

    static LOOPS: LazyLock<Mutex<Loops>> = LazyLock::new(|| Mutex::new(Loops::default()));
    static DIRTY: AtomicBool = AtomicBool::new(false);

    fn path() -> std::path::PathBuf {
        crate::serve_dir().join("loops.json")
    }

    fn with<T>(change: impl FnOnce(&mut Loops) -> T) -> T {
        let mut loops = LOOPS.lock().unwrap();
        if !loops.loaded {
            loops.loaded = true;
            let saved = crate::read_json_file(&path());
            loops.next_number = saved["nextNumber"].as_i64().unwrap_or(1).max(1);
            loops.tasks = saved["tasks"].as_array().into_iter().flatten().filter_map(Task::from_json).collect();
            let highest = loops.tasks.iter().map(|task| task.number + 1).max().unwrap_or(1);
            loops.next_number = loops.next_number.max(highest);
        }
        change(&mut loops)
    }

    fn save(loops: &Loops) {
        let value = json!({ "nextNumber": loops.next_number, "tasks": loops.tasks.iter().map(Task::to_json).collect::<Vec<_>>() });
        let _ = crate::write_json_file(&path(), &value);
        DIRTY.store(true, Ordering::Relaxed);
    }

    /// 起来时：每 30 秒看一次到点的（马上看一次），80 毫秒合并一次推 loops
    pub fn start() {
        std::thread::spawn(|| {
            loop {
                check_running();
                tick();
                std::thread::sleep(Duration::from_secs(30));
            }
        });
        std::thread::spawn(|| {
            loop {
                std::thread::sleep(Duration::from_millis(80));
                if DIRTY.swap(false, Ordering::Relaxed) {
                    crate::notify_all_clients("loops", state());
                }
            }
        });
    }

    /// 有没暂停的任务（常驻的那份不退）
    pub fn active() -> bool {
        with(|loops| loops.tasks.iter().any(|task| !task.paused))
    }

    /// 处理一条「/loop …」：建、列、停；回给人看的。conversation：在哪段会话里打的；by：user / agent。
    /// kick：刚建的马上看一次（网关交来的不：它先把问答记进会话，再叫 loops.tick）
    pub fn handle(text: &str, conversation: &str, by: &str, kick: bool) -> String {
        let now = Utc::now();
        let command = match parse(text, now) {
            Ok(command) => command,
            Err(error) => return format!("没建成：{error}。\n{}", usage()),
        };
        let conversation = crate::lower(conversation);
        let reply = with(|loops| {
            let owns = |task: &Task| task.conversation == conversation;
            match command {
                Command::List => list_text(loops, &conversation, now),
                Command::Next(_) | Command::Done => {
                    format!("「/loop next」「/loop done」是定时任务跑的那次写在回复最后的，用来定下次什么时候来、或者结束。\n{}", usage())
                }
                Command::Stop { number, all } => {
                    // 智能体（工具）停：只停智能体建的，用户建的留着
                    let by_agent = by != "user";
                    let stoppable = |task: &Task| owns(task) && (!by_agent || task.agent_can_stop());
                    let mut stopped = Vec::new();
                    let mut kept = Vec::new();
                    if all {
                        for task in &loops.tasks {
                            if stoppable(task) {
                                stopped.push(format!("#{}", task.number));
                            } else if owns(task) {
                                kept.push(format!("#{}", task.number));
                            }
                        }
                        loops.tasks.retain(|task| !stoppable(task));
                    } else {
                        let Some(found) = loops.tasks.iter().find(|task| task.number == number && owns(task)).cloned() else {
                            return format!("没有定时任务 #{number}。\n{}", list_text(loops, &conversation, now));
                        };
                        if !stoppable(&found) {
                            return user_only(&format!("#{}", found.number));
                        }
                        stopped.push(format!("#{}", found.number));
                        loops.tasks.retain(|task| task.id != found.id);
                    }
                    if !stopped.is_empty() {
                        save(loops);
                    }
                    if !kept.is_empty() {
                        let head = if stopped.is_empty() {
                            String::new()
                        } else {
                            format!("停掉了智能体建的定时任务 {}。", stopped.join("、"))
                        };
                        return head + &user_only(&kept.join("、"));
                    }
                    if stopped.is_empty() {
                        "没有在跑的定时任务。".into()
                    } else {
                        format!("停掉了定时任务 {}。", stopped.join("、"))
                    }
                }
                Command::Create(trigger, content) => {
                    // 智能体建的有上限
                    if by != "user" && loops.tasks.iter().filter(|task| task.created_by != "user" && owns(task)).count() >= MAX_PER_OWNER {
                        return format!("没建成：智能体在这里建的定时任务最多 {MAX_PER_OWNER} 个，先停掉不用的（/loop stop 编号）。");
                    }
                    // 间隔、自己定节奏的马上跑第一次；定点、一次的到点再跑
                    let next_run =
                        if matches!(trigger, Trigger::Itself | Trigger::Interval(_)) { Some(now) } else { trigger.next_after(now) };
                    let task = Task {
                        id: uuid::Uuid::new_v4().to_string().to_uppercase(),
                        number: loops.next_number,
                        trigger,
                        content,
                        conversation: conversation.clone(),
                        created_by: by.to_string(),
                        created_at: now,
                        next_run,
                        last_run: None,
                        run_started_at: None,
                        run_turn: String::new(),
                        paused: false,
                        run_count: 0,
                        last_result: String::new(),
                        history: Vec::new(),
                        run_times: Vec::new(),
                    };
                    loops.next_number += 1;
                    let mut reply = format!("建好了定时任务 #{}（{}）：{}", task.number, task.trigger.describe(), brief(&task.content, 80));
                    match &task.next_run {
                        Some(next) if *next == now => reply += "。现在先做第一次",
                        Some(next) => reply += &format!("。第一次 {}", when(next, now)),
                        None => {}
                    }
                    if task.trigger == Trigger::Itself {
                        reply += if task.agent_can_stop() {
                            "，之后每次做完它自己定多久再来，办完了就停"
                        } else {
                            "，之后每次做完它自己定多久再来"
                        };
                    }
                    if task.capped() {
                        reply += &format!("（一天最多 {MAX_RUNS_PER_DAY} 次）");
                    }
                    reply += &format!("。\n/loop 看全部，/loop stop {} 停掉。", task.number);
                    loops.tasks.push(task);
                    save(loops);
                    reply
                }
            }
        });
        // 刚建的马上看一次（间隔、自己定节奏的现在就跑）
        if kick {
            std::thread::spawn(tick);
        }
        reply
    }

    fn user_only(numbers: &str) -> String {
        format!(
            "定时任务 {numbers} 是用户建的，只有用户能停：不要再试着停它，照常做好每一次；用户想停可以自己打 /loop stop 编号，或在设置 › 定时任务里删。"
        )
    }

    fn list_text(loops: &Loops, conversation: &str, now: DateTime<Utc>) -> String {
        let lines: Vec<String> = loops.tasks.iter().filter(|task| task.conversation == conversation).map(|task| task.line(now)).collect();
        if lines.is_empty() {
            return format!("这里还没有定时任务。\n{}", usage());
        }
        format!("定时任务（{} 个）：\n{}\n停掉：/loop stop 编号，或 /loop stop 全部。", lines.len(), lines.join("\n"))
    }

    /// 到点的跑起来
    fn tick() {
        let now = Utc::now();
        let due: Vec<Task> = with(|loops| {
            let mut dirty = false;
            let mut due = Vec::new();
            for task in loops.tasks.iter_mut() {
                let Some(next) = task.next_run.filter(|next| !task.paused && *next <= now) else { continue };
                // 错过太久（常驻的那份没开）：不补，从现在算下一次
                if (now - next).num_hours() >= CATCH_UP_HOURS {
                    task.record(now, "missed", &format!("错过了 {} 那次（超过 {CATCH_UP_HOURS} 小时，不补）", when(&next, now)));
                    task.next_run = match task.trigger {
                        Trigger::Once(_) => None,
                        Trigger::Itself => Some(now),
                        _ => task.trigger.next_after(now),
                    };
                    dirty = true;
                    if task.trigger != Trigger::Itself {
                        continue;
                    }
                }
                if task.running() {
                    task.record(now, "skipped", "上一次还没做完，跳过这次");
                    task.next_run = task.trigger.next_after(now);
                    dirty = true;
                    continue;
                }
                if task.capped() && task.runs_in_last_day(now) >= MAX_RUNS_PER_DAY {
                    // 到了一天的上限：等最早那次满 24 小时
                    let earliest = task.run_times.iter().filter(|at| (now - **at).num_seconds() < 86400).min().copied().unwrap_or(now);
                    task.record(now, "skipped", &format!("今天已经跑满 {MAX_RUNS_PER_DAY} 次"));
                    let after_cap = earliest + chrono::Duration::hours(24);
                    task.next_run = Some(task.trigger.next_after(now).map_or(after_cap, |next| next.max(after_cap)));
                    dirty = true;
                    continue;
                }
                // 开始这一次：定点、间隔的从这次开始算下一次；自己定节奏的等它说；一次的跑完就删
                task.last_run = Some(now);
                task.run_count += 1;
                task.run_times.retain(|at| (now - *at).num_seconds() < 86400);
                task.run_times.push(now);
                task.next_run = match task.trigger {
                    Trigger::Itself | Trigger::Once(_) => None,
                    _ => task.trigger.next_after(now),
                };
                task.run_started_at = Some(now);
                task.run_turn.clear();
                dirty = true;
                due.push(task.clone());
            }
            // 一次性的、错过了的删掉
            let before = loops.tasks.len();
            loops.tasks.retain(|task| !(matches!(task.trigger, Trigger::Once(_)) && task.next_run.is_none() && !task.running()));
            if dirty || loops.tasks.len() != before {
                save(loops);
            }
            due
        });
        for task in due {
            std::thread::spawn(move || {
                let outcome = crate::fire_loop(&task.conversation, &task.run_text());
                with(|loops| {
                    let Some(current) = loops.tasks.iter_mut().find(|item| item.id == task.id) else { return };
                    match outcome {
                        Ok(turn) => current.run_turn = turn,
                        Err(gone) if gone.is_empty() => {
                            // 会话不在了：任务也删掉
                            loops.tasks.retain(|item| item.id != task.id);
                        }
                        Err(error) => {
                            current.run_started_at = None;
                            current.record(Utc::now(), "failed", &error);
                            if current.trigger == Trigger::Itself {
                                current.next_run = Some(Utc::now() + chrono::Duration::minutes(SELF_DEFAULT_MINUTES));
                            }
                        }
                    }
                    save(loops);
                });
            });
        }
    }

    /// 在跑的超过 6 小时没结果：这次算没做成
    fn check_running() {
        let now = Utc::now();
        let stale: Vec<String> = with(|loops| {
            loops
                .tasks
                .iter()
                .filter(|task| task.run_started_at.is_some_and(|started| (now - started).num_hours() >= RUN_TIMEOUT_HOURS))
                .map(|task| task.id.clone())
                .collect()
        });
        for id in stale {
            finish(&id, &format!("超过 {RUN_TIMEOUT_HOURS} 小时没等到结果"), "failed");
        }
    }

    /// 大脑里一轮做完了（done）：是哪个任务的那一次就收下结果
    pub fn on_done(session: &str, turn: &str, data: &Value) {
        let id = with(|loops| {
            loops
                .tasks
                .iter()
                .find(|task| task.running() && !task.run_turn.is_empty() && task.run_turn == turn && task.conversation == session)
                .map(|task| task.id.clone())
        });
        let Some(id) = id else { return };
        match data["error"].as_str().filter(|error| !error.is_empty()) {
            Some(error) => finish(&id, &format!("出错了：{error}"), "failed"),
            None if data["stopped"].as_bool() == Some(true) => finish(&id, "被停下了", "failed"),
            None => finish(&id, data["answer"].as_str().unwrap_or(""), "ok"),
        }
    }

    fn finish(id: &str, reply: &str, status: &str) {
        let now = Utc::now();
        with(|loops| {
            let Some(task) = loops.tasks.iter_mut().find(|task| task.id == id) else { return };
            let started = task.run_started_at.take().unwrap_or(now);
            task.run_turn.clear();
            let result = without_directives(reply);
            task.last_result = brief(&result, 400);
            let last = task.last_result.clone();
            task.record(started, status, &last);
            let directive = if status == "ok" { directive(reply) } else { None };
            let mut ended = matches!(task.trigger, Trigger::Once(_));
            // 用户建的只有用户能停：写了 /loop done 也不算
            if matches!(directive, Some(Command::Done)) && task.agent_can_stop() {
                ended = true;
            }
            if !ended && task.trigger == Trigger::Itself {
                // 它定的下次（1 分钟到 24 小时）；没说就半小时后
                let seconds = match directive {
                    Some(Command::Next(seconds)) => seconds.clamp(60, 86400),
                    _ => SELF_DEFAULT_MINUTES * 60,
                };
                task.next_run = Some(now + chrono::Duration::seconds(seconds));
            }
            if ended {
                loops.tasks.retain(|task| task.id != id);
            }
            save(loops);
        });
    }

    /// 会话删了：它的任务一起删
    pub fn forget_conversation(conversation: &str) {
        let conversation = crate::lower(conversation);
        with(|loops| {
            let before = loops.tasks.len();
            loops.tasks.retain(|task| task.conversation != conversation);
            if loops.tasks.len() != before {
                save(loops);
            }
        });
    }

    /// LoopsState：loops.json 里的那份加 targetLabel（「会话：标题」）
    pub fn state() -> Value {
        let tasks: Vec<Value> = with(|loops| loops.tasks.clone())
            .iter()
            .map(|task| {
                let mut value = task.to_json();
                value["targetLabel"] = json!(format!("会话：{}", crate::conversation_title(&task.conversation)));
                value
            })
            .collect();
        json!({ "tasks": tasks })
    }

    /// loops.*（设置 › 定时任务看着这台时）
    pub fn request(method: &str, params: &Value) -> crate::Reply {
        let id = || params["id"].as_str().filter(|id| !id.is_empty()).map(str::to_string).ok_or((-32602, "缺少参数 id".to_string()));
        let missing = |id: &str| (crate::NOT_FOUND, format!("没有定时任务 {id}"));
        match method {
            "loops.list" => {}
            "loops.setPaused" => {
                let id = id()?;
                let paused = params["paused"].as_bool().ok_or((-32602, "缺少参数 paused（布尔）".to_string()))?;
                let found = with(|loops| {
                    let Some(task) = loops.tasks.iter_mut().find(|task| task.id == id) else { return false };
                    if task.paused != paused {
                        task.paused = paused;
                        // 恢复时错过的不补：从现在算下一次
                        let now = Utc::now();
                        if !paused && task.next_run.is_none_or(|next| next < now) {
                            task.next_run = if matches!(task.trigger, Trigger::Itself | Trigger::Interval(_)) {
                                Some(now)
                            } else {
                                task.trigger.next_after(now)
                            };
                        }
                        save(loops);
                    }
                    true
                });
                if !found {
                    return Err(missing(&id));
                }
                std::thread::spawn(tick);
            }
            "loops.runNow" => {
                let id = id()?;
                let found = with(|loops| {
                    let Some(task) = loops.tasks.iter_mut().find(|task| task.id == id) else { return false };
                    if !task.running() {
                        task.next_run = Some(Utc::now());
                        task.paused = false;
                        save(loops);
                    }
                    true
                });
                if !found {
                    return Err(missing(&id));
                }
                tick();
            }
            "loops.remove" => {
                let id = id()?;
                let removed = with(|loops| {
                    let before = loops.tasks.len();
                    loops.tasks.retain(|task| task.id != id);
                    let removed = loops.tasks.len() != before;
                    if removed {
                        save(loops);
                    }
                    removed
                });
                if !removed {
                    return Err(missing(&id));
                }
            }
            "loops.command" => {
                let text = params["text"].as_str().unwrap_or("");
                let conversation = params["conversationID"].as_str().unwrap_or("");
                return Ok(json!({ "reply": handle(text, conversation, params["by"].as_str().unwrap_or("user"), false) }));
            }
            "loops.tick" => {
                std::thread::spawn(tick);
            }
            "loops.forget" => forget_conversation(params["conversationID"].as_str().unwrap_or("")),
            other => return Err((-32601, format!("没有这个方法：{other}"))),
        }
        Ok(state())
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn parses_like_friday() {
            let now = Local.with_ymd_and_hms(2026, 10, 15, 10, 0, 0).unwrap().with_timezone(&Utc);
            let create = |text: &str| match parse(text, now) {
                Ok(Command::Create(trigger, content)) => (trigger, content),
                _ => panic!("没认出：{text}"),
            };
            assert_eq!(create("/loop 10m 看 CI"), (Trigger::Interval(10), "看 CI".into()));
            assert_eq!(create("/loop 每2小时 看看").0, Trigger::Interval(120));
            assert_eq!(create("/loop every 15 minutes check").0, Trigger::Interval(15));
            assert_eq!(create("/loop 盯着 CI，过了告诉我").0, Trigger::Itself);
            assert_eq!(create("/loop 每天9:00 写日报").0, Trigger::Weekly { days: EVERY_DAY, minute_of_day: 540 });
            assert_eq!(create("/loop 工作日 9点半 开会").0, Trigger::Weekly { days: WORKDAYS, minute_of_day: 570 });
            assert_eq!(create("/loop 每周一三10:00 周会").0, Trigger::Weekly { days: 0b101, minute_of_day: 600 });
            assert_eq!(create("/loop 周末下午3点 打球").0, Trigger::Weekly { days: WEEKEND, minute_of_day: 900 });
            let tomorrow = Local.with_ymd_and_hms(2026, 10, 16, 8, 0, 0).unwrap().with_timezone(&Utc);
            assert_eq!(create("/loop 明天8:00 叫我").0, Trigger::Once(tomorrow));
            let afternoon = Local.with_ymd_and_hms(2026, 10, 20, 14, 0, 0).unwrap().with_timezone(&Utc);
            assert_eq!(create("/loop 10月20日 下午2点 交报告").0, Trigger::Once(afternoon));
            assert_eq!(create("/loop 30分钟后 提醒我").0, Trigger::Once(now + chrono::Duration::minutes(30)));
            // 只写钟点：过了算明天
            let bare = Local.with_ymd_and_hms(2026, 10, 16, 9, 30, 0).unwrap().with_timezone(&Utc);
            assert_eq!(create("/loop 9:30 起床").0, Trigger::Once(bare));
            assert!(matches!(parse("/loop", now), Ok(Command::List)));
            assert!(matches!(parse("/loop stop 3", now), Ok(Command::Stop { number: 3, all: false })));
            assert!(matches!(parse("/loop stop 全部", now), Ok(Command::Stop { all: true, .. })));
            assert!(parse("/loop 10m", now).is_err());
            assert!(
                parse("/loop 10-01 8:00 过去了", now)
                    .is_ok_and(|command| matches!(command, Command::Create(Trigger::Once(at), _) if at.year() == 2027))
            );
            // 回复里的指令
            assert!(matches!(directive("看过了。\n/loop next 20m"), Some(Command::Next(1200))));
            assert!(matches!(directive("办完了\n`/loop done`"), Some(Command::Done)));
            assert!(directive("正文里提到 /loop done 不算").is_none());
            assert_eq!(without_directives("结果\n/loop next 2h\n"), "结果");
            // 下一次
            let weekly = Trigger::Weekly { days: WORKDAYS, minute_of_day: 540 };
            let saturday = Local.with_ymd_and_hms(2026, 10, 17, 12, 0, 0).unwrap().with_timezone(&Utc);
            let monday = Local.with_ymd_and_hms(2026, 10, 19, 9, 0, 0).unwrap().with_timezone(&Utc);
            assert_eq!(weekly.next_after(saturday), Some(monday));
            assert_eq!(weekly.describe(), "工作日 9:00");
        }
    }
}

// MARK: - 自定义脚本工具

mod scripts {
    //! 自定义脚本工具（照 Friday 的 ScriptTools / MetaTools，Swift / Qt 同一个格式，存储用 friday-core 的 script_tools）：
    //! 每个工具是 `~/.edith/friday/tools/<名字>/`（tool.json、main.py|sh|js、work/、.versions/vN/；`FRIDAY_TOOLS_DIR` 可以换）。
    //! 启用了的是常驻那份的动作（`friday__<名字>`），参数 JSON 从标准输入进、结果从标准输出出，按权限加沙盒（macOS sandbox-exec，
    //! Linux 有 bwrap 时用它），声明的密钥从 EDITH 的密钥库按同名环境变量注入，60 秒超时。智能体用 create_tool / update_tool /
    //! delete_tool / list_tools / read_tool 自己建、改（存了就用 test_arguments 自测，通过才启用）；设置 › 工具 看着这台时也在这里改。
    //! 工具变了：各个大脑收 capability.update（动作换一份），网关收 scripts（设置页重列）。

    use std::io::{Read, Write};
    use std::path::{Path, PathBuf};
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    use friday_core::script_tools;
    use serde_json::{Map, Value, json};

    const TIMEOUT: Duration = Duration::from_secs(60);
    /// 不能用的名字（常驻那份自己的动作）
    const RESERVED: [&str; 6] = ["loop", "create_tool", "update_tool", "delete_tool", "list_tools", "read_tool"];

    pub fn root() -> PathBuf {
        match std::env::var_os("FRIDAY_TOOLS_DIR").filter(|dir| !dir.is_empty()) {
            Some(dir) => PathBuf::from(dir),
            None => crate::serve_dir().join("tools"),
        }
    }

    /// 自己建的 + 启用的插件带来的（插件的只读；和自己的重名的不载入）
    fn all() -> Vec<Value> {
        let mut tools = script_tools::load_all(&root());
        for (plugin, directory) in crate::plugins::tools_dirs() {
            for mut tool in script_tools::load_all(&directory) {
                if tools.iter().any(|other| other["manifest"]["name"] == tool["manifest"]["name"]) {
                    continue;
                }
                tool["plugin"] = json!(plugin);
                tools.push(tool);
            }
        }
        tools
    }

    /// 自己建的（能改、能删）
    fn own(name: &str) -> Option<Value> {
        script_tools::is_valid_name(name).then(|| script_tools::load(&root().join(name))).flatten()
    }

    fn load(name: &str) -> Option<Value> {
        own(name).or_else(|| all().into_iter().find(|tool| tool["manifest"]["name"] == json!(name)))
    }

    /// 插件带来的：不能在这里改
    fn plugin_of(name: &str) -> Option<String> {
        own(name).is_none().then(|| load(name).and_then(|tool| tool["plugin"].as_str().map(str::to_string))).flatten()
    }

    fn usable(tool: &Value) -> bool {
        let name = tool["manifest"]["name"].as_str().unwrap_or("");
        !RESERVED.contains(&name) && !crate::browser_tools::is_tool(name)
    }

    // MARK: 跑

    struct Run {
        exit_code: i64,
        stdout: String,
        stderr: String,
        timed_out: bool,
    }

    impl Run {
        fn passed(&self) -> bool {
            self.exit_code == 0 && !self.timed_out
        }

        fn summary(&self) -> String {
            script_tools::run_summary(self.exit_code, &self.stdout, &self.stderr, self.timed_out, TIMEOUT.as_secs_f64())
        }
    }

    fn which(name: &str) -> Option<PathBuf> {
        let suffixes: &[&str] = if cfg!(windows) { &[".exe", ".cmd", ".bat", ""] } else { &[""] };
        std::env::split_paths(&std::env::var_os("PATH")?)
            .flat_map(|dir| suffixes.iter().map(move |suffix| dir.join(format!("{name}{suffix}"))))
            .find(|path| path.is_file())
    }

    /// 解释器：python3 / bash / node（Windows 上 python 试 python3、python、py，跳过 WindowsApps 里的占位；bash 找 Git Bash）
    fn interpreter(language: &str) -> Option<PathBuf> {
        match language {
            "bash" if cfg!(windows) => {
                let candidates =
                    ["ProgramFiles", "ProgramFiles(x86)", "LOCALAPPDATA"].iter().filter_map(std::env::var_os).flat_map(|base| {
                        [
                            PathBuf::from(&base).join("Git").join("bin").join("bash.exe"),
                            PathBuf::from(&base).join("Programs").join("Git").join("bin").join("bash.exe"),
                        ]
                    });
                candidates.into_iter().find(|path| path.is_file()).or_else(|| which("bash"))
            }
            "bash" => Some(PathBuf::from("/bin/bash")).filter(|path| path.is_file()).or_else(|| which("bash")),
            "node" => which("node"),
            _ if cfg!(windows) => ["python3", "python", "py"]
                .iter()
                .filter_map(|name| which(name))
                .find(|path| !path.to_string_lossy().contains("WindowsApps")),
            _ => which("python3"),
        }
    }

    fn title_of(language: &str) -> &'static str {
        match language {
            "bash" => "Bash",
            "node" => "JavaScript (Node)",
            _ => "Python",
        }
    }

    /// 按权限加的沙盒：只读 / 联网级别不能写文件（临时目录与 work/ 除外），只读 / 写文件级别不能联网，execute 级别不加
    fn sandbox(permission: &str, writable: &Path) -> Vec<String> {
        if permission == "execute" {
            return Vec::new();
        }
        if cfg!(target_os = "macos") {
            if !Path::new("/usr/bin/sandbox-exec").exists() {
                return Vec::new();
            }
            let mut rules = vec!["(version 1)".to_string(), "(allow default)".to_string()];
            if permission != "network" {
                rules.push("(deny network-outbound (remote ip))".into());
            }
            if permission != "writeFiles" {
                let path = crate::canonical(writable).to_string_lossy().replace('"', "\\\"");
                rules.push("(deny file-write*)".into());
                rules.push(format!("(allow file-write* (subpath \"/private/tmp\") (subpath \"/private/var/folders\") (subpath \"/dev\") (subpath \"{path}\"))"));
            }
            return vec!["/usr/bin/sandbox-exec".into(), "-p".into(), rules.join("\n")];
        }
        if cfg!(windows) {
            return Vec::new();
        }
        let Some(bwrap) = which("bwrap") else { return Vec::new() };
        // 有的机器上 bwrap 装了却用不了（没开用户命名空间）：试一下
        let works = Command::new(&bwrap)
            .args(["--ro-bind", "/", "/", "--dev-bind", "/dev", "/dev", "true"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success());
        if !works {
            return Vec::new();
        }
        let mut arguments = vec![bwrap.to_string_lossy().into_owned()];
        if permission == "writeFiles" {
            arguments.extend(["--bind", "/", "/", "--dev-bind", "/dev", "/dev"].map(str::to_string));
        } else {
            arguments.extend(["--ro-bind", "/", "/", "--dev-bind", "/dev", "/dev"].map(str::to_string));
            let temporary = std::env::temp_dir();
            for dir in [temporary.as_path(), Path::new("/tmp"), Path::new("/var/tmp"), writable] {
                if dir.is_dir() {
                    let path = crate::canonical(dir).to_string_lossy().into_owned();
                    arguments.extend(["--bind".to_string(), path.clone(), path]);
                }
            }
        }
        if permission != "network" {
            arguments.push("--unshare-net".into());
        }
        arguments.push("--die-with-parent".into());
        arguments
    }

    /// 脚本声明的密钥：从 EDITH 的密钥库按同名环境变量给它（别的密钥它看不到）
    fn secrets(names: &[String]) -> Vec<(String, String)> {
        let store = crate::read_json_file(&crate::edith_home().join("secrets.json"));
        names.iter().filter_map(|name| store[name].as_str().map(|value| (name.clone(), value.to_string()))).collect()
    }

    fn run(tool: &Value, arguments: &Value, caller: Option<&str>) -> Result<Run, String> {
        let manifest = &tool["manifest"];
        let language = manifest["language"].as_str().unwrap_or("python");
        let directory = PathBuf::from(tool["directory"].as_str().unwrap_or(""));
        let work = directory.join("work");
        let _ = std::fs::create_dir_all(&work);
        let entry = directory.join(script_tools::file_name(language));
        let interpreter = interpreter(language).ok_or_else(|| {
            format!("找不到 {}，请先安装 {}", if language == "python" { "python3" } else { language }, title_of(language))
        })?;
        let wrapper = sandbox(manifest["permission"].as_str().unwrap_or("execute"), &work);
        let mut command = match wrapper.split_first() {
            Some((program, rest)) => {
                let mut command = Command::new(program);
                command.args(rest).arg(&interpreter).arg(&entry);
                command
            }
            None => {
                let mut command = Command::new(&interpreter);
                command.arg(&entry);
                command
            }
        };
        command.current_dir(&work).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
        command.env("FRIDAY_TOOL_DIR", &directory).env("FRIDAY_WORK_DIR", &work);
        // 对话的工作目录：脚本的当前目录是工具自己的工作文件夹，参数里的相对路径按它解析
        if let Some(caller) = caller.filter(|caller| !caller.is_empty()) {
            command.env("FRIDAY_CWD", caller);
        }
        let names: Vec<String> =
            manifest["secrets"].as_array().into_iter().flatten().filter_map(Value::as_str).map(str::to_string).collect();
        command.envs(secrets(&names));
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
            command.creation_flags(0x0800_0000);
        }
        let mut child = command.spawn().map_err(|error| format!("无法启动 {}：{error}", interpreter.display()))?;
        let input = serde_json::to_vec(if arguments.is_null() { &Value::Null } else { arguments }).unwrap_or_default();
        if let Some(mut stdin) = child.stdin.take() {
            std::thread::spawn(move || {
                let _ = stdin.write_all(&input);
            });
        }
        let read = |pipe: Option<Box<dyn Read + Send>>| {
            std::thread::spawn(move || {
                let mut text = Vec::new();
                if let Some(mut pipe) = pipe {
                    let _ = pipe.read_to_end(&mut text);
                }
                String::from_utf8_lossy(&text).trim().to_string()
            })
        };
        let stdout = read(child.stdout.take().map(|pipe| Box::new(pipe) as Box<dyn Read + Send>));
        let stderr = read(child.stderr.take().map(|pipe| Box::new(pipe) as Box<dyn Read + Send>));
        let started = Instant::now();
        let (code, timed_out) = loop {
            match child.try_wait() {
                Ok(Some(status)) => break (status.code().map(i64::from).unwrap_or(-1), false),
                Ok(None) if started.elapsed() >= TIMEOUT => {
                    let _ = child.kill();
                    let _ = child.wait();
                    break (-1, true);
                }
                Ok(None) => std::thread::sleep(Duration::from_millis(50)),
                Err(error) => return Err(error.to_string()),
            }
        };
        Ok(Run { exit_code: code, stdout: stdout.join().unwrap_or_default(), stderr: stderr.join().unwrap_or_default(), timed_out })
    }

    /// 存成新版本（先关着），用 testArguments 自测，通过才启用；回 (存好的工具, 自测)
    fn save_and_test(manifest: &Map<String, Value>, code: &str, caller: Option<&str>) -> Result<(Value, Run), String> {
        let mut pending = manifest.clone();
        pending.insert("enabled".into(), json!(false));
        let mut tool = script_tools::save(&root(), &pending, code)?;
        let arguments =
            Some(tool["manifest"]["testArguments"].clone()).filter(|arguments| !arguments.is_null()).unwrap_or_else(|| json!({}));
        let test = run(&tool, &arguments, caller).unwrap_or_else(|error| Run {
            exit_code: -1,
            stdout: String::new(),
            stderr: error,
            timed_out: false,
        });
        let mut manifest = tool["manifest"].as_object().cloned().unwrap_or_default();
        manifest.insert(
            "lastTest".into(),
            json!({ "passed": test.passed(), "output": test.summary(), "date": friday_core::archive::now_iso8601() }),
        );
        manifest.insert("enabled".into(), json!(test.passed()));
        script_tools::update_manifest(&root(), &manifest)?;
        tool["manifest"] = Value::Object(manifest);
        changed();
        Ok((tool, test))
    }

    // MARK: 动作

    /// 启用了的脚本工具：常驻那份的动作
    pub fn actions() -> Vec<Value> {
        all()
            .into_iter()
            .filter(|tool| tool["manifest"]["enabled"] == true && usable(tool))
            .map(|tool| {
                let manifest = &tool["manifest"];
                json!({
                    "name": manifest["name"],
                    "description": manifest["description"],
                    "parameters": manifest["parameters"],
                    "readOnly": manifest["permission"] == "readOnly",
                    "timeoutSeconds": 90,
                })
            })
            .collect()
    }

    fn property(description: &str, kind: &str) -> Value {
        json!({ "type": kind, "description": description })
    }

    /// 扩展自己的能力（同 Friday 的 MetaTools；说明同 prompts-tools.md 的 tool.create_tool……）
    pub fn meta_actions() -> Vec<Value> {
        let permission = "readOnly（只读）| network（需要联网）| writeFiles（需要写文件）| execute（需要执行任意命令，不受沙盒限制）";
        let code = "完整脚本源码：从标准输入读取 JSON 参数，把结果打印到标准输出；出错时写标准错误并以非零状态码退出。当前目录是工具自己的工作文件夹，参数里的相对路径按环境变量 FRIDAY_CWD（对话的工作目录）解析";
        let fields = |update: bool| {
            let mut properties = Map::new();
            properties.insert(
                "name".into(),
                property(
                    if update {
                        "要修改的工具名"
                    } else {
                        "工具名，小写字母开头，只含小写字母、数字、下划线，如 get_weather"
                    },
                    "string",
                ),
            );
            if !update {
                properties.insert("title".into(), property("给用户看的中文名称", "string"));
            }
            properties.insert(
                "description".into(),
                property(if update { "新的描述" } else { "工具做什么、什么时候该用它，写清楚参数含义" }, "string"),
            );
            properties.insert("parameters".into(), property(if update { "新的参数 JSON Schema" } else { "参数的 JSON Schema，例如 {\"type\":\"object\",\"properties\":{\"city\":{\"type\":\"string\",\"description\":\"城市\"}},\"required\":[\"city\"]}" }, "object"));
            properties.insert("language".into(), property("python | bash | node", "string"));
            properties.insert("code".into(), property(if update { "新的完整源码" } else { code }, "string"));
            properties.insert("permission".into(), property(permission, "string"));
            properties.insert("secrets".into(), json!({ "type": "array", "items": { "type": "string" }, "description": if update { "新的密钥名数组" } else { "脚本需要的密钥名数组（如 [\"DASHSCOPE_API_KEY\"]），运行时以同名环境变量注入；没有的密钥要用户在设置 › 服务与密钥里加" } }));
            properties.insert("test_arguments".into(), property(if update { "新的自测参数" } else { "用于自测的一组真实参数" }, "object"));
            let required: Vec<&str> = if update {
                vec!["name"]
            } else {
                vec!["name", "description", "parameters", "language", "code", "permission", "test_arguments"]
            };
            json!({ "type": "object", "properties": properties, "required": required })
        };
        let name_only = |description: &str| json!({ "type": "object", "properties": { "name": property(description, "string") }, "required": ["name"] });
        vec![
            json!({ "name": "create_tool", "description": "当现有工具无法完成任务时，编写一个新的脚本工具并注册。创建后会自动用 test_arguments 自测，通过就启用，马上就能调用。", "parameters": fields(false), "timeoutSeconds": 120 }),
            json!({ "name": "update_tool", "description": "修改已有的自定义工具（例如修复自测失败的代码）。只需提供要改的字段，旧版本会自动保留以便回滚。", "parameters": fields(true), "timeoutSeconds": 120 }),
            json!({ "name": "delete_tool", "description": "删除一个自定义工具（移到工具目录的 .trash 里）。", "parameters": name_only("要删除的工具名") }),
            json!({ "name": "list_tools", "description": "列出当前所有工具（内置、自定义脚本、MCP）及其状态。", "parameters": { "type": "object", "properties": {} }, "readOnly": true }),
            json!({ "name": "read_tool", "description": "查看某个自定义工具的清单和源码，修改前先读一遍。", "parameters": name_only("工具名"), "readOnly": true }),
        ]
    }

    pub fn is_meta(action: &str) -> bool {
        RESERVED[1..].contains(&action)
    }

    pub fn is_script(action: &str) -> bool {
        load(action).is_some_and(|tool| tool["manifest"]["enabled"] == true && usable(&tool))
    }

    /// 兜底修正模型写的 schema：必须是 type=object
    fn normalized_schema(value: Option<&Value>) -> Value {
        let mut schema = value.and_then(Value::as_object).cloned().unwrap_or_default();
        schema.entry("type").or_insert(json!("object"));
        if !schema.contains_key("properties") {
            // 有些模型直接给出 properties 本身
            let looks = schema.len() > 1 && schema.iter().filter(|(key, _)| *key != "type").all(|(_, value)| value.get("type").is_some());
            if looks {
                let properties: Map<String, Value> =
                    schema.iter().filter(|(key, _)| *key != "type").map(|(key, value)| (key.clone(), value.clone())).collect();
                schema = Map::new();
                schema.insert("type".into(), json!("object"));
                schema.insert("properties".into(), Value::Object(properties));
            } else {
                schema.insert("properties".into(), json!({}));
            }
        }
        Value::Object(schema)
    }

    fn language(value: &str) -> Option<&'static str> {
        match value.trim().to_lowercase().as_str() {
            "python" | "python3" | "py" => Some("python"),
            "bash" | "sh" | "shell" | "zsh" => Some("bash"),
            "node" | "javascript" | "js" | "nodejs" => Some("node"),
            _ => None,
        }
    }

    fn secret_names(value: Option<&Value>) -> Option<Vec<Value>> {
        let names: Vec<String> = match value? {
            Value::Array(items) => items.iter().filter_map(Value::as_str).map(str::to_string).collect(),
            Value::String(text) => text.split([',', ' ']).filter(|name| !name.is_empty()).map(str::to_string).collect(),
            _ => return None,
        };
        Some(names.into_iter().map(|name| json!(name.trim().to_uppercase())).collect())
    }

    fn code_of(arguments: &Map<String, Value>) -> Option<String> {
        let code = arguments.get("code")?.as_str()?;
        if code.trim().is_empty() {
            return None;
        }
        Some(if code.ends_with('\n') { code.to_string() } else { format!("{code}\n") })
    }

    fn report(tool: &Value, test: &Run, created: bool) -> (bool, String) {
        let name = tool["manifest"]["name"].as_str().unwrap_or("");
        let version = tool["manifest"]["version"].as_i64().unwrap_or(1);
        let action = if created { "已创建".to_string() } else { format!("已更新到 v{version}") };
        if test.passed() {
            (true, format!("工具 {name} {action}，自测通过，已载入。\n自测输出：\n{}", test.summary()))
        } else {
            (
                false,
                format!(
                    "工具 {name} {action}（已保存为 v{version}），但自测失败，暂未启用。请用 update_tool（name 仍为 {name}）修正后再试：\n{}",
                    test.summary()
                ),
            )
        }
    }

    /// 智能体的动作：脚本工具、扩展自己的能力（回 (成没成, 给模型看的, 产出的文件)）
    pub fn act(action: &str, arguments: &Map<String, Value>, caller: Option<&str>, brain: &crate::Brain) -> (bool, String, Vec<String>) {
        let text =
            |key: &str| arguments.get(key).and_then(Value::as_str).map(str::trim).filter(|text| !text.is_empty()).map(str::to_string);
        let failed = |message: String| (false, message, Vec::new());
        match action {
            "list_tools" => {
                let listing = brain.request("tools.list", json!({}), Duration::from_secs(15)).unwrap_or_default();
                let disabled: Vec<&str> = listing["disabled"].as_array().into_iter().flatten().filter_map(Value::as_str).collect();
                let mut lines: Vec<String> = listing["tools"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter(|tool| {
                        !tool["name"].as_str().is_some_and(|name| name.starts_with("friday__") && is_script(&name["friday__".len()..]))
                    })
                    .map(|tool| {
                        let name = tool["name"].as_str().unwrap_or("");
                        let first = tool["description"].as_str().unwrap_or("").split(['。', '\n']).next().unwrap_or("").to_string();
                        let state = if disabled.contains(&name) { "［已停用］" } else { "" };
                        format!(
                            "- {name}（{}，{}）{state}：{first}",
                            tool["group"].as_str().unwrap_or("builtin"),
                            if tool["readOnly"] == true { "只读" } else { "会改东西" }
                        )
                    })
                    .collect();
                for tool in all() {
                    let manifest = &tool["manifest"];
                    let state = if manifest["enabled"] == true { "" } else { "［已停用］" };
                    lines.push(format!(
                        "- friday__{}（自定义脚本，{}）{state}：{}",
                        manifest["name"].as_str().unwrap_or(""),
                        manifest["permission"].as_str().unwrap_or(""),
                        manifest["description"].as_str().unwrap_or("")
                    ));
                }
                (true, lines.join("\n"), Vec::new())
            }
            "read_tool" => {
                let Some(tool) = text("name").and_then(|name| load(&name)) else {
                    return failed(format!("没有名为 {} 的自定义工具（内置和 MCP 工具不能查看源码）", text("name").unwrap_or_default()));
                };
                let manifest = &tool["manifest"];
                let mut out = format!(
                    "清单：\n{}\n权限：{}，语言：{}，版本：v{}\n",
                    serde_json::to_string_pretty(&manifest["parameters"]).unwrap_or_default(),
                    manifest["permission"].as_str().unwrap_or(""),
                    manifest["language"].as_str().unwrap_or(""),
                    manifest["version"]
                );
                if manifest["lastTest"].is_object() {
                    out += &format!(
                        "上次自测：{}\n{}\n",
                        if manifest["lastTest"]["passed"] == true { "通过" } else { "失败" },
                        manifest["lastTest"]["output"].as_str().unwrap_or("")
                    );
                }
                (true, out + "源码：\n" + tool["code"].as_str().unwrap_or(""), Vec::new())
            }
            "create_tool" => {
                let Some(name) = text("name") else { return failed("缺少参数 name".into()) };
                if !script_tools::is_valid_name(&name) || RESERVED.contains(&name.as_str()) || crate::browser_tools::is_tool(&name) {
                    return failed(format!(
                        "工具名 {name} 不合法：需小写字母开头，只含小写字母、数字、下划线，3–41 个字符，也不能和已有的工具重名"
                    ));
                }
                if load(&name).is_some() {
                    return failed(format!("已存在名为 {name} 的工具，请换个名字，或用 update_tool 修改"));
                }
                let Some(language) = text("language").and_then(|value| language(&value)) else {
                    return failed("language 只能是 python | bash | node".into());
                };
                let permission = text("permission").unwrap_or_else(|| "readOnly".into());
                if !script_tools::PERMISSIONS.contains(&permission.as_str()) {
                    return failed("permission 只能是 readOnly | network | writeFiles | execute".into());
                }
                let Some(description) = text("description") else { return failed("缺少参数 description".into()) };
                let Some(code) = code_of(arguments) else { return failed("缺少参数 code".into()) };
                let mut manifest = Map::new();
                manifest.insert("name".into(), json!(name));
                manifest.insert("title".into(), json!(text("title").unwrap_or_else(|| name.clone())));
                manifest.insert("description".into(), json!(description));
                manifest.insert("permission".into(), json!(permission));
                manifest.insert("language".into(), json!(language));
                manifest.insert("parameters".into(), normalized_schema(arguments.get("parameters")));
                manifest.insert(
                    "testArguments".into(),
                    arguments.get("test_arguments").filter(|value| value.is_object()).cloned().unwrap_or(json!({})),
                );
                if let Some(secrets) = secret_names(arguments.get("secrets")) {
                    manifest.insert("secrets".into(), json!(secrets));
                }
                manifest.insert("createdBy".into(), json!("agent"));
                match save_and_test(&manifest, &code, caller) {
                    Ok((tool, test)) => {
                        let (ok, text) = report(&tool, &test, true);
                        (ok, text, Vec::new())
                    }
                    Err(error) => failed(error),
                }
            }
            "update_tool" => {
                let Some(name) = text("name") else { return failed("缺少参数 name".into()) };
                if let Some(plugin) = plugin_of(&name) {
                    return failed(format!("工具 {name} 是插件「{plugin}」带来的，不能改：要改就改插件，或者换个名字新建"));
                }
                let Some(existing) = own(&name) else {
                    return failed(format!("没有名为 {name} 的自定义工具，请用 create_tool 创建"));
                };
                let mut manifest = existing["manifest"].as_object().cloned().unwrap_or_default();
                if let Some(description) = text("description") {
                    manifest.insert("description".into(), json!(description));
                }
                if let Some(value) = text("language") {
                    let Some(language) = language(&value) else { return failed("language 只能是 python | bash | node".into()) };
                    manifest.insert("language".into(), json!(language));
                }
                if let Some(permission) = text("permission") {
                    if !script_tools::PERMISSIONS.contains(&permission.as_str()) {
                        return failed("permission 只能是 readOnly | network | writeFiles | execute".into());
                    }
                    manifest.insert("permission".into(), json!(permission));
                }
                if arguments.contains_key("parameters") {
                    manifest.insert("parameters".into(), normalized_schema(arguments.get("parameters")));
                }
                if let Some(test) = arguments.get("test_arguments").filter(|value| value.is_object()) {
                    manifest.insert("testArguments".into(), test.clone());
                }
                if arguments.contains_key("secrets")
                    && let Some(secrets) = secret_names(arguments.get("secrets"))
                {
                    manifest.insert("secrets".into(), json!(secrets));
                }
                manifest.insert("createdBy".into(), json!("agent"));
                let code = code_of(arguments).unwrap_or_else(|| existing["code"].as_str().unwrap_or("").to_string());
                match save_and_test(&manifest, &code, caller) {
                    Ok((tool, test)) => {
                        let (ok, text) = report(&tool, &test, false);
                        (ok, text, Vec::new())
                    }
                    Err(error) => failed(error),
                }
            }
            "delete_tool" => {
                let Some(name) = text("name") else { return failed("缺少参数 name".into()) };
                match remove(&name) {
                    Ok(()) => (true, format!("已删除工具 {name}（已移到工具目录的 .trash 里）。"), Vec::new()),
                    Err(error) => failed(error),
                }
            }
            name => {
                let Some(tool) = load(name).filter(|tool| tool["manifest"]["enabled"] == true) else {
                    return failed(format!("没有启用的工具 {name}"));
                };
                match run(&tool, &Value::Object(arguments.clone()), caller) {
                    Ok(result) => {
                        let work = PathBuf::from(tool["directory"].as_str().unwrap_or("")).join("work");
                        let media = if result.passed() {
                            script_tools::media_paths(&result.stdout, &work, &crate::user_home())
                        } else {
                            Vec::new()
                        };
                        (result.passed(), result.summary(), media)
                    }
                    Err(error) => failed(error),
                }
            }
        }
    }

    /// 移到工具目录的 .trash 里（误删还能找回来）
    fn remove(name: &str) -> Result<(), String> {
        if let Some(plugin) = plugin_of(name) {
            return Err(format!("工具 {name} 是插件「{plugin}」带来的，不能单独删：不想要就在 设置 › 插件 里停用或卸载这个插件"));
        }
        let directory = root().join(name);
        if !script_tools::is_valid_name(name) || !directory.is_dir() {
            return Err(format!("没有名为 {name} 的自定义工具"));
        }
        let trash = root().join(".trash");
        std::fs::create_dir_all(&trash).map_err(|error| error.to_string())?;
        let target = trash.join(format!("{name}-{}", chrono::Local::now().format("%Y%m%d-%H%M%S")));
        std::fs::rename(&directory, &target).map_err(|error| format!("未能将“{name}”移走：{error}"))?;
        changed();
        Ok(())
    }

    /// 工具变了：各个大脑换一份动作，网关的设置页重列
    fn changed() {
        crate::serve_update_actions();
        crate::notify_all_clients("scripts", json!({}));
    }

    // MARK: 设置 › 工具

    /// ScriptTool（同 Friday：清单的各键 + code、directory、entry、hasLastTest、lastTest……）
    fn tool_json(tool: &Value) -> Value {
        let manifest = tool["manifest"].as_object().cloned().unwrap_or_default();
        let language = manifest.get("language").and_then(Value::as_str).unwrap_or("python").to_string();
        let permission = manifest.get("permission").and_then(Value::as_str).unwrap_or("readOnly").to_string();
        let directory = tool["directory"].as_str().unwrap_or("").to_string();
        let mut value = Value::Object(manifest.clone());
        let (permission_title, permission_symbol) = match permission.as_str() {
            "readOnly" => ("只读", "eye"),
            "network" => ("需要联网", "network"),
            "writeFiles" => ("需要写文件", "square.and.pencil"),
            _ => ("需要执行命令", "terminal"),
        };
        value["id"] = manifest.get("name").cloned().unwrap_or_default();
        value["manifest"] = Value::Object(manifest.clone());
        value["permissionTitle"] = json!(permission_title);
        value["permissionSymbol"] = json!(permission_symbol);
        value["languageTitle"] = json!(title_of(&language));
        value["fileName"] = json!(script_tools::file_name(&language));
        value["parametersText"] =
            json!(serde_json::to_string_pretty(&manifest.get("parameters").cloned().unwrap_or(Value::Null)).unwrap_or_default());
        value["testArgumentsText"] = json!(
            manifest.get("testArguments").map(|arguments| serde_json::to_string_pretty(arguments).unwrap_or_default()).unwrap_or_default()
        );
        value["hasLastTest"] = json!(manifest.get("lastTest").is_some_and(Value::is_object));
        value["lastTest"] = manifest.get("lastTest").cloned().unwrap_or(json!({}));
        value["secrets"] = manifest.get("secrets").cloned().unwrap_or(json!([]));
        value["code"] = tool["code"].clone();
        value["directory"] = json!(directory);
        value["entry"] = json!(format!("{directory}/{}", script_tools::file_name(&language)));
        value["workDirectory"] = json!(format!("{directory}/work"));
        value["plugin"] = json!(tool["plugin"].as_str().unwrap_or(""));
        value
    }

    pub fn state() -> Value {
        json!({ "scriptTools": all().iter().map(tool_json).collect::<Vec<_>>(), "toolsDirectory": root().to_string_lossy() })
    }

    /// 新建表单的模板源码（按这台的系统给）
    fn template(language: &str) -> String {
        match language {
            "bash" if cfg!(target_os = "macos") => "#!/bin/bash\n# 参数 JSON 从标准输入读入；macOS 自带 plutil 可以解析 JSON\ninput=$(cat)\ntext=$(echo \"$input\" | plutil -extract text raw -o - - 2>/dev/null)\necho \"{\\\"result\\\": \\\"$text\\\"}\"\n".into(),
            "bash" => "#!/bin/bash\n# 参数 JSON 从标准输入读入；系统自带的 sed 可以取出简单的字符串字段\ninput=$(cat)\ntext=$(echo \"$input\" | sed -n 's/.*\"text\"[[:space:]]*:[[:space:]]*\"\\([^\"]*\\)\".*/\\1/p')\necho \"{\\\"result\\\": \\\"$text\\\"}\"\n".into(),
            "node" => "let input = \"\";\nprocess.stdin.on(\"data\", (chunk) => (input += chunk));\nprocess.stdin.on(\"end\", () => {\n  const args = JSON.parse(input || \"{}\");\n  console.log(JSON.stringify({ result: String(args.text ?? \"\").toUpperCase() }));\n});\n".into(),
            _ => "import json, sys\n\nargs = json.load(sys.stdin)\ntext = args.get(\"text\", \"\")\nprint(json.dumps({\"result\": text.upper()}, ensure_ascii=False))\n".into(),
        }
    }

    /// 界面传来的清单（键同 ScriptTool；parametersText / testArgumentsText 是 JSON 文本）叠到这台已有的上
    fn manifest_from(given: &Map<String, Value>, base: Option<&Value>) -> Result<Map<String, Value>, String> {
        let mut manifest = base.and_then(|base| base["manifest"].as_object().cloned()).unwrap_or_default();
        for key in ["name", "title", "description", "symbol", "permission", "language", "enabled", "secrets"] {
            if let Some(value) = given.get(key).filter(|value| !value.is_null()) {
                manifest.insert(key.into(), value.clone());
            }
        }
        if let Some(text) = given.get("parametersText").and_then(Value::as_str) {
            manifest.insert("parameters".into(), serde_json::from_str(text).map_err(|_| "参数 schema 不是合法的 JSON".to_string())?);
        } else if let Some(parameters) = given.get("parameters") {
            manifest.insert("parameters".into(), parameters.clone());
        }
        if let Some(text) = given.get("testArgumentsText").and_then(Value::as_str) {
            if text.trim().is_empty() {
                manifest.remove("testArguments");
            } else {
                manifest.insert("testArguments".into(), serde_json::from_str(text).map_err(|_| "自测参数不是合法的 JSON".to_string())?);
            }
        }
        manifest.insert("createdBy".into(), json!("user"));
        Ok(manifest)
    }

    /// tools.*（设置 › 工具看着这台时；会马上在这台上跑代码的都会自测）
    pub fn request(method: &str, params: &Value) -> crate::Reply {
        let name =
            || params["name"].as_str().filter(|name| !name.is_empty()).map(str::to_string).ok_or((-32602, "缺少参数 name".to_string()));
        let result = |test: Run| json!({ "passed": test.passed(), "summary": test.summary() });
        match method {
            "scripts.state" => Ok(state()),
            "tools.scriptVersions" => Ok(json!({
                "versions": script_tools::versions(&root(), &name()?).iter().map(|version| {
                    let mut value = tool_json(&json!({ "manifest": version["manifest"], "code": version["code"], "directory": "" }));
                    value["id"] = version["manifest"]["version"].clone();
                    value
                }).collect::<Vec<_>>()
            })),
            "tools.saveScript" => {
                let given = params["manifest"].as_object().cloned().unwrap_or_default();
                let wanted = given.get("name").and_then(Value::as_str).unwrap_or("").to_string();
                if let Some(plugin) = plugin_of(&wanted) {
                    return Err((crate::NOT_ALLOWED, format!("工具 {wanted} 是插件「{plugin}」带来的，不能在这里改")));
                }
                let base = own(&wanted);
                let manifest = manifest_from(&given, base.as_ref()).map_err(|error| (-32602, error))?;
                let code = params["code"]
                    .as_str()
                    .map(str::to_string)
                    .or_else(|| base.as_ref().and_then(|base| base["code"].as_str().map(str::to_string)))
                    .unwrap_or_default();
                let (_, test) = save_and_test(&manifest, &code, None).map_err(|error| (-32603, error))?;
                Ok(result(test))
            }
            "tools.testScript" => {
                let tool = load(&name()?).ok_or((crate::NOT_FOUND, "没有这个工具".to_string()))?;
                let text = params["arguments"].as_str().unwrap_or("{}");
                let arguments: Value = serde_json::from_str(if text.trim().is_empty() { "{}" } else { text })
                    .map_err(|_| (-32602, "参数不是合法的 JSON".to_string()))?;
                let test = run(&tool, &arguments, None).map_err(|error| (-32603, error))?;
                // 记下这次的结果（设置页显示上次自测）
                let mut manifest = tool["manifest"].as_object().cloned().unwrap_or_default();
                manifest.insert(
                    "lastTest".into(),
                    json!({ "passed": test.passed(), "output": test.summary(), "date": friday_core::archive::now_iso8601() }),
                );
                let _ = script_tools::update_manifest(&root(), &manifest);
                changed();
                Ok(result(test))
            }
            "tools.rollback" => {
                let name = name()?;
                if let Some(plugin) = plugin_of(&name) {
                    return Err((crate::NOT_ALLOWED, format!("工具 {name} 是插件「{plugin}」带来的，不能在这里改")));
                }
                let wanted = params["version"].as_i64().unwrap_or(0);
                let version = script_tools::versions(&root(), &name)
                    .into_iter()
                    .find(|version| version["manifest"]["version"].as_i64() == Some(wanted))
                    .ok_or((crate::NOT_FOUND, format!("没有 v{wanted}")))?;
                let mut manifest = version["manifest"].as_object().cloned().unwrap_or_default();
                manifest.insert("createdBy".into(), json!("user"));
                let (_, test) = save_and_test(&manifest, version["code"].as_str().unwrap_or(""), None).map_err(|error| (-32603, error))?;
                Ok(result(test))
            }
            "tools.createScript" => {
                let fields = params["fields"].as_object().cloned().unwrap_or_default();
                let text = |key: &str| fields.get(key).and_then(Value::as_str).unwrap_or("").trim().to_string();
                let name = text("name");
                if !script_tools::is_valid_name(&name) || RESERVED.contains(&name.as_str()) {
                    return Ok(json!({ "error": "工具名要小写字母开头，只含小写字母、数字、下划线，3–41 个字符" }));
                }
                if load(&name).is_some() {
                    return Ok(json!({ "error": format!("已经有叫 {name} 的工具了") }));
                }
                let language = language(&text("language")).unwrap_or("python");
                let permission = Some(text("permission"))
                    .filter(|permission| script_tools::PERMISSIONS.contains(&permission.as_str()))
                    .unwrap_or_else(|| "readOnly".into());
                let mut manifest = Map::new();
                manifest.insert("name".into(), json!(name));
                manifest.insert("title".into(), json!(if text("title").is_empty() { name.clone() } else { text("title") }));
                manifest.insert("description".into(), json!(text("description")));
                manifest.insert("permission".into(), json!(permission));
                manifest.insert("language".into(), json!(language));
                manifest.insert(
                    "parameters".into(),
                    json!({ "type": "object", "properties": { "text": { "type": "string", "description": "输入文本" } }, "required": [] }),
                );
                manifest.insert("testArguments".into(), json!({ "text": "hello" }));
                manifest.insert("createdBy".into(), json!("user"));
                match save_and_test(&manifest, &template(language), None) {
                    Ok(_) => Ok(json!({ "error": "" })),
                    Err(error) => Ok(json!({ "error": error })),
                }
            }
            "tools.deleteScript" => remove(&name()?).map(|_| json!({})).map_err(|error| (crate::NOT_ALLOWED, error)),
            // 开关：是脚本工具的记在清单里（handled），别的交回网关（EDITH 的 disabledTools）
            "tools.setEnabled" => {
                let name = name()?;
                let short = name.strip_prefix("friday__").unwrap_or(&name).to_string();
                if let Some(plugin) = plugin_of(&short) {
                    return Err((crate::NOT_ALLOWED, format!("工具 {short} 是插件「{plugin}」带来的：在 设置 › 插件 里停用这个插件")));
                }
                let Some(tool) = own(&short) else { return Ok(json!({ "handled": false })) };
                let mut manifest = tool["manifest"].as_object().cloned().unwrap_or_default();
                manifest.insert("enabled".into(), json!(params["enabled"].as_bool().unwrap_or(true)));
                script_tools::update_manifest(&root(), &manifest).map_err(|error| (-32603, error))?;
                changed();
                Ok(json!({ "handled": true }))
            }
            other => Err((-32601, format!("没有这个方法：{other}"))),
        }
    }
}

// MARK: - 钩子（用户配的命令）

mod hooks {
    //! 钩子（照 Friday 的 Hooks，Friday 仓库的 qt/docs/HOOKS.md；格式同 Claude Code）：这台全局的 `~/.edith/friday/hooks.json`、
    //! 项目里的 `.friday/hooks.json`（仓库带来的命令：信任了、内容没变才跑）、启用的插件带的。大脑在四个时机问常驻的那份（能力协议 §6.7
    //! 的 hook），这里跑匹配的命令：标准输入是 Claude Code 那个 JSON，退出码 2 拦下 / 接着做，JSON 输出按 Claude Code 的键认。

    use std::collections::HashMap;
    use std::io::{Read, Write};
    use std::path::{Path, PathBuf};
    use std::process::{Command, Stdio};
    use std::sync::{LazyLock, Mutex};
    use std::time::{Duration, Instant};

    use regex::Regex;
    use serde_json::{Map, Value, json};
    use sha2::Digest;

    const MAX_RECORDS: usize = 20;
    const MAX_OUTPUT: usize = 2000;
    const MAX_FEEDBACK: usize = 4000;
    const DEFAULT_TIMEOUT: u64 = 60;
    const MAX_TIMEOUT: u64 = 600;
    pub const EVENTS: [&str; 4] = ["UserPromptSubmit", "PreToolUse", "PostToolUse", "Stop"];

    fn event_title(event: &str) -> &'static str {
        match event {
            "PreToolUse" => "调用工具前",
            "PostToolUse" => "调用工具后",
            "Stop" => "一轮结束",
            _ => "用户发消息",
        }
    }

    #[derive(Clone)]
    struct Group {
        event: String,
        matcher: String,
        commands: Vec<(String, u64)>,
        source: String,
        /// 插件带来的：插件的文件夹（${CLAUDE_PLUGIN_ROOT}）
        plugin_root: Option<PathBuf>,
    }

    #[derive(Clone, Default)]
    struct Config {
        exists: bool,
        groups: Vec<Group>,
        problems: Vec<String>,
        hash: String,
    }

    fn cut(text: &str, limit: usize) -> String {
        let count = text.chars().count();
        if count <= limit {
            text.to_string()
        } else {
            format!("{}…（后面还有 {} 字）", text.chars().take(limit).collect::<String>(), count - limit)
        }
    }

    /// 解析一份 hooks.json（和 Claude Code 的 settings.json 一样放在 hooks 下；直接写时机名的也认）
    fn parse(data: &[u8], source: &str) -> Config {
        let mut config = Config {
            exists: true,
            hash: sha2::Sha256::digest(data).iter().map(|byte| format!("{byte:02x}")).collect(),
            ..Default::default()
        };
        if String::from_utf8_lossy(data).trim().is_empty() {
            return config;
        }
        let document: Value = match serde_json::from_slice(data) {
            Ok(Value::Object(object)) => Value::Object(object),
            Ok(_) => {
                config.problems.push("不是 JSON 对象：最外层要是 {…}".into());
                return config;
            }
            Err(error) => {
                config.problems.push(format!("不是 JSON 对象：{error}"));
                return config;
            }
        };
        let events = if document.get("hooks").is_some() { document["hooks"].clone() } else { document };
        for (event, groups) in events.as_object().into_iter().flatten() {
            if !EVENTS.contains(&event.as_str()) {
                config.problems.push(format!("不认识的时机「{event}」（认 UserPromptSubmit、PreToolUse、PostToolUse、Stop），跳过"));
                continue;
            }
            let Some(groups) = groups.as_array() else {
                config.problems.push(format!("{event} 要是一个数组"));
                continue;
            };
            for item in groups {
                let matcher = item["matcher"].as_str().unwrap_or("").trim().to_string();
                let mut commands = Vec::new();
                for hook in item["hooks"].as_array().into_iter().flatten() {
                    let kind = hook["type"].as_str().unwrap_or("command");
                    let command = hook["command"].as_str().unwrap_or("").trim().to_string();
                    if kind != "command" {
                        config.problems.push(format!("{event} 里有一个 type 是「{kind}」的钩子：只认 command，跳过"));
                        continue;
                    }
                    if command.is_empty() {
                        config.problems.push(format!("{event} 里有一个钩子没写 command，跳过"));
                        continue;
                    }
                    let timeout = hook["timeout"].as_f64().map(|seconds| (seconds as u64).clamp(1, MAX_TIMEOUT)).unwrap_or(DEFAULT_TIMEOUT);
                    commands.push((command, timeout));
                }
                if !matcher.is_empty() && matcher != "*" && Regex::new(&format!("^(?:{matcher})$")).is_err() {
                    config.problems.push(format!("{event} 的 matcher「{matcher}」不是合法的正则"));
                }
                if !commands.is_empty() {
                    config.groups.push(Group { event: event.clone(), matcher, commands, source: source.to_string(), plugin_root: None });
                }
            }
        }
        config
    }

    /// 按修改时间 + 大小缓存
    /// 文件 → (修改时间, 大小, 解析好的)
    type Cached = (std::time::SystemTime, u64, Config);
    static CACHE: LazyLock<Mutex<HashMap<PathBuf, Cached>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

    fn load(path: &Path, source: &str) -> Config {
        let Some(meta) = std::fs::metadata(path).ok().filter(|meta| meta.is_file()) else {
            CACHE.lock().unwrap().remove(path);
            return Config::default();
        };
        let modified = meta.modified().unwrap_or(std::time::UNIX_EPOCH);
        if let Some((time, size, config)) = CACHE.lock().unwrap().get(path)
            && *time == modified
            && *size == meta.len()
        {
            return config.clone();
        }
        let config = match std::fs::read(path) {
            Ok(data) => parse(&data, source),
            Err(error) => Config { exists: true, problems: vec![format!("读不了 {}：{error}", path.display())], ..Default::default() },
        };
        CACHE.lock().unwrap().insert(path.to_path_buf(), (modified, meta.len(), config.clone()));
        config
    }

    pub fn global_path() -> PathBuf {
        crate::serve_dir().join("hooks.json")
    }

    fn project_path(directory: &str) -> Option<PathBuf> {
        (!directory.is_empty()).then(|| PathBuf::from(directory).join(".friday").join("hooks.json"))
    }

    fn settings_path() -> PathBuf {
        crate::serve_dir().join("settings.json")
    }

    fn trust_key(path: &Path) -> String {
        let text = crate::canonical(path.parent().unwrap_or(path)).join("hooks.json").to_string_lossy().into_owned();
        if cfg!(windows) { text.to_lowercase() } else { text }
    }

    /// 项目钩子：none（没有）/ untrusted / trusted（信任时记下的内容哈希和现在的一样）
    fn project_trust(directory: &str) -> &'static str {
        let Some(path) = project_path(directory) else { return "none" };
        let config = load(&path, "project");
        if !config.exists {
            return "none";
        }
        let trusted = crate::read_json_file(&settings_path())["trustedProjectHooks"][trust_key(&path)].as_str().map(str::to_string);
        if trusted.is_some_and(|hash| !config.hash.is_empty() && hash == config.hash) { "trusted" } else { "untrusted" }
    }

    fn set_trust(directory: &str, trusted: bool) {
        let Some(path) = project_path(directory) else { return };
        let config = load(&path, "project");
        crate::update_serve_settings(|settings| {
            let table = settings.entry("trustedProjectHooks").or_insert_with(|| json!({}));
            if !table.is_object() {
                *table = json!({});
            }
            let table = table.as_object_mut().unwrap();
            if trusted && config.exists && !config.hash.is_empty() {
                table.insert(trust_key(&path), json!(config.hash));
            } else {
                table.remove(&trust_key(&path));
            }
        });
    }

    /// 启用的插件带来的（装的时候确认过，不用再信任）
    fn plugin_groups() -> Vec<Group> {
        crate::plugins::enabled()
            .into_iter()
            .filter_map(|(name, directory)| crate::plugins::hooks_file(&directory).map(|file| (name, directory, file)))
            .flat_map(|(name, directory, file)| {
                load(&file, &format!("plugin:{name}")).groups.into_iter().map(move |mut group| {
                    group.plugin_root = Some(directory.clone());
                    group
                })
            })
            .collect()
    }

    /// 工具名对得上 matcher（正则整段匹配；也认 Claude Code 的工具名：Bash、Write、Edit…）
    fn matches(matcher: &str, tool: &str) -> bool {
        let pattern = matcher.trim();
        if pattern.is_empty() || pattern == "*" {
            return true;
        }
        if Regex::new(&format!("^(?:{pattern})$")).is_ok_and(|expression| expression.is_match(tool)) {
            return true;
        }
        pattern
            .split('|')
            .map(str::trim)
            .filter(|name| !name.is_empty() && name.chars().all(|c| c.is_ascii_alphabetic() || c == '_'))
            .any(|name| friday_core::tool_aliases::canonical_name(name) == tool)
    }

    fn groups_for(event: &str, directory: &str, tool: &str) -> Vec<Group> {
        let mut all = load(&global_path(), "global").groups;
        if project_trust(directory) == "trusted"
            && let Some(path) = project_path(directory)
        {
            all.extend(load(&path, "project").groups);
        }
        all.extend(plugin_groups());
        all.into_iter().filter(|group| group.event == event && (!event.ends_with("ToolUse") || matches(&group.matcher, tool))).collect()
    }

    #[derive(Default, PartialEq, PartialOrd, Clone, Copy)]
    enum Decision {
        #[default]
        None,
        Allow,
        Ask,
        Block,
    }

    #[derive(Default)]
    pub struct Outcome {
        decision: Decision,
        reason: String,
        updated_input: Option<Map<String, Value>>,
        context: String,
    }

    impl Outcome {
        fn decide(&mut self, decision: Decision, reason: &str) {
            if decision < self.decision {
                return;
            }
            if decision > self.decision {
                self.reason.clear();
            }
            self.decision = decision;
            if !reason.is_empty() {
                self.reason = if self.reason.is_empty() { reason.to_string() } else { format!("{}\n{reason}", self.reason) };
            }
        }

        fn add_context(&mut self, text: &str) {
            let text = text.trim();
            if !text.is_empty() {
                self.context = if self.context.is_empty() { text.to_string() } else { format!("{}\n{text}", self.context) };
            }
        }

        /// 交回大脑的（能力协议 §6.7 的回话）
        pub fn reply(&self, event: &str) -> Value {
            let mut reply = json!({});
            match (event, self.decision) {
                ("PreToolUse", Decision::Block) => {
                    reply["permissionDecision"] = json!("deny");
                    reply["permissionDecisionReason"] = json!(self.reason);
                }
                ("PreToolUse", Decision::Ask) => {
                    reply["permissionDecision"] = json!("ask");
                    reply["permissionDecisionReason"] = json!(self.reason);
                }
                ("PreToolUse", Decision::Allow) => reply["permissionDecision"] = json!("allow"),
                (_, Decision::Block) => {
                    reply["decision"] = json!("block");
                    reply["reason"] = json!(self.reason);
                }
                _ => {}
            }
            if let Some(input) = &self.updated_input {
                reply["updatedInput"] = Value::Object(input.clone());
            }
            if !self.context.is_empty() {
                reply["additionalContext"] = json!(self.context);
            }
            reply
        }
    }

    struct Ran {
        started: bool,
        error: String,
        exit_code: i64,
        timed_out: bool,
        stdout: String,
        stderr: String,
    }

    /// 按 Claude Code 的规矩认一个钩子的结果；回运行记录里的「结果」
    fn interpret(event: &str, ran: &Ran, command: &str, outcome: &mut Outcome) -> String {
        let (out, err) = (ran.stdout.trim(), ran.stderr.trim());
        if !ran.started {
            crate::serve_log("hooks", &format!("钩子「{command}」启动不了：{}", ran.error));
            return "出错".into();
        }
        if ran.timed_out {
            return "超时".into();
        }
        if ran.exit_code != 0 && ran.exit_code != 2 {
            return "出错".into();
        }
        if ran.exit_code == 2 {
            let reason = cut(if err.is_empty() { "（钩子没写原因）" } else { err }, MAX_FEEDBACK);
            if event == "PostToolUse" {
                outcome.add_context(&reason);
                return "补了说明".into();
            }
            outcome.decide(Decision::Block, &reason);
            return if event == "Stop" { "接着做".into() } else { "拦下".into() };
        }
        // 退出码 0：JSON 输出按 JSON 认
        if out.starts_with('{')
            && let Ok(Value::Object(json)) = serde_json::from_str::<Value>(out)
        {
            let mut label = "通过".to_string();
            let reason = cut(json.get("reason").and_then(Value::as_str).unwrap_or("").trim(), MAX_FEEDBACK);
            let specific = json.get("hookSpecificOutput").cloned().unwrap_or_default();
            match json.get("decision").and_then(Value::as_str) {
                Some("block") if event == "PostToolUse" => {
                    outcome.add_context(&reason);
                    label = "补了说明".into();
                }
                Some("block") => {
                    outcome.decide(Decision::Block, if reason.is_empty() { "（钩子没写原因）" } else { &reason });
                    label = if event == "Stop" { "接着做".into() } else { "拦下".into() };
                }
                Some("approve") if event == "PreToolUse" => {
                    outcome.decide(Decision::Allow, &reason);
                    label = "免确认".into();
                }
                _ => {}
            }
            if event == "PreToolUse" {
                let why = cut(specific["permissionDecisionReason"].as_str().unwrap_or("").trim(), MAX_FEEDBACK);
                match specific["permissionDecision"].as_str() {
                    Some("deny") => {
                        outcome.decide(Decision::Block, if why.is_empty() { "（钩子没写原因）" } else { &why });
                        label = "拦下".into();
                    }
                    Some("ask") => {
                        outcome.decide(Decision::Ask, &why);
                        label = "要确认".into();
                    }
                    Some("allow") => {
                        outcome.decide(Decision::Allow, &why);
                        label = "免确认".into();
                    }
                    _ => {}
                }
                if let Some(input) = specific["updatedInput"].as_object() {
                    outcome.updated_input = Some(input.clone());
                    label += "、改了参数";
                }
            }
            if let Some(context) = specific["additionalContext"].as_str().map(str::trim).filter(|context| !context.is_empty())
                && (event == "PostToolUse" || event == "UserPromptSubmit")
            {
                outcome.add_context(&cut(context, MAX_FEEDBACK));
                if label == "通过" {
                    label = "补了说明".into();
                }
            }
            return label;
        }
        // 普通文字：用户发消息时补到消息后面，其余只记日志
        if event == "UserPromptSubmit" && !out.is_empty() {
            outcome.add_context(&cut(out, MAX_FEEDBACK));
            return "补了说明".into();
        }
        "通过".into()
    }

    /// 跑一条命令（这台的 shell：Windows PowerShell，其他 sh）
    fn execute(command: &str, directory: &Path, input: &[u8], environment: &[(String, String)], timeout: Duration) -> Ran {
        let mut process = if cfg!(windows) {
            let mut process = Command::new("powershell");
            process.args(["-NoProfile", "-NonInteractive", "-Command", &format!("try{{[Console]::InputEncoding=[Text.UTF8Encoding]::new($false)}}catch{{}};[Console]::OutputEncoding=[Text.UTF8Encoding]::new($false);{command}")]);
            process
        } else {
            let mut process = Command::new("/bin/sh");
            process.args(["-c", command]);
            process
        };
        process
            .current_dir(if directory.is_dir() { directory.to_path_buf() } else { crate::user_home() })
            .envs(environment.iter().cloned());
        process.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
        #[cfg(unix)]
        unsafe {
            use std::os::unix::process::CommandExt;
            process.pre_exec(|| {
                libc::setsid();
                Ok(())
            });
        }
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            process.creation_flags(0x0800_0000);
        }
        let mut child = match process.spawn() {
            Ok(child) => child,
            Err(error) => {
                return Ran {
                    started: false,
                    error: error.to_string(),
                    exit_code: -1,
                    timed_out: false,
                    stdout: String::new(),
                    stderr: String::new(),
                };
            }
        };
        if let Some(mut stdin) = child.stdin.take() {
            let input = input.to_vec();
            std::thread::spawn(move || {
                let _ = stdin.write_all(&input);
            });
        }
        let read = |pipe: Option<Box<dyn Read + Send>>| {
            std::thread::spawn(move || {
                let mut data = Vec::new();
                if let Some(pipe) = pipe {
                    let _ = pipe.take(1 << 20).read_to_end(&mut data);
                }
                String::from_utf8_lossy(&data).into_owned()
            })
        };
        let stdout = read(child.stdout.take().map(|pipe| Box::new(pipe) as Box<dyn Read + Send>));
        let stderr = read(child.stderr.take().map(|pipe| Box::new(pipe) as Box<dyn Read + Send>));
        let started = Instant::now();
        let (exit_code, timed_out) = loop {
            match child.try_wait() {
                Ok(Some(status)) => break (status.code().map(i64::from).unwrap_or(-1), false),
                Ok(None) if started.elapsed() >= timeout => {
                    let _ = child.kill();
                    let _ = child.wait();
                    break (-1, true);
                }
                Ok(None) => std::thread::sleep(Duration::from_millis(20)),
                Err(_) => break (-1, false),
            }
        };
        Ran {
            started: true,
            error: String::new(),
            exit_code,
            timed_out,
            stdout: stdout.join().unwrap_or_default(),
            stderr: stderr.join().unwrap_or_default(),
        }
    }

    static RECORDS: LazyLock<Mutex<Vec<Value>>> = LazyLock::new(|| Mutex::new(Vec::new()));

    /// 跑一个时机的钩子（大脑问的：params 是 Claude Code 交给钩子命令的标准输入）
    pub fn run(params: &Value) -> Outcome {
        let mut outcome = Outcome::default();
        let event = params["hook_event_name"].as_str().unwrap_or("");
        let directory = params["cwd"].as_str().unwrap_or("").to_string();
        // 常驻那份自己的动作（friday__browser_navigate）按动作名比
        let raw_tool = params["tool_name"].as_str().unwrap_or("");
        let tool =
            raw_tool.split_once("__").filter(|(owner, _)| owner.starts_with("friday")).map_or(raw_tool, |(_, action)| action).to_string();
        let groups = groups_for(event, &directory, &tool);
        if groups.is_empty() {
            return outcome;
        }
        let mut input = params.clone();
        input["tool_name"] = json!(tool);
        let stdin = serde_json::to_vec(&input).unwrap_or_default();
        let mut environment = vec![
            ("FRIDAY_HOOK_EVENT".to_string(), event.to_string()),
            ("FRIDAY_CWD".to_string(), directory.clone()),
            ("CLAUDE_PROJECT_DIR".to_string(), directory.clone()),
        ];
        if !tool.is_empty() {
            environment.push(("FRIDAY_TOOL".into(), tool.clone()));
        }
        // 写文件的工具：FRIDAY_FILE 是完整路径
        if let Some(file) =
            ["path", "file_path", "file"].iter().find_map(|key| params["tool_input"][key].as_str().filter(|value| !value.is_empty()))
        {
            let path = PathBuf::from(file);
            let full = if path.is_absolute() { path } else { PathBuf::from(&directory).join(path) };
            environment.push(("FRIDAY_FILE".into(), full.to_string_lossy().into_owned()));
        }
        for group in groups {
            for (command, timeout) in &group.commands {
                let mut environment = environment.clone();
                let command_text = match &group.plugin_root {
                    Some(root) => {
                        let root = root.to_string_lossy().into_owned();
                        environment.push(("CLAUDE_PLUGIN_ROOT".into(), root.clone()));
                        environment.push(("FRIDAY_PLUGIN_ROOT".into(), root.clone()));
                        command.replace("${CLAUDE_PLUGIN_ROOT}", &root).replace("${FRIDAY_PLUGIN_ROOT}", &root)
                    }
                    None => command.clone(),
                };
                let started = Instant::now();
                let ran = execute(&command_text, Path::new(&directory), &stdin, &environment, Duration::from_secs(*timeout));
                let result = interpret(event, &ran, command, &mut outcome);
                let output =
                    [ran.stdout.trim(), ran.stderr.trim()].into_iter().filter(|part| !part.is_empty()).collect::<Vec<_>>().join("\n");
                let record = json!({
                    "at": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true), "event": event, "eventTitle": event_title(event),
                    "command": command, "source": group.source, "toolName": tool, "exitCode": ran.exit_code, "timedOut": ran.timed_out,
                    "milliseconds": started.elapsed().as_millis() as u64, "result": result, "output": cut(&output, MAX_OUTPUT),
                });
                {
                    let mut records = RECORDS.lock().unwrap();
                    records.insert(0, record);
                    records.truncate(MAX_RECORDS);
                }
                // 拦下了：同一时机后面的不跑
                if outcome.decision == Decision::Block {
                    return outcome;
                }
            }
        }
        outcome
    }

    /// 设置 › 钩子
    pub fn state(directory: &str) -> Value {
        let read_text = |path: &Path| std::fs::read_to_string(path).unwrap_or_default();
        let global = load(&global_path(), "global");
        let project_file = project_path(directory);
        let project = project_file.as_ref().map(|path| load(path, "project")).unwrap_or_default();
        let trust = project_trust(directory);
        let mut hooks = Vec::new();
        let mut add = |config: &Config, active: bool| {
            for group in &config.groups {
                for (command, timeout) in &group.commands {
                    hooks.push(
                        json!({ "event": group.event, "eventTitle": event_title(&group.event), "matcher": group.matcher, "command": command,
                        "timeout": timeout, "source": group.source, "active": active }),
                    );
                }
            }
        };
        add(&global, true);
        add(&project, trust == "trusted");
        add(&Config { groups: plugin_groups(), ..Default::default() }, true);
        json!({
            "globalPath": global_path().to_string_lossy(),
            "globalExists": global_path().is_file(),
            "globalText": read_text(&global_path()),
            "globalProblems": global.problems,
            "projectDirectory": directory,
            "projectPath": project_file.as_ref().map(|path| path.to_string_lossy().into_owned()).unwrap_or_default(),
            "projectExists": trust != "none",
            "projectText": if trust != "none" { project_file.as_ref().map(|path| read_text(path)).unwrap_or_default() } else { String::new() },
            "projectTrust": trust,
            "projectProblems": project.problems,
            "hooks": hooks,
            "recent": RECORDS.lock().unwrap().clone(),
            "example": example(),
        })
    }

    fn example() -> String {
        let (guard, test) = if cfg!(windows) {
            (
                "$i = [Console]::In.ReadToEnd() | ConvertFrom-Json; if ($i.tool_input.command -match 'rm -rf|Remove-Item -Recurse') { [Console]::Error.WriteLine('不许递归删除'); exit 2 }",
                "npm test --silent; if ($LASTEXITCODE -ne 0) { [Console]::Error.WriteLine('测试没过，修好再交'); exit 2 }",
            )
        } else {
            (
                "grep -q 'rm -rf' && { echo '不许 rm -rf' >&2; exit 2; } || exit 0",
                "npm test --silent || { echo 测试没过，修好再交 >&2; exit 2; }",
            )
        };
        serde_json::to_string_pretty(&json!({ "hooks": {
            "PreToolUse": [{ "matcher": "run_shell", "hooks": [{ "type": "command", "command": guard }] }],
            "Stop": [{ "hooks": [{ "type": "command", "command": test, "timeout": 300 }] }],
        } }))
        .unwrap_or_default()
    }

    fn save_global(text: &str) -> String {
        if !text.trim().is_empty() {
            match serde_json::from_str::<Value>(text) {
                Ok(Value::Object(_)) => {}
                Ok(_) => return "最外层要是 {…}".into(),
                Err(error) => return format!("不是合法的 JSON：{error}"),
            }
        }
        let path = global_path();
        if let Some(dir) = path.parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        match std::fs::write(&path, text) {
            Ok(()) => {
                CACHE.lock().unwrap().remove(&path);
                String::new()
            }
            Err(error) => format!("写不了 {}：{error}", path.display()),
        }
    }

    /// hooks.*（设置 › 钩子看着这台时；项目文件夹不给用默认的工作目录）
    pub fn request(method: &str, params: &Value) -> crate::Reply {
        let directory = params["directory"]
            .as_str()
            .filter(|directory| !directory.is_empty())
            .map(str::to_string)
            .unwrap_or_else(crate::default_directory);
        match method {
            "hooks.state" => Ok(state(&directory)),
            "hooks.saveGlobal" => {
                let error = save_global(params["text"].as_str().unwrap_or(""));
                Ok(json!({ "error": error, "state": state(&directory) }))
            }
            "hooks.trust" => {
                set_trust(&directory, params["trusted"].as_bool().unwrap_or(false));
                Ok(state(&directory))
            }
            "hooks.example" => Ok(json!({ "text": example() })),
            other => Err((-32601, format!("没有这个方法：{other}"))),
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn parses_and_interprets_like_claude_code() {
            let config = parse(br#"{"hooks":{"PreToolUse":[{"matcher":"Bash|write_file","hooks":[{"type":"command","command":"x","timeout":9999}]}],"Nope":[],"Stop":[{"hooks":[{"type":"prompt"}]}]}}"#, "global");
            assert_eq!(config.groups.len(), 1);
            assert_eq!(config.groups[0].commands[0].1, MAX_TIMEOUT);
            assert!(
                config.problems.iter().any(|problem| problem.contains("Nope"))
                    && config.problems.iter().any(|problem| problem.contains("prompt"))
            );
            assert!(
                matches("Bash|write_file", "run_shell")
                    && matches("write_file", "write_file")
                    && !matches("edit_file", "write_file")
                    && matches("", "x")
            );
            let ran = |code: i64, stdout: &str, stderr: &str| Ran {
                started: true,
                error: String::new(),
                exit_code: code,
                timed_out: false,
                stdout: stdout.into(),
                stderr: stderr.into(),
            };
            let mut outcome = Outcome::default();
            assert_eq!(interpret("PreToolUse", &ran(2, "", "不许"), "c", &mut outcome), "拦下");
            assert_eq!(outcome.reply("PreToolUse")["permissionDecision"], "deny");
            let mut outcome = Outcome::default();
            interpret(
                "PreToolUse",
                &ran(
                    0,
                    r#"{"hookSpecificOutput":{"permissionDecision":"ask","permissionDecisionReason":"看看","updatedInput":{"a":1}}}"#,
                    "",
                ),
                "c",
                &mut outcome,
            );
            let reply = outcome.reply("PreToolUse");
            assert_eq!((reply["permissionDecision"].as_str(), reply["updatedInput"]["a"].as_i64()), (Some("ask"), Some(1)));
            let mut outcome = Outcome::default();
            interpret("UserPromptSubmit", &ran(0, "分支 main", ""), "c", &mut outcome);
            assert_eq!(outcome.reply("UserPromptSubmit")["additionalContext"], "分支 main");
            let mut outcome = Outcome::default();
            interpret("Stop", &ran(2, "", "测试没过"), "c", &mut outcome);
            assert_eq!(outcome.reply("Stop")["decision"], "block");
            let mut outcome = Outcome::default();
            interpret("PostToolUse", &ran(0, "格式化了 a.py", ""), "c", &mut outcome);
            assert_eq!(outcome.reply("PostToolUse"), json!({}));
        }
    }
}

// MARK: - 插件

mod plugins {
    //! 插件（照 Friday 的 Plugins，Friday 仓库的 qt/docs/PLUGINS.md）：技能、脚本工具、MCP 服务、钩子、命令打成的包，Friday 的写法
    //! （plugin.json）和 Claude Code 的（.claude-plugin/plugin.json、hooks/hooks.json、.mcp.json）都认。装到 `~/.edith/friday/plugins/<名字>/`，
    //! 先「看一眼」列清单、用户点了才装。启用的：钩子、脚本工具这里直接用；技能链进 EDITH 的 `~/.edith/skills/`（`<插件>-<技能>`），
    //! MCP 服务并进 `~/.edith/mcp.json`（名字「插件名-服务名」，带 fridayPlugin 标记）——停用、卸载时撤掉。

    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::{LazyLock, Mutex};

    use base64::Engine;
    use regex::Regex;
    use serde_json::{Map, Value, json};
    use sha2::Digest;

    pub fn root() -> PathBuf {
        crate::serve_dir().join("plugins")
    }

    fn registry_path() -> PathBuf {
        root().join("plugins.json")
    }

    fn staging_root() -> PathBuf {
        root().join(".staging")
    }

    fn registry() -> Vec<Value> {
        let mut list: Vec<Value> = crate::read_json_file(&registry_path())["plugins"].as_array().cloned().unwrap_or_default();
        list.retain(|plugin| plugin["name"].as_str().is_some_and(|name| !name.is_empty()));
        list.sort_by_key(|plugin| plugin["name"].as_str().unwrap_or("").to_lowercase());
        list
    }

    fn write_registry(list: &[Value]) -> Result<(), String> {
        let _ = std::fs::create_dir_all(root());
        crate::write_json_file(&registry_path(), &json!({ "plugins": list })).map_err(|(_, message)| message)
    }

    fn directory_of(name: &str) -> PathBuf {
        root().join(name)
    }

    /// 启用的插件：(名字, 文件夹)
    pub fn enabled() -> Vec<(String, PathBuf)> {
        registry()
            .into_iter()
            .filter(|plugin| plugin["enabled"].as_bool().unwrap_or(true))
            .map(|plugin| plugin["name"].as_str().unwrap_or("").to_string())
            .map(|name| (name.clone(), directory_of(&name)))
            .filter(|(_, directory)| directory.is_dir())
            .collect()
    }

    fn first_existing(directory: &Path, candidates: &[&str]) -> Option<PathBuf> {
        candidates.iter().map(|candidate| directory.join(candidate)).find(|path| path.exists())
    }

    pub fn hooks_file(directory: &Path) -> Option<PathBuf> {
        first_existing(directory, &["hooks.json", "hooks/hooks.json"])
    }

    fn mcp_file(directory: &Path) -> Option<PathBuf> {
        first_existing(directory, &["mcp.json", ".mcp.json"])
    }

    fn sub(directory: &Path, name: &str) -> Option<PathBuf> {
        Some(directory.join(name)).filter(|path| path.is_dir())
    }

    /// 启用的插件的脚本工具文件夹：(插件名, tools/)
    pub fn tools_dirs() -> Vec<(String, PathBuf)> {
        enabled().into_iter().filter_map(|(name, directory)| sub(&directory, "tools").map(|tools| (name, tools))).collect()
    }

    /// plugin.json：Friday 的放在根上，Claude Code 的在 .claude-plugin/ 里
    fn manifest_file(directory: &Path) -> Option<(Value, &'static str)> {
        for (relative, format) in [("plugin.json", "friday"), (".claude-plugin/plugin.json", "claude")] {
            if let Ok(data) = std::fs::read(directory.join(relative)) {
                return Some((serde_json::from_slice(&data).unwrap_or(json!({})), format));
            }
        }
        None
    }

    fn looks_like_plugin(path: &Path) -> bool {
        manifest_file(path).is_some()
            || ["skills", "tools", "commands", "agents"].iter().any(|name| path.join(name).is_dir())
            || first_existing(path, &["mcp.json", ".mcp.json", "hooks.json", "hooks/hooks.json"]).is_some()
    }

    /// 插件的根：这里，或者只有一个子文件夹时它
    fn find_root(directory: &Path) -> Option<PathBuf> {
        if looks_like_plugin(directory) {
            return Some(directory.to_path_buf());
        }
        let children: Vec<PathBuf> = std::fs::read_dir(directory)
            .into_iter()
            .flatten()
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| path.is_dir() && path.file_name().is_some_and(|name| name != "__MACOSX"))
            .collect();
        (children.len() == 1 && looks_like_plugin(&children[0])).then(|| children[0].clone())
    }

    fn files_under(directory: &Path) -> Vec<String> {
        let mut result = Vec::new();
        let mut stack = vec![directory.to_path_buf()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
                let path = entry.path();
                let relative =
                    path.strip_prefix(directory).map(|relative| relative.to_string_lossy().replace('\\', "/")).unwrap_or_default();
                if relative.starts_with(".git") || relative.contains("/.git/") || relative.starts_with("__MACOSX") {
                    continue;
                }
                if path.is_dir() {
                    stack.push(path);
                } else {
                    result.push(relative);
                }
            }
        }
        result.sort();
        result
    }

    fn hash_directory(directory: &Path) -> String {
        let mut hasher = sha2::Sha256::new();
        for relative in files_under(directory) {
            hasher.update(relative.as_bytes());
            hasher.update([0u8]);
            if let Ok(data) = std::fs::read(directory.join(&relative)) {
                hasher.update(sha2::Sha256::digest(&data));
            }
        }
        hasher.finalize().iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn name_problem(name: &str) -> Option<String> {
        let name = name.trim();
        if name.is_empty() {
            return Some("没有名字（plugin.json 里写 name）".into());
        }
        if name.contains(' ') {
            return Some("插件名不能有空格（MCP 服务、技能名里要用它）".into());
        }
        if name.starts_with('.') || name.chars().any(|c| "/\\:*?\"<>|".contains(c)) || name.len() > 64 {
            return Some("插件名只能用字母、数字、-、_ 这类能当文件夹名的字".into());
        }
        None
    }

    fn markdown_names(directory: Option<PathBuf>) -> Vec<String> {
        let Some(directory) = directory else { return Vec::new() };
        files_under(&directory).into_iter().filter_map(|relative| relative.strip_suffix(".md").map(|name| name.replace('/', ":"))).collect()
    }

    /// 包里有什么（清单）
    fn read(directory: &Path, hash: bool) -> Value {
        let mut problems: Vec<String> = Vec::new();
        let (manifest, format) = manifest_file(directory).unwrap_or_else(|| {
            problems.push("没有 plugin.json：名字用文件夹名".into());
            (json!({}), "friday")
        });
        let text = |key: &str| manifest[key].as_str().unwrap_or("").trim().to_string();
        let name = Some(text("name"))
            .filter(|name| !name.is_empty())
            .unwrap_or_else(|| directory.file_name().map(|name| name.to_string_lossy().into_owned()).unwrap_or_default());
        let author = manifest["author"]["name"].as_str().map(str::to_string).unwrap_or_else(|| text("author"));
        let skills: Vec<String> = sub(directory, "skills")
            .map(|skills| {
                crate::sorted_names(&skills)
                    .into_iter()
                    .filter(|skill| !skill.starts_with('.') && skills.join(skill).join("SKILL.md").is_file())
                    .collect()
            })
            .unwrap_or_default();
        let mut tools = Vec::new();
        if let Some(dir) = sub(directory, "tools") {
            for folder in crate::sorted_names(&dir).into_iter().filter(|folder| !folder.starts_with('.')) {
                match friday_core::script_tools::load(&dir.join(&folder)) {
                    Some(tool) => tools.push(json!({ "name": tool["manifest"]["name"], "permission": tool["manifest"]["permission"] })),
                    None => problems.push(format!("tools/{folder} 读不出来（要有 tool.json 和入口脚本）")),
                }
            }
        }
        let mut mcp = Vec::new();
        if let Some(file) = mcp_file(directory) {
            match std::fs::read(&file).ok().and_then(|data| serde_json::from_slice::<Value>(&data).ok()) {
                Some(config) => {
                    for (server, value) in config["mcpServers"].as_object().into_iter().flatten() {
                        let summary = value["url"].as_str().map(str::to_string).unwrap_or_else(|| {
                            let args: Vec<&str> = value["args"].as_array().into_iter().flatten().filter_map(Value::as_str).collect();
                            format!("{} {}", value["command"].as_str().unwrap_or(""), args.join(" ")).trim().to_string()
                        });
                        mcp.push(json!({ "name": format!("{name}-{server}"), "summary": summary.replace("${CLAUDE_PLUGIN_ROOT}", "〈插件文件夹〉").replace("${FRIDAY_PLUGIN_ROOT}", "〈插件文件夹〉") }));
                    }
                }
                None => problems.push("MCP 配置读不懂".into()),
            }
        }
        let mut hooks = Vec::new();
        if let Some(file) = hooks_file(directory)
            && let Ok(Value::Object(config)) =
                std::fs::read(&file).map(|data| serde_json::from_slice::<Value>(&data).unwrap_or(json!(null)))
        {
            let events = config.get("hooks").cloned().unwrap_or(Value::Object(config.clone()));
            for (event, groups) in events.as_object().into_iter().flatten() {
                for group in groups.as_array().into_iter().flatten() {
                    for hook in group["hooks"].as_array().into_iter().flatten() {
                        if let Some(command) = hook["command"].as_str() {
                            hooks.push(json!({ "event": event, "command": command.replace("${CLAUDE_PLUGIN_ROOT}", "〈插件文件夹〉").replace("${FRIDAY_PLUGIN_ROOT}", "〈插件文件夹〉") }));
                        }
                    }
                }
            }
        }
        let skipped: Vec<String> =
            ["output-styles", "providers"].iter().filter(|name| directory.join(name).is_dir()).map(|name| format!("{name}/")).collect();
        json!({
            "name": name, "title": Some(text("title")).filter(|title| !title.is_empty()).unwrap_or_else(|| name.clone()), "version": text("version"),
            "description": text("description"), "author": author, "format": format, "skills": skills, "tools": tools, "mcp": mcp, "hooks": hooks,
            "commands": markdown_names(sub(directory, "commands")), "agents": markdown_names(sub(directory, "agents")), "skipped": skipped,
            "problems": problems, "contentHash": if hash { hash_directory(directory) } else { String::new() },
        })
    }

    struct Staged {
        directory: PathBuf,
        contents: Value,
        source: Value,
    }

    static STAGED: LazyLock<Mutex<HashMap<String, Staged>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

    fn source_label(source: &Value) -> String {
        if source["type"] == "github" {
            source["url"].as_str().unwrap_or("").to_string()
        } else {
            source["path"].as_str().unwrap_or("").to_string()
        }
    }

    fn staged_json(token: &str, staged: &Staged) -> Value {
        let name = staged.contents["name"].as_str().unwrap_or("");
        let existing = registry().into_iter().find(|plugin| plugin["name"].as_str().is_some_and(|other| other.eq_ignore_ascii_case(name)));
        let mut contents = staged.contents.clone();
        if let Some(object) = contents.as_object_mut() {
            object.remove("contentHash");
        }
        json!({
            "token": token, "contents": contents, "replaces": existing.is_some(),
            "unchanged": existing.is_some_and(|existing| existing["contentHash"] == staged.contents["contentHash"]),
            "source": staged.source, "sourceLabel": source_label(&staged.source),
        })
    }

    /// 解开 zip（macOS、Windows 的 tar 认 zip；Linux 用 unzip，没有就用 python 的 zipfile）
    fn unzip(archive: &Path, into: &Path) -> Result<(), String> {
        std::fs::create_dir_all(into).map_err(|error| error.to_string())?;
        let tar = if cfg!(windows) {
            PathBuf::from(std::env::var("SystemRoot").unwrap_or_else(|_| r"C:\Windows".into())).join("System32").join("tar.exe")
        } else {
            PathBuf::from("tar")
        };
        let attempts: Vec<Command> = if cfg!(target_os = "linux") {
            let mut unzip = Command::new("unzip");
            unzip.arg("-q").arg("-o").arg(archive).arg("-d").arg(into);
            let mut python = Command::new("python3");
            python.arg("-m").arg("zipfile").arg("-e").arg(archive).arg(into);
            vec![unzip, python]
        } else {
            let mut command = Command::new(tar);
            command.arg("-xf").arg(archive).arg("-C").arg(into);
            vec![command]
        };
        let mut last = String::new();
        for mut command in attempts {
            match command.output() {
                Ok(output) if output.status.success() => return Ok(()),
                Ok(output) => last = String::from_utf8_lossy(&output.stderr).trim().to_string(),
                Err(error) => last = error.to_string(),
            }
        }
        Err(format!("解不开 zip：{last}"))
    }

    fn copy_tree(source: &Path, destination: &Path) -> Result<(), String> {
        std::fs::create_dir_all(destination).map_err(|error| error.to_string())?;
        for entry in std::fs::read_dir(source).map_err(|error| error.to_string())?.flatten() {
            let name = entry.file_name();
            if name == ".git" || name == "__MACOSX" || entry.file_type().is_ok_and(|kind| kind.is_symlink()) {
                continue;
            }
            let target = destination.join(&name);
            if entry.path().is_dir() {
                copy_tree(&entry.path(), &target)?;
            } else {
                std::fs::copy(entry.path(), &target).map_err(|_| format!("拷不了 {}", entry.path().display()))?;
            }
        }
        Ok(())
    }

    fn stage(staging: &Path, base: &Path, token: &str, source: Value) -> Result<Value, String> {
        let Some(plugin_root) = find_root(base) else {
            let _ = std::fs::remove_dir_all(staging);
            return Err("这不是插件：没找到 plugin.json（Friday 的放在最外层，Claude Code 的在 .claude-plugin/ 里），也没有 skills/、tools/、mcp.json、hooks.json".into());
        };
        let contents = read(&plugin_root, true);
        if let Some(problem) = name_problem(contents["name"].as_str().unwrap_or("")) {
            let _ = std::fs::remove_dir_all(staging);
            return Err(format!("插件名不能用：{problem}"));
        }
        let staged = Staged { directory: plugin_root, contents, source };
        let value = staged_json(token, &staged);
        STAGED.lock().unwrap().insert(token.to_string(), staged);
        Ok(value)
    }

    fn new_staging() -> (String, PathBuf) {
        let token = uuid::Uuid::new_v4().to_string();
        let staging = staging_root().join(&token);
        let _ = std::fs::create_dir_all(&staging);
        (token, staging)
    }

    /// 看一眼 GitHub 上的（https://github.com/主人/仓库[/tree/分支[/子文件夹]]，下 codeload 的 zip，不用装 git）
    fn inspect_github(url: &str) -> Result<Value, String> {
        static GITHUB: LazyLock<Regex> = LazyLock::new(|| {
            Regex::new(r"^(?:https?://)?(?:www\.)?github\.com/([^/\s]+)/([^/\s#?]+?)(?:\.git)?(?:/(?:tree|blob)/([^/\s]+)(?:/(.*?))?)?/?(?:[#?].*)?$").unwrap()
        });
        let captures = GITHUB
            .captures(url.trim())
            .ok_or_else(|| format!("认不出这个 GitHub 地址：{url}（要像 https://github.com/主人/仓库，或者 …/tree/分支/子文件夹）"))?;
        let (owner, repo) = (&captures[1], &captures[2]);
        let reference = captures.get(3).map_or("HEAD", |reference| reference.as_str());
        let subdirectory = captures.get(4).map(|sub| sub.as_str().to_string()).unwrap_or_default();
        let (token, staging) = new_staging();
        let archive = staging.with_extension("zip");
        let fetched = crate::powers::fetch(
            &format!("https://codeload.github.com/{owner}/{repo}/zip/{reference}"),
            &archive,
            &std::sync::atomic::AtomicBool::new(false),
            &mut |_| {},
        )
        .and_then(|_| unzip(&archive, &staging));
        let _ = std::fs::remove_file(&archive);
        if let Err(error) = fetched {
            let _ = std::fs::remove_dir_all(&staging);
            return Err(error);
        }
        // zip 顶层是「仓库-分支/」；指了子文件夹的取它
        let top: Vec<PathBuf> =
            std::fs::read_dir(&staging).into_iter().flatten().flatten().map(|entry| entry.path()).filter(|path| path.is_dir()).collect();
        let mut base = if top.len() == 1 { top[0].clone() } else { staging.clone() };
        if !subdirectory.is_empty() {
            let wanted = base.join(&subdirectory);
            if subdirectory.split('/').any(|part| part == "..") || !wanted.is_dir() {
                let _ = std::fs::remove_dir_all(&staging);
                return Err(format!("仓库里没有子文件夹 {subdirectory}"));
            }
            base = wanted;
        }
        stage(&staging, &base, &token, json!({ "type": "github", "url": url.trim() }))
    }

    /// 看一眼客户端传来的 zip（.fridayplugin / .zip，文件夹由客户端打成 zip）
    fn inspect_data(name: &str, data: &str) -> Result<Value, String> {
        let bytes = base64::engine::general_purpose::STANDARD.decode(data.trim()).map_err(|_| "data 不是 base64".to_string())?;
        let (token, staging) = new_staging();
        let archive = staging.with_extension("zip");
        std::fs::write(&archive, bytes).map_err(|error| error.to_string())?;
        let unpacked = unzip(&archive, &staging);
        let _ = std::fs::remove_file(&archive);
        if let Err(error) = unpacked {
            let _ = std::fs::remove_dir_all(&staging);
            return Err(error);
        }
        stage(&staging, &staging.clone(), &token, json!({ "type": "file", "path": name }))
    }

    fn trash(path: &Path) -> Result<(), String> {
        let bin = root().join(".trash");
        std::fs::create_dir_all(&bin).map_err(|error| error.to_string())?;
        let name = path.file_name().map(|name| name.to_string_lossy().into_owned()).unwrap_or_default();
        std::fs::rename(path, bin.join(format!("{name}-{}", chrono::Local::now().format("%Y%m%d-%H%M%S"))))
            .map_err(|error| format!("{} 移不走：{error}", path.display()))
    }

    fn install(token: &str) -> Result<(), String> {
        let staged = STAGED.lock().unwrap().remove(token).ok_or("这次的安装已经过期了，重新选一次")?;
        let name = staged.contents["name"].as_str().unwrap_or("").to_string();
        let target = directory_of(&name);
        if target.exists() {
            trash(&target)?;
        }
        copy_tree(&staged.directory, &target)?;
        let _ = std::fs::remove_dir_all(staging_root().join(token));
        let mut list = registry();
        list.retain(|plugin| !plugin["name"].as_str().is_some_and(|other| other.eq_ignore_ascii_case(&name)));
        list.push(json!({
            "name": name, "version": staged.contents["version"], "enabled": true, "source": staged.source,
            "installedAt": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true), "contentHash": staged.contents["contentHash"],
        }));
        write_registry(&list)?;
        changed();
        Ok(())
    }

    fn find(name: &str) -> Option<Value> {
        registry().into_iter().find(|plugin| plugin["name"].as_str().is_some_and(|other| other.eq_ignore_ascii_case(name)))
    }

    /// 设置 › 插件的列表
    fn list() -> Value {
        let plugins: Vec<Value> = registry()
            .into_iter()
            .map(|mut plugin| {
                let name = plugin["name"].as_str().unwrap_or("").to_string();
                let directory = directory_of(&name);
                if let Some(object) = plugin.as_object_mut() {
                    object.remove("contentHash");
                    object.remove("projected");
                }
                plugin["sourceLabel"] = json!(source_label(&plugin["source"]));
                plugin["directory"] = json!(directory.to_string_lossy());
                plugin["missing"] = json!(!directory.is_dir());
                let mut contents = read(&directory, false);
                if let Some(object) = contents.as_object_mut() {
                    object.remove("contentHash");
                }
                plugin["contents"] = contents;
                plugin
            })
            .collect();
        json!({ "plugins": plugins })
    }

    // MARK: 投到 EDITH 那边（技能、MCP）

    fn skills_dir() -> PathBuf {
        crate::edith_home().join("skills")
    }

    fn mcp_path() -> PathBuf {
        crate::edith_home().join("mcp.json")
    }

    /// 启用的插件的技能、MCP 服务放到 EDITH 读的地方；停用、卸载了的撤掉
    fn project() {
        let wanted = enabled();
        // 技能：~/.edith/skills/<插件>-<技能>（Unix 是符号链接，Windows 拷一份），记在 .friday-plugins.json 里好撤
        let record_path = skills_dir().join(".friday-plugins.json");
        let previous: Vec<String> = crate::read_json_file(&record_path)["links"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect();
        let mut links = Vec::new();
        for (name, directory) in &wanted {
            let Some(skills) = sub(directory, "skills") else { continue };
            for skill in crate::sorted_names(&skills).into_iter().filter(|skill| skills.join(skill).join("SKILL.md").is_file()) {
                let link = format!("{name}-{skill}");
                let target = skills_dir().join(&link);
                let _ = std::fs::create_dir_all(skills_dir());
                if std::fs::symlink_metadata(&target).is_err() {
                    #[cfg(unix)]
                    let _ = std::os::unix::fs::symlink(skills.join(&skill), &target);
                    #[cfg(windows)]
                    let _ = copy_tree(&skills.join(&skill), &target);
                }
                links.push(link);
            }
        }
        for stale in previous.iter().filter(|link| !links.contains(link)) {
            let path = skills_dir().join(stale);
            if std::fs::symlink_metadata(&path).is_ok_and(|meta| meta.file_type().is_symlink()) {
                let _ = std::fs::remove_file(&path);
            } else {
                let _ = std::fs::remove_dir_all(&path);
            }
        }
        if !links.is_empty() || !previous.is_empty() {
            let _ = crate::write_json_file(&record_path, &json!({ "links": links }));
        }
        // MCP：并进 ~/.edith/mcp.json，带 fridayPlugin 标记；${CLAUDE_PLUGIN_ROOT} 换成插件文件夹
        let path = mcp_path();
        let mut config = crate::read_json_file(&path);
        if !config.is_object() {
            config = json!({});
        }
        let mut servers: Map<String, Value> = config["mcpServers"].as_object().cloned().unwrap_or_default();
        let before = servers.clone();
        servers.retain(|_, server| server.get("fridayPlugin").is_none());
        for (name, directory) in &wanted {
            let Some(file) = mcp_file(directory) else { continue };
            let Some(plugin_config) = std::fs::read(&file).ok().and_then(|data| serde_json::from_slice::<Value>(&data).ok()) else {
                continue;
            };
            let root = directory.to_string_lossy().into_owned();
            for (server, value) in plugin_config["mcpServers"].as_object().into_iter().flatten() {
                let text = value
                    .to_string()
                    .replace("${CLAUDE_PLUGIN_ROOT}", &root.replace('\\', "\\\\"))
                    .replace("${FRIDAY_PLUGIN_ROOT}", &root.replace('\\', "\\\\"));
                let mut server_config: Value = serde_json::from_str(&text).unwrap_or_else(|_| value.clone());
                server_config["fridayPlugin"] = json!(name);
                servers.insert(format!("{name}-{server}"), server_config);
            }
        }
        if servers != before {
            config["mcpServers"] = Value::Object(servers);
            let _ = crate::write_json_file(&path, &config);
        }
    }

    /// 插件变了：投一遍、脚本工具和钩子跟着（动作换一份），网关重列设置页
    fn changed() {
        project();
        crate::serve_update_actions();
        crate::notify_all_clients("scripts", json!({}));
    }

    /// 起来时投一遍（插件是在别处装的、上次没投完的）
    pub fn start() {
        std::thread::spawn(project);
    }

    /// plugins.*（设置 › 插件看着这台时；下载、解压在工作线程里）
    pub fn request(method: &str, params: &Value) -> crate::Reply {
        let failed = |message: String| (crate::NOT_ALLOWED, message);
        let name =
            || params["name"].as_str().filter(|name| !name.is_empty()).map(str::to_string).ok_or((-32602, "缺少参数 name".to_string()));
        match method {
            "plugins.list" => Ok(list()),
            "plugins.inspectGithub" => inspect_github(params["url"].as_str().unwrap_or("")).map_err(failed),
            "plugins.inspectData" => {
                inspect_data(params["name"].as_str().unwrap_or("plugin.zip"), params["data"].as_str().unwrap_or("")).map_err(failed)
            }
            "plugins.update" => {
                let name = name()?;
                let plugin = find(&name).ok_or_else(|| failed(format!("没有装插件「{name}」")))?;
                match plugin["source"]["type"].as_str() {
                    Some("github") => inspect_github(plugin["source"]["url"].as_str().unwrap_or("")).map_err(failed),
                    _ => Err(failed("从文件装的插件：重新从文件装一次".into())),
                }
            }
            "plugins.install" => {
                install(params["token"].as_str().unwrap_or("")).map_err(failed)?;
                Ok(list())
            }
            "plugins.discard" => {
                let token = params["token"].as_str().unwrap_or("");
                STAGED.lock().unwrap().remove(token);
                let _ = std::fs::remove_dir_all(staging_root().join(token));
                Ok(json!({}))
            }
            "plugins.setEnabled" => {
                let name = name()?;
                let mut list_all = registry();
                let plugin = list_all
                    .iter_mut()
                    .find(|plugin| plugin["name"].as_str().is_some_and(|other| other.eq_ignore_ascii_case(&name)))
                    .ok_or_else(|| failed(format!("没有装插件「{name}」")))?;
                plugin["enabled"] = json!(params["enabled"].as_bool().unwrap_or(true));
                write_registry(&list_all).map_err(failed)?;
                changed();
                Ok(list())
            }
            "plugins.uninstall" => {
                let name = name()?;
                find(&name).ok_or_else(|| failed(format!("没有装插件「{name}」")))?;
                let directory = directory_of(&name);
                if directory.exists() {
                    trash(&directory).map_err(failed)?;
                }
                let mut list_all = registry();
                list_all.retain(|plugin| !plugin["name"].as_str().is_some_and(|other| other.eq_ignore_ascii_case(&name)));
                write_registry(&list_all).map_err(failed)?;
                changed();
                Ok(list())
            }
            other => Err((-32601, format!("没有这个方法：{other}"))),
        }
    }
}

// MARK: - 能力提供方（本机的：本地语音、生图 / 生视频）

mod caps {
    //! 能力层（照 Friday 的 CapabilityRegistry + PowerBackend）：装好的本地能力是「本机」提供方（`power-<id>`）——本地语音能听录音、说话，
    //! friday-diffusion 能生图、生视频。智能体看到的是固定的几个工具（transcribe_audio、speak、generate_image、generate_video、check_job），
    //! 是常驻那份的动作；有提供方开着、这项没关的才给。生视频是异步任务（jobs.json），check_job 查进度、做完取回文件。
    //! 设置 › 工具 › 能力提供方 看着这台时：列这些提供方、开关、关掉某一项、改名字（存在 ~/.edith/friday/capabilities.json）。

    use std::io::Read;
    use std::path::PathBuf;
    use std::sync::{LazyLock, Mutex};
    use std::time::Duration;

    use base64::Engine;
    use serde_json::{Map, Value, json};

    use crate::{models, powers};

    struct Kind {
        raw: &'static str,
        title: &'static str,
        symbol: &'static str,
        tool: &'static str,
        detail: &'static str,
        /// 装着的 Power 清单里的 capabilities 写的
        power: &'static str,
    }

    const KINDS: [Kind; 4] = [
        Kind {
            raw: "transcription",
            title: "听录音",
            symbol: "waveform",
            tool: "transcribe_audio",
            detail: "把录音转成文字",
            power: "transcription",
        },
        Kind { raw: "speech", title: "说话", symbol: "speaker.wave.2", tool: "speak", detail: "把文字合成语音", power: "speech" },
        Kind {
            raw: "imageGeneration",
            title: "生图",
            symbol: "photo",
            tool: "generate_image",
            detail: "按描述生成图片",
            power: "imageGeneration",
        },
        Kind {
            raw: "videoGeneration",
            title: "生视频",
            symbol: "film",
            tool: "generate_video",
            detail: "按描述生成短视频",
            power: "videoGeneration",
        },
    ];

    fn kind(raw: &str) -> Option<&'static Kind> {
        KINDS.iter().find(|kind| kind.raw == raw || kind.tool == raw)
    }

    fn settings_path() -> PathBuf {
        crate::serve_dir().join("capabilities.json")
    }

    fn jobs_path() -> PathBuf {
        crate::serve_dir().join("jobs.json")
    }

    pub fn media_dir() -> PathBuf {
        crate::serve_dir().join("media")
    }

    /// 本机的提供方：装着、清单里写了这几项能力的 Power（用户的开关、关掉的项、名字记在 capabilities.json 的 powerEntries）
    fn entries() -> Vec<Value> {
        let saved = crate::read_json_file(&settings_path());
        let state = powers::state();
        state["powers"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|power| power["installedVersion"].as_str().is_some_and(|version| !version.is_empty()))
            .filter_map(|power| {
                let id = power["id"].as_str().unwrap_or("").to_string();
                let declared: Vec<&str> = power["capabilities"].as_array().into_iter().flatten().filter_map(Value::as_str).collect();
                let kinds: Vec<&str> = KINDS.iter().filter(|kind| declared.contains(&kind.power)).map(|kind| kind.raw).collect();
                if kinds.is_empty() {
                    return None;
                }
                let entry_id = format!("power-{id}");
                let user = &saved["powerEntries"][&entry_id];
                Some(json!({
                    "id": entry_id,
                    "type": "power",
                    "title": user["title"].as_str().map(str::to_string).unwrap_or_else(|| power["title"].as_str().unwrap_or(&id).to_string()),
                    "options": { "power": id, "capabilities": kinds.join(",") },
                    "enabled": user["enabled"].as_bool().unwrap_or(true),
                    "disabledKinds": user["disabledKinds"].as_array().cloned().unwrap_or_default(),
                    "kinds": kinds,
                    "powerEnabled": power["enabled"].as_bool().unwrap_or(true),
                }))
            })
            .collect()
    }

    fn save_user(entry_id: &str, change: impl FnOnce(&mut Map<String, Value>)) {
        let path = settings_path();
        let mut saved = crate::read_json_file(&path);
        if !saved.is_object() {
            saved = json!({});
        }
        if !saved["powerEntries"].is_object() {
            saved["powerEntries"] = json!({});
        }
        let entries = saved["powerEntries"].as_object_mut().unwrap();
        let entry = entries.entry(entry_id.to_string()).or_insert_with(|| json!({}));
        if !entry.is_object() {
            *entry = json!({});
        }
        change(entry.as_object_mut().unwrap());
        let _ = crate::write_json_file(&path, &saved);
        changed();
    }

    /// 这一项能不能用、还缺什么
    fn availability(entry: &Value, raw: &str) -> (bool, String) {
        let power = entry["options"]["power"].as_str().unwrap_or("");
        if !powers::is_installed(power) {
            return (false, "还没安装（设置 › 能力）".into());
        }
        if !powers::is_enabled(power) {
            return (false, "被关掉了（设置 › 能力）".into());
        }
        if raw == "imageGeneration" || raw == "videoGeneration" {
            let what = if raw == "videoGeneration" { "video" } else { "image" };
            let model = models::selected(what);
            if model.is_empty() {
                return (
                    false,
                    if what == "video" {
                        "还没选本地生视频模型（模型库里类型选「生视频」，点「使用」）".into()
                    } else {
                        "还没选本地生图模型（模型库里类型选「生图」，点「使用」）".into()
                    },
                );
            }
            if models::diffusion_request(&model).is_none() {
                return (false, format!("{model} 的文件还没下完（设置 › 能力里能看到进度）"));
            }
        }
        (true, String::new())
    }

    /// 这项能力现在用哪个提供方（开着、这项没关的第一个）
    fn provider_for(raw: &str) -> Option<Value> {
        entries().into_iter().find(|entry| {
            entry["enabled"] == true
                && entry["kinds"].as_array().is_some_and(|kinds| kinds.contains(&json!(raw)))
                && !entry["disabledKinds"].as_array().is_some_and(|kinds| kinds.contains(&json!(raw)))
        })
    }

    // MARK: 动作

    fn property(description: &str, kind: &str) -> Value {
        json!({ "type": kind, "description": description })
    }

    /// 常驻那份的动作（有提供方的才给）
    pub fn actions() -> Vec<Value> {
        let mut actions = Vec::new();
        if provider_for("transcription").is_some() {
            actions.push(json!({ "name": "transcribe_audio", "description": "听一段录音（语音消息、会议录音、视频里的声音）并转成文字。录音是本地路径，常见格式（wav、mp3、flac）都行。",
                "parameters": { "type": "object", "properties": { "audio": property("录音的本地路径", "string"), "language": property("语言：zh、en、ja 等；不填自动判断", "string") }, "required": ["audio"] },
                "readOnly": true, "timeoutSeconds": 300 }));
        }
        if provider_for("speech").is_some() {
            actions.push(json!({ "name": "speak", "description": "把一段话说出来：合成语音，返回 wav 文件的路径。用户让你「说」「念」「读出来」「用语音回答」时用它。text 写成要说的原话，不要带 Markdown 符号、代码和表格。",
                "parameters": { "type": "object", "properties": { "text": property("要说的话，原样朗读（中英文等皆可）", "string"), "voice": property("音色（本地语音的音色名）；不填用默认音色", "string"), "style": property("语气或情绪，一句话描述；不填为自然语气", "string") }, "required": ["text"] },
                "timeoutSeconds": 300 }));
        }
        if provider_for("imageGeneration").is_some() {
            actions.push(json!({ "name": "generate_image", "description": "按文字描述生成图片（在这台的显卡上用本地生图模型）。生成的图片会直接展示给用户并返回文件路径。提示词写具体：主体、风格、构图、光线。",
                "parameters": { "type": "object", "properties": {
                    "prompt": property("画面描述（中英文皆可）", "string"), "negative_prompt": property("不希望出现的内容", "string"),
                    "size": property("尺寸，如 1024x1024、1280x720；不填用默认", "string"), "count": property("张数，默认 1，最多 4", "integer"),
                    "reference_image": property("参考图路径（图生图 / 编辑）；不填则纯文生图", "string") }, "required": ["prompt"] },
                "timeoutSeconds": 900 }));
        }
        if provider_for("videoGeneration").is_some() {
            actions.push(json!({ "name": "generate_video", "description": "按文字（可附一张参考图）生成一段短视频（本地生视频模型）。这是异步任务：调用后立刻返回任务 ID，通常要等几分钟，之后用 check_job 查询并取回视频。",
                "parameters": { "type": "object", "properties": {
                    "prompt": property("画面与动作描述", "string"), "duration": property("时长（秒），默认 2，最多 10", "integer"),
                    "size": property("分辨率，如 832x480；不填用默认", "string"), "image": property("首帧参考图的路径（图生视频）；不填则文生视频", "string") }, "required": ["prompt"] },
                "timeoutSeconds": 120 }));
            actions.push(json!({ "name": "check_job", "description": "查询 generate_video 等异步任务的进度。完成后会把结果文件取回来展示给用户并返回路径；未完成就告诉用户稍后再查（间隔至少 30 秒，不要连续轮询）。",
                "parameters": { "type": "object", "properties": { "job_id": property("任务 ID", "string") }, "required": ["job_id"] }, "readOnly": true, "timeoutSeconds": 300 }));
        }
        actions
    }

    pub fn is_action(action: &str) -> bool {
        matches!(action, "transcribe_audio" | "speak" | "generate_image" | "generate_video" | "check_job")
    }

    fn text<'a>(arguments: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
        arguments.get(key).and_then(Value::as_str).map(str::trim).filter(|text| !text.is_empty())
    }

    /// 参数里的路径：相对的按会话的工作目录
    fn path_of(value: &str, caller: Option<&str>) -> PathBuf {
        let path = PathBuf::from(value.strip_prefix("file://").unwrap_or(value));
        if path.is_absolute() {
            return path;
        }
        if let Some(rest) = value.strip_prefix("~/") {
            return crate::user_home().join(rest);
        }
        caller.map(PathBuf::from).unwrap_or_else(crate::user_home).join(path)
    }

    fn data_url(path: &std::path::Path) -> Result<String, String> {
        let bytes = std::fs::read(path).map_err(|_| format!("读不了 {}", path.display()))?;
        let lower = path.to_string_lossy().to_lowercase();
        let mime = if lower.ends_with(".jpg") || lower.ends_with(".jpeg") {
            "image/jpeg"
        } else if lower.ends_with(".webp") {
            "image/webp"
        } else {
            "image/png"
        };
        Ok(format!("data:{mime};base64,{}", base64::engine::general_purpose::STANDARD.encode(bytes)))
    }

    fn send(method: &str, url: &str, token: &str, body: Option<&Value>, timeout: u64) -> Result<(u16, Vec<u8>), String> {
        let agent: ureq::Agent =
            ureq::Agent::config_builder().http_status_as_error(false).timeout_global(Some(Duration::from_secs(timeout))).build().into();
        let authorization = format!("Bearer {token}");
        let response = match (method, body) {
            ("GET", _) => agent.get(url).header("Authorization", &authorization).call(),
            (_, Some(body)) => agent
                .post(url)
                .header("Authorization", &authorization)
                .header("Content-Type", "application/json")
                .send(&serde_json::to_vec(body).unwrap_or_default()[..]),
            _ => agent.post(url).header("Authorization", &authorization).send_empty(),
        };
        let mut response = response.map_err(|error| error.to_string())?;
        let status = response.status().as_u16();
        let mut data = Vec::new();
        response.body_mut().as_reader().read_to_end(&mut data).map_err(|error| error.to_string())?;
        Ok((status, data))
    }

    fn json_of(status: u16, data: &[u8], what: &str) -> Result<Value, String> {
        let value: Value = serde_json::from_slice(data).unwrap_or_default();
        if !(200..300).contains(&status) {
            let message = value["error"]["message"].as_str().map(str::to_string).unwrap_or_else(|| format!("HTTP {status}"));
            return Err(format!("{what}出错：{message}"));
        }
        Ok(value)
    }

    /// 结果存进 ~/.edith/friday/media/
    fn keep(data: &[u8], extension: &str) -> Result<String, String> {
        let dir = media_dir();
        std::fs::create_dir_all(&dir).map_err(|error| error.to_string())?;
        let path = dir.join(format!(
            "{}-{}.{extension}",
            chrono::Local::now().format("%Y%m%d-%H%M%S"),
            &uuid::Uuid::new_v4().simple().to_string()[..6]
        ));
        std::fs::write(&path, data).map_err(|error| format!("存不了结果：{error}"))?;
        Ok(path.to_string_lossy().into_owned())
    }

    fn outputs(url: &str, token: &str, job: &Value) -> Result<Vec<String>, String> {
        job["outputs"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|output| {
                let (status, data) = send("GET", &format!("{url}{}", output["url"].as_str().unwrap_or("")), token, None, 120)?;
                if !(200..300).contains(&status) {
                    return Err(format!("取本地生成的结果失败：HTTP {status}"));
                }
                keep(&data, if output["mime"] == "video/webm" { "webm" } else { "png" })
            })
            .collect()
    }

    fn diffusion_body(arguments: &Map<String, Value>, what: &str, caller: Option<&str>) -> Result<Value, String> {
        let model = models::selected(what);
        let files = models::diffusion_request(&model).ok_or_else(|| {
            if model.is_empty() {
                format!(
                    "还没选本地生{}模型：在模型库里类型选「生{}」，点「使用」",
                    if what == "video" { "视频" } else { "图" },
                    if what == "video" { "视频" } else { "图" }
                )
            } else {
                format!("本地模型 {model} 的文件还没下完")
            }
        })?;
        let mut body = json!({ "kind": what, "prompt": text(arguments, "prompt").unwrap_or(""), "x_friday": files });
        if let Some(negative) = text(arguments, "negative_prompt") {
            body["negative_prompt"] = json!(negative);
        }
        if let Some((width, height)) = text(arguments, "size").and_then(|size| size.split_once(['x', 'X', '×'])) {
            let number = |text: &str| text.trim().parse::<u32>().ok().map(|value| (value / 16 * 16).max(64));
            if let (Some(width), Some(height)) = (number(width), number(height)) {
                body["width"] = json!(width);
                body["height"] = json!(height);
            }
        }
        if what == "image" {
            body["n"] = json!(arguments.get("count").and_then(Value::as_u64).unwrap_or(1).clamp(1, 4));
            if let Some(reference) = text(arguments, "reference_image") {
                let url = data_url(&path_of(reference, caller))?;
                if models::family_edits(&model) {
                    body["reference_images"] = json!([url]);
                } else {
                    body["image"] = json!(url);
                }
            }
        } else {
            // 时长（秒）→ 帧数：16 fps，帧数取 4k+1（Wan 的要求）
            let seconds = arguments.get("duration").and_then(Value::as_u64).unwrap_or(2).clamp(1, 10);
            body["fps"] = json!(16);
            body["frames"] = json!(seconds * 16 / 4 * 4 + 1);
            if let Some(image) = text(arguments, "image") {
                body["image"] = json!(data_url(&path_of(image, caller))?);
            }
        }
        Ok(body)
    }

    /// 做一个动作：(成没成, 给模型看的, 产出的文件)
    pub fn act(action: &str, arguments: &Map<String, Value>, caller: Option<&str>) -> (bool, String, Vec<String>) {
        match perform(action, arguments, caller) {
            Ok((text, files)) => (true, text, files),
            Err(error) => (false, error, Vec::new()),
        }
    }

    fn perform(action: &str, arguments: &Map<String, Value>, caller: Option<&str>) -> Result<(String, Vec<String>), String> {
        if action == "check_job" {
            return check_job(text(arguments, "job_id").ok_or("要给 job_id")?);
        }
        let kind = kind(action).ok_or_else(|| format!("没有这个能力：{action}"))?;
        let entry = provider_for(kind.raw).ok_or_else(|| format!("没有开着的「{}」提供方", kind.title))?;
        let (ready, needs) = availability(&entry, kind.raw);
        if !ready {
            return Err(format!("「{}」现在用不了：{needs}", kind.title));
        }
        let power = entry["options"]["power"].as_str().unwrap_or("").to_string();
        let (url, token) = powers::ensure_running(&power, Duration::from_secs(150))?;
        match action {
            "transcribe_audio" => {
                let path = path_of(text(arguments, "audio").ok_or("要给 audio（录音的路径）")?, caller);
                let data = std::fs::read(&path).map_err(|_| format!("读不了录音 {}", path.display()))?;
                let boundary = format!("Friday-{}", uuid::Uuid::new_v4().simple());
                let mut body: Vec<u8> = Vec::new();
                let mut field = |key: &str, value: &str| {
                    body.extend_from_slice(
                        format!("--{boundary}\r\nContent-Disposition: form-data; name=\"{key}\"\r\n\r\n{value}\r\n").as_bytes(),
                    )
                };
                field("model", "sensevoice-small");
                field("response_format", "json");
                if let Some(language) = text(arguments, "language") {
                    field("language", language);
                }
                let name = path.file_name().map(|name| name.to_string_lossy().into_owned()).unwrap_or_else(|| "audio.wav".into());
                body.extend_from_slice(format!("--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"{name}\"\r\nContent-Type: application/octet-stream\r\n\r\n").as_bytes());
                body.extend_from_slice(&data);
                body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
                let mut response = crate::http_agent()
                    .post(&format!("{url}/v1/audio/transcriptions"))
                    .header("Authorization", format!("Bearer {token}"))
                    .header("Content-Type", format!("multipart/form-data; boundary={boundary}"))
                    .send(&body[..])
                    .map_err(|error| format!("听录音出错：{error}"))?;
                let status = response.status().as_u16();
                let mut reply = Vec::new();
                let _ = response.body_mut().as_reader().read_to_end(&mut reply);
                let value = json_of(status, &reply, "听录音")?;
                let text = value["text"].as_str().unwrap_or("").trim().to_string();
                Ok((if text.is_empty() { "（没听出字来）".into() } else { text }, Vec::new()))
            }
            "speak" => {
                let input = text(arguments, "text").ok_or("要给 text（要说的话）")?;
                let mut body = json!({ "model": "cosyvoice3", "input": input, "voice": text(arguments, "voice").unwrap_or("default"), "response_format": "wav" });
                if let Some(style) = text(arguments, "style") {
                    body["instructions"] = json!(style);
                }
                let (status, data) = send("POST", &format!("{url}/v1/audio/speech"), &token, Some(&body), 280)?;
                if !(200..300).contains(&status) {
                    return Err(json_of(status, &data, "说话").err().unwrap_or_default());
                }
                let path = keep(&data, "wav")?;
                Ok((format!("合成好了，语音文件：{path}"), vec![path]))
            }
            "generate_image" => {
                let body = diffusion_body(arguments, "image", caller)?;
                let (status, data) = send("POST", &format!("{url}/v1/jobs"), &token, Some(&body), 60)?;
                let job = json_of(status, &data, "本地生图")?;
                let id = job["id"].as_str().unwrap_or("").to_string();
                let started = std::time::Instant::now();
                loop {
                    let (status, data) = send("GET", &format!("{url}/v1/jobs/{id}"), &token, None, 60)?;
                    let state = json_of(status, &data, "本地生图")?;
                    match state["status"].as_str() {
                        Some("done") => {
                            let files = outputs(&url, &token, &state)?;
                            let text = format!(
                                "已用本机的 {} 生成 {} 张图片（{:.1} 秒）：\n{}",
                                models::selected("image"),
                                files.len(),
                                state["elapsed_ms"].as_f64().unwrap_or(0.0) / 1000.0,
                                files.join("\n")
                            );
                            return Ok((text, files));
                        }
                        Some("failed" | "cancelled") => {
                            return Err(state["error"]["message"].as_str().unwrap_or("本地生图失败").to_string());
                        }
                        _ if started.elapsed() > Duration::from_secs(840) => {
                            let _ = send("POST", &format!("{url}/v1/jobs/{id}/cancel"), &token, None, 10);
                            return Err("本地生图超过 14 分钟没做完，取消了".into());
                        }
                        _ => std::thread::sleep(Duration::from_millis(500)),
                    }
                }
            }
            _ => {
                // 生视频：提交任务，记进 jobs.json，check_job 取
                let body = diffusion_body(arguments, "video", caller)?;
                let (status, data) = send("POST", &format!("{url}/v1/jobs"), &token, Some(&body), 60)?;
                let job = json_of(status, &data, "本地生视频")?;
                let remote = job["id"].as_str().unwrap_or("").to_string();
                let id = uuid::Uuid::new_v4().to_string().to_uppercase();
                let record = json!({
                    "id": id, "kind": "videoGeneration", "provider": entry["id"], "providerTitle": entry["title"], "remoteID": format!("diffusion:{remote}"),
                    "status": "running", "prompt": text(arguments, "prompt").unwrap_or(""), "message": "本地生视频排队中",
                    "createdAt": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true), "files": [],
                });
                with_jobs(|jobs| jobs.push(record));
                Ok((
                    format!(
                        "本机的 {} 正在生成视频（{} 帧），任务 ID：{id}。通常要等几分钟，之后用 check_job 查询并取回视频。",
                        models::selected("video"),
                        body["frames"]
                    ),
                    Vec::new(),
                ))
            }
        }
    }

    static JOBS_LOCK: Mutex<()> = Mutex::new(());

    fn with_jobs<T>(change: impl FnOnce(&mut Vec<Value>) -> T) -> T {
        let _guard = JOBS_LOCK.lock().unwrap();
        let mut jobs: Vec<Value> = crate::read_json_file(&jobs_path())["jobs"].as_array().cloned().unwrap_or_default();
        let result = change(&mut jobs);
        let _ = crate::write_json_file(&jobs_path(), &json!({ "jobs": jobs }));
        drop(_guard);
        changed();
        result
    }

    fn check_job(id: &str) -> Result<(String, Vec<String>), String> {
        let job = with_jobs(|jobs| jobs.iter().find(|job| job["id"].as_str().is_some_and(|other| other.eq_ignore_ascii_case(id))).cloned())
            .ok_or_else(|| format!("没有任务 {id}"))?;
        if job["status"] == "succeeded" {
            let files: Vec<String> = job["files"].as_array().into_iter().flatten().filter_map(Value::as_str).map(str::to_string).collect();
            return Ok((format!("已经做好了：\n{}", files.join("\n")), files));
        }
        if job["status"] == "failed" {
            return Err(format!("任务没做成：{}", job["message"].as_str().unwrap_or("")));
        }
        let remote = job["remoteID"].as_str().and_then(|remote| remote.strip_prefix("diffusion:")).ok_or("不认识的本地任务")?.to_string();
        let power = job["provider"].as_str().and_then(|provider| provider.strip_prefix("power-")).unwrap_or("diffusion").to_string();
        let (url, token) = powers::ensure_running(&power, Duration::from_secs(150))?;
        let (status, data) = send("GET", &format!("{url}/v1/jobs/{remote}"), &token, None, 60)?;
        let update = |status: &str, message: &str, files: &[String]| {
            let (id, status, message, files) = (job["id"].clone(), status.to_string(), message.to_string(), files.to_vec());
            with_jobs(move |jobs| {
                if let Some(item) = jobs.iter_mut().find(|item| item["id"] == id) {
                    item["status"] = json!(status);
                    item["message"] = json!(message);
                    item["files"] = json!(files);
                }
            });
        };
        if status == 404 {
            update("failed", "本地生视频的任务没了（friday-diffusion 重启过，任务不保留）", &[]);
            return Err("本地生视频的任务没了（friday-diffusion 重启过，任务不保留）".into());
        }
        let state = json_of(status, &data, "本地生视频")?;
        match state["status"].as_str() {
            Some("done") => {
                let files = outputs(&url, &token, &state)?;
                let _ = send("POST", &format!("{url}/v1/memory/release"), &token, None, 30);
                update("succeeded", &format!("本机生成的视频（{:.0} 秒）", state["elapsed_ms"].as_f64().unwrap_or(0.0) / 1000.0), &files);
                Ok((format!("视频做好了：\n{}", files.join("\n")), files))
            }
            Some(failed @ ("failed" | "cancelled")) => {
                let message = state["error"]["message"]
                    .as_str()
                    .unwrap_or(if failed == "cancelled" { "取消了" } else { "本地生视频失败" })
                    .to_string();
                update("failed", &message, &[]);
                Err(message)
            }
            other => {
                let message = match other {
                    Some("queued") => "本地生视频排队中".to_string(),
                    Some("loading") => "本地生视频：正在加载模型".to_string(),
                    Some("encoding") => "本地生视频：正在编码".to_string(),
                    _ => format!(
                        "本地生视频：{:.0}%（第 {} / {} 步）",
                        state["progress"].as_f64().unwrap_or(0.0) * 100.0,
                        state["step"],
                        state["steps"]
                    ),
                };
                update("running", &message, &[]);
                Ok((format!("还没做完：{message}。过一会儿（至少 30 秒）再查。"), Vec::new()))
            }
        }
    }

    // MARK: 设置页

    /// 上次推出去的（设置页的状态、动作名）：没变不推
    static LAST: LazyLock<Mutex<(String, String)>> = LazyLock::new(|| Mutex::new((String::new(), String::new())));

    /// 提供方、动作变了：设置页重列，大脑的动作换一份（本地能力下载时每 80 毫秒一次，只在真变了时推）
    fn changed() {
        let page = state().to_string();
        let names = serde_json::to_string(&actions().iter().map(|action| action["name"].clone()).collect::<Vec<_>>()).unwrap_or_default();
        let (page_changed, names_changed) = {
            let mut last = LAST.lock().unwrap();
            let changed = (last.0 != page, last.1 != names);
            *last = (page, names);
            changed
        };
        if page_changed {
            crate::notify_all_clients("capabilities", json!({}));
        }
        if names_changed {
            crate::serve_update_actions();
        }
    }

    /// 本地能力变了（装了、关了、模型选了）：提供方跟着
    pub fn powers_changed() {
        changed();
    }

    /// CapabilitiesState
    pub fn state() -> Value {
        let mut available = Map::new();
        let entries: Vec<Value> = entries()
            .into_iter()
            .map(|entry| {
                let disabled: Vec<String> = entry["disabledKinds"].as_array().into_iter().flatten().filter_map(Value::as_str).map(str::to_string).collect();
                let kinds: Vec<&str> = entry["kinds"].as_array().into_iter().flatten().filter_map(Value::as_str).collect();
                let infos: Vec<Value> = kinds
                    .iter()
                    .filter_map(|raw| kind(raw))
                    .map(|kind| {
                        let (ready, needs) = availability(&entry, kind.raw);
                        available.insert(format!("{}|{}", entry["id"].as_str().unwrap_or(""), kind.raw), json!({ "ready": ready, "needs": needs }));
                        json!({ "raw": kind.raw, "title": kind.title, "symbol": kind.symbol, "toolName": kind.tool, "detail": kind.detail, "description": kind.detail,
                            "isBuiltin": true, "allowed": !disabled.iter().any(|item| item == kind.raw) })
                    })
                    .collect();
                let mut provider = entry.clone();
                if let Some(object) = provider.as_object_mut() {
                    object.remove("powerEnabled");
                }
                provider["kindInfos"] = json!(infos);
                provider["secretNames"] = json!([]);
                provider["active"] = json!(entry["enabled"] == true && entry["powerEnabled"] == true);
                provider["isLocal"] = json!(true);
                provider["isScript"] = json!(false);
                provider["createdByAgent"] = json!(false);
                provider["symbol"] = json!("cpu");
                provider
            })
            .collect();
        let jobs: Vec<Value> = crate::read_json_file(&jobs_path())["jobs"].as_array().cloned().unwrap_or_default();
        json!({ "entries": entries, "packages": [], "problems": [], "jobs": jobs, "availability": available, "optionSpecs": {}, "packageFor": {},
            "providersDirectory": crate::serve_dir().join("providers").to_string_lossy() })
    }

    /// capabilities.*（设置 › 工具 › 能力提供方看着这台时）
    pub fn request(method: &str, params: &Value) -> crate::Reply {
        let id = || params["id"].as_str().filter(|id| !id.is_empty()).map(str::to_string).ok_or((-32602, "缺少参数 id".to_string()));
        let known = |id: &str| entries().iter().any(|entry| entry["id"] == json!(id));
        match method {
            "capabilities.state" | "capabilities.refreshAvailability" => {
                if method == "capabilities.refreshAvailability" {
                    changed();
                }
                return Ok(state());
            }
            "capabilities.setEnabled" => {
                let id = id()?;
                if !known(&id) {
                    return Err((crate::NOT_FOUND, format!("没有提供方 {id}")));
                }
                let enabled = params["enabled"].as_bool().unwrap_or(true);
                save_user(&id, |entry| {
                    entry.insert("enabled".into(), json!(enabled));
                });
            }
            "capabilities.setKindAllowed" => {
                let id = id()?;
                let raw = params["kind"].as_str().unwrap_or("").to_string();
                if !known(&id) || kind(&raw).is_none() {
                    return Err((crate::NOT_FOUND, format!("没有提供方 {id} 的这项能力")));
                }
                let allowed = params["allowed"].as_bool().unwrap_or(true);
                save_user(&id, |entry| {
                    let mut disabled: Vec<Value> = entry.get("disabledKinds").and_then(Value::as_array).cloned().unwrap_or_default();
                    disabled.retain(|item| *item != json!(raw));
                    if !allowed {
                        disabled.push(json!(raw));
                    }
                    entry.insert("disabledKinds".into(), json!(disabled));
                });
            }
            "capabilities.setTitle" => {
                let id = id()?;
                let title = params["title"].as_str().unwrap_or("").trim().to_string();
                save_user(&id, |entry| {
                    if title.is_empty() {
                        entry.remove("title");
                    } else {
                        entry.insert("title".into(), json!(title));
                    }
                });
            }
            "capabilities.deleteJob" => {
                let id = id()?;
                with_jobs(|jobs| jobs.retain(|job| job["id"] != json!(id)));
            }
            // 本机的提供方没有选项；提供方包（脚本提供方）这台的 EDITH 还不支持
            "capabilities.setOption" => {}
            "capabilities.retest" | "capabilities.import" | "capabilities.export" | "capabilities.deletePackage" => {
                return Ok(
                    json!({ "ok": false, "error": "这台的 EDITH 还不支持提供方包（脚本提供方）：本机的提供方跟着设置 › 能力里装的本地能力走" }),
                );
            }
            other => return Err((-32601, format!("没有这个方法：{other}"))),
        }
        Ok(json!({}))
    }
}

// MARK: - 内置终端（伪终端：macOS / Linux 的 posix_openpt，Windows 的 ConPTY）

mod pty {
    //! 内置终端（Friday 的 term.*，Friday 仓库的 qt/docs/TERMINAL.md）：大脑开伪终端跑 shell，客户端断了照常跑，连上来的接着看
    //! （最近 512 KB 的原始输出重放）。macOS / Linux 用 posix_openpt，Windows 用 ConPTY。
    //! 每个终端记着谁接着（连接名）：输出推 `terminal.output` 给它们；开了、关了、退出了推 `terminals`（全部终端）给全部全权的连接。

    use std::collections::{HashMap, HashSet, VecDeque};
    use std::io::{Read, Write};
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, LazyLock, Mutex};
    use std::time::{Duration, Instant};

    use serde_json::{Value, json};

    /// 重放最多留多少输出
    const KEEP: usize = 512 * 1024;

    /// 伪终端的那一头：写进去、改大小、结束
    trait Pty: Send + Sync {
        fn write(&self, data: &[u8]) -> std::io::Result<()>;
        fn resize(&self, cols: u16, rows: u16);
        fn kill(&self);
    }

    pub struct Terminal {
        id: String,
        session: String,
        title: String,
        command: Option<String>,
        size: Mutex<(u16, u16)>,
        output: Mutex<VecDeque<u8>>,
        /// 最后一次有输出的时候（Windows 上等 shell 起好再写开时带的命令）
        last_output: Mutex<Option<Instant>>,
        running: AtomicBool,
        exit_code: Mutex<Option<i32>>,
        /// 接着它的连接（能力名）
        watchers: Mutex<HashSet<String>>,
        pty: Box<dyn Pty>,
    }

    static TERMINALS: LazyLock<Mutex<HashMap<String, Arc<Terminal>>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

    fn base64(bytes: &[u8]) -> String {
        use base64::Engine;
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }

    impl Terminal {
        fn info(&self) -> Value {
            let (cols, rows) = *self.size.lock().unwrap();
            json!({
                "id": self.id, "conversationID": self.session, "title": self.title, "shell": self.title, "command": self.command,
                "running": self.running.load(Ordering::Relaxed), "exitCode": *self.exit_code.lock().unwrap(), "cols": cols, "rows": rows,
            })
        }

        /// 读到输出：记下（重放用）、推给接着的
        fn output(&self, data: &[u8]) {
            {
                let mut kept = self.output.lock().unwrap();
                kept.extend(data);
                let extra = kept.len().saturating_sub(KEEP);
                kept.drain(..extra);
            }
            *self.last_output.lock().unwrap() = Some(Instant::now());
            let watchers: Vec<String> = self.watchers.lock().unwrap().iter().cloned().collect();
            if !watchers.is_empty() {
                crate::notify_clients(&watchers, "terminal.output", json!({ "id": self.id, "data": base64(data) }));
            }
        }

        fn exited(&self, code: Option<i32>) {
            self.running.store(false, Ordering::Relaxed);
            *self.exit_code.lock().unwrap() = code;
            changed();
        }
    }

    /// 全部终端（TerminalInfo）
    /// 有在跑的终端（常驻的那份闲着不退）
    pub fn any_running() -> bool {
        TERMINALS.lock().unwrap().values().any(|terminal| terminal.running.load(Ordering::Relaxed))
    }

    pub fn list() -> Vec<Value> {
        let mut all: Vec<Arc<Terminal>> = TERMINALS.lock().unwrap().values().cloned().collect();
        all.sort_by(|a, b| a.id.cmp(&b.id));
        all.iter().map(|terminal| terminal.info()).collect()
    }

    fn changed() {
        crate::notify_all_clients("terminals", json!({ "terminals": list() }));
    }

    fn find(id: &str) -> Result<Arc<Terminal>, String> {
        TERMINALS.lock().unwrap().get(id).cloned().ok_or_else(|| format!("没有终端 {id}"))
    }

    /// 用的 shell：Unix 的 $SHELL（没有用 /bin/sh），Windows 的 PowerShell
    fn shell() -> (String, Vec<String>) {
        if cfg!(windows) {
            return ("powershell.exe".into(), vec!["-NoLogo".into()]);
        }
        let shell = std::env::var("SHELL").ok().filter(|shell| Path::new(shell).is_file()).unwrap_or_else(|| "/bin/sh".into());
        (shell, vec!["-l".into()])
    }

    /// 开一个（id 是客户端给的；watcher：开它的连接，开好就接着它）。command 起好后打进去回车
    pub fn open(id: &str, session: &str, cwd: &Path, cols: u16, rows: u16, command: Option<&str>, watcher: &str) -> Result<Value, String> {
        if id.is_empty() || TERMINALS.lock().unwrap().contains_key(id) {
            return Err(format!("终端 id 不对或已经有了：{id}"));
        }
        let (cols, rows) = (cols.max(2), rows.max(1));
        let (program, args) = shell();
        let title = Path::new(&program).file_stem().map(|name| name.to_string_lossy().into_owned()).unwrap_or_else(|| program.clone());
        let cwd = if cwd.is_dir() { cwd.to_path_buf() } else { crate::user_home() };
        let (pty, reader, waiter) = spawn(&program, &args, &cwd, cols, rows)?;
        let terminal = Arc::new(Terminal {
            id: id.to_string(),
            session: session.to_string(),
            title,
            command: command.map(str::to_string),
            size: Mutex::new((cols, rows)),
            output: Mutex::new(VecDeque::new()),
            last_output: Mutex::new(None),
            running: AtomicBool::new(true),
            exit_code: Mutex::new(None),
            watchers: Mutex::new(HashSet::from([watcher.to_string()])),
            pty,
        });
        TERMINALS.lock().unwrap().insert(id.to_string(), terminal.clone());
        {
            let terminal = terminal.clone();
            let mut reader = reader;
            std::thread::spawn(move || {
                let mut buffer = [0u8; 16 * 1024];
                loop {
                    match reader.read(&mut buffer) {
                        Ok(0) | Err(_) => break,
                        Ok(count) => terminal.output(&buffer[..count]),
                    }
                }
            });
        }
        {
            let terminal = terminal.clone();
            std::thread::spawn(move || terminal.exited(waiter()));
        }
        if let Some(command) = command.filter(|command| !command.trim().is_empty()) {
            if cfg!(windows) {
                // PowerShell 起得慢（windows-arm64 上好几秒），起好之前写进去的会被吞掉：等它出过提示符、安静 0.5 秒再写
                let terminal = terminal.clone();
                let line = format!("{command}\r");
                std::thread::spawn(move || {
                    let deadline = Instant::now() + Duration::from_secs(20);
                    while Instant::now() < deadline && terminal.running.load(Ordering::Relaxed) {
                        let quiet = terminal.last_output.lock().unwrap().is_some_and(|at| at.elapsed() >= Duration::from_millis(500));
                        if quiet {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(100));
                    }
                    let _ = terminal.pty.write(line.as_bytes());
                });
            } else {
                let _ = terminal.pty.write(format!("{command}\n").as_bytes());
            }
        }
        changed();
        Ok(terminal.info())
    }

    /// 接上：之后的输出推给它；回最近的输出和现在的大小
    pub fn attach(id: &str, watcher: &str) -> Result<Value, String> {
        let terminal = find(id)?;
        terminal.watchers.lock().unwrap().insert(watcher.to_string());
        let data: Vec<u8> = terminal.output.lock().unwrap().iter().copied().collect();
        let (cols, rows) = *terminal.size.lock().unwrap();
        Ok(json!({ "data": base64(&data), "cols": cols, "rows": rows }))
    }

    pub fn detach(id: &str, watcher: &str) -> Result<Value, String> {
        find(id)?.watchers.lock().unwrap().remove(watcher);
        Ok(json!({}))
    }

    /// 连接断了：不再推给它
    pub fn forget(watcher: &str) {
        for terminal in TERMINALS.lock().unwrap().values() {
            terminal.watchers.lock().unwrap().remove(watcher);
        }
    }

    pub fn write(id: &str, data: &[u8]) -> Result<Value, String> {
        find(id)?.pty.write(data).map_err(|error| format!("写不进终端：{error}"))?;
        Ok(json!({}))
    }

    pub fn resize(id: &str, cols: u16, rows: u16) -> Result<Value, String> {
        let terminal = find(id)?;
        let (cols, rows) = (cols.max(2), rows.max(1));
        *terminal.size.lock().unwrap() = (cols, rows);
        terminal.pty.resize(cols, rows);
        changed();
        Ok(json!({}))
    }

    /// 结束 shell 和它起的进程，从列表去掉
    pub fn close(id: &str) -> Result<Value, String> {
        let terminal = TERMINALS.lock().unwrap().remove(id).ok_or_else(|| format!("没有终端 {id}"))?;
        terminal.pty.kill();
        changed();
        Ok(json!({}))
    }

    /// 大脑退出时：都结束
    pub fn close_all() {
        let all: Vec<Arc<Terminal>> = TERMINALS.lock().unwrap().drain().map(|(_, terminal)| terminal).collect();
        for terminal in all {
            terminal.pty.kill();
        }
    }

    type Spawned = (Box<dyn Pty>, Box<dyn Read + Send>, Box<dyn FnOnce() -> Option<i32> + Send>);

    #[cfg(unix)]
    fn spawn(program: &str, args: &[String], cwd: &Path, cols: u16, rows: u16) -> Result<Spawned, String> {
        use std::os::fd::{FromRawFd, OwnedFd};
        use std::os::unix::process::CommandExt;

        static PTSNAME: Mutex<()> = Mutex::new(());
        let failed = |what: &str| format!("开不了伪终端（{what}）：{}", std::io::Error::last_os_error());
        let master = unsafe { libc::posix_openpt(libc::O_RDWR | libc::O_NOCTTY) };
        if master < 0 {
            return Err(failed("posix_openpt"));
        }
        let master = unsafe { OwnedFd::from_raw_fd(master) };
        use std::os::fd::AsRawFd;
        let fd = master.as_raw_fd();
        if unsafe { libc::grantpt(fd) } != 0 || unsafe { libc::unlockpt(fd) } != 0 {
            return Err(failed("grantpt"));
        }
        let name = {
            let _guard = PTSNAME.lock().unwrap();
            let name = unsafe { libc::ptsname(fd) };
            if name.is_null() {
                return Err(failed("ptsname"));
            }
            unsafe { std::ffi::CStr::from_ptr(name) }.to_owned()
        };
        let slave = unsafe { libc::open(name.as_ptr(), libc::O_RDWR | libc::O_NOCTTY) };
        if slave < 0 {
            return Err(failed("open"));
        }
        let slave = unsafe { OwnedFd::from_raw_fd(slave) };
        set_size(fd, cols, rows);
        let mut command = std::process::Command::new(program);
        command
            .args(args)
            .current_dir(cwd)
            .env("TERM", "xterm-256color")
            .env("COLORTERM", "truecolor")
            .stdin(slave.try_clone().map_err(|error| error.to_string())?)
            .stdout(slave.try_clone().map_err(|error| error.to_string())?)
            .stderr(slave.try_clone().map_err(|error| error.to_string())?);
        unsafe {
            command.pre_exec(|| {
                // 自己一个会话、这个伪终端当控制终端（Ctrl+C、作业控制才对）
                if libc::setsid() < 0 || libc::ioctl(0, libc::TIOCSCTTY as _, 0) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = command.spawn().map_err(|error| format!("起不了 {program}：{error}"))?;
        drop(slave);
        let pid = child.id() as libc::pid_t;
        let reader = std::fs::File::from(master.try_clone().map_err(|error| error.to_string())?);
        struct Unix {
            master: Mutex<std::fs::File>,
            pid: libc::pid_t,
        }
        impl Pty for Unix {
            fn write(&self, data: &[u8]) -> std::io::Result<()> {
                let mut master = self.master.lock().unwrap();
                master.write_all(data).and_then(|_| master.flush())
            }
            fn resize(&self, cols: u16, rows: u16) {
                use std::os::fd::AsRawFd;
                set_size(self.master.lock().unwrap().as_raw_fd(), cols, rows);
            }
            fn kill(&self) {
                // 整个进程组：shell 和它起的
                unsafe {
                    libc::kill(-self.pid, libc::SIGHUP);
                    libc::kill(-self.pid, libc::SIGTERM);
                }
                let pid = self.pid;
                std::thread::spawn(move || {
                    std::thread::sleep(std::time::Duration::from_secs(2));
                    unsafe { libc::kill(-pid, libc::SIGKILL) };
                });
            }
        }
        let pty = Unix { master: Mutex::new(std::fs::File::from(master)), pid };
        let waiter = Box::new(move || child.wait().ok().and_then(|status| status.code()));
        Ok((Box::new(pty), Box::new(reader), waiter))
    }

    #[cfg(unix)]
    fn set_size(fd: std::os::fd::RawFd, cols: u16, rows: u16) {
        let size = libc::winsize { ws_row: rows, ws_col: cols, ws_xpixel: 0, ws_ypixel: 0 };
        unsafe { libc::ioctl(fd, libc::TIOCSWINSZ as _, &size) };
    }

    #[cfg(windows)]
    #[allow(non_snake_case, clippy::upper_case_acronyms)]
    mod conpty {
        use std::ffi::c_void;

        pub type HANDLE = *mut c_void;
        pub type HPCON = *mut c_void;

        #[repr(C)]
        #[derive(Clone, Copy)]
        pub struct COORD {
            pub X: i16,
            pub Y: i16,
        }

        #[repr(C)]
        pub struct STARTUPINFOW {
            pub cb: u32,
            pub lpReserved: *mut u16,
            pub lpDesktop: *mut u16,
            pub lpTitle: *mut u16,
            pub dwX: u32,
            pub dwY: u32,
            pub dwXSize: u32,
            pub dwYSize: u32,
            pub dwXCountChars: u32,
            pub dwYCountChars: u32,
            pub dwFillAttribute: u32,
            pub dwFlags: u32,
            pub wShowWindow: u16,
            pub cbReserved2: u16,
            pub lpReserved2: *mut u8,
            pub hStdInput: HANDLE,
            pub hStdOutput: HANDLE,
            pub hStdError: HANDLE,
        }

        #[repr(C)]
        pub struct STARTUPINFOEXW {
            pub StartupInfo: STARTUPINFOW,
            pub lpAttributeList: *mut c_void,
        }

        #[repr(C)]
        pub struct PROCESS_INFORMATION {
            pub hProcess: HANDLE,
            pub hThread: HANDLE,
            pub dwProcessId: u32,
            pub dwThreadId: u32,
        }

        pub const PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE: usize = 0x0002_0016;
        pub const EXTENDED_STARTUPINFO_PRESENT: u32 = 0x0008_0000;
        pub const CREATE_UNICODE_ENVIRONMENT: u32 = 0x0000_0400;
        pub const STARTF_USESTDHANDLES: u32 = 0x0000_0100;
        pub const INFINITE: u32 = 0xFFFF_FFFF;

        #[link(name = "kernel32")]
        unsafe extern "system" {
            pub fn CreatePipe(read: *mut HANDLE, write: *mut HANDLE, attributes: *const c_void, size: u32) -> i32;
            pub fn CreatePseudoConsole(size: COORD, input: HANDLE, output: HANDLE, flags: u32, console: *mut HPCON) -> i32;
            pub fn ResizePseudoConsole(console: HPCON, size: COORD) -> i32;
            pub fn ClosePseudoConsole(console: HPCON);
            pub fn InitializeProcThreadAttributeList(list: *mut c_void, count: u32, flags: u32, size: *mut usize) -> i32;
            pub fn UpdateProcThreadAttribute(
                list: *mut c_void,
                flags: u32,
                attribute: usize,
                value: *const c_void,
                size: usize,
                previous: *mut c_void,
                returned: *mut usize,
            ) -> i32;
            pub fn DeleteProcThreadAttributeList(list: *mut c_void);
            pub fn CreateProcessW(
                application: *const u16,
                command_line: *mut u16,
                process_attributes: *const c_void,
                thread_attributes: *const c_void,
                inherit_handles: i32,
                flags: u32,
                environment: *const c_void,
                directory: *const u16,
                startup: *mut STARTUPINFOEXW,
                information: *mut PROCESS_INFORMATION,
            ) -> i32;
            pub fn CloseHandle(handle: HANDLE) -> i32;
            pub fn WaitForSingleObject(handle: HANDLE, milliseconds: u32) -> u32;
            pub fn GetExitCodeProcess(handle: HANDLE, code: *mut u32) -> i32;
            pub fn TerminateProcess(handle: HANDLE, code: u32) -> i32;
        }
    }

    #[cfg(windows)]
    fn spawn(program: &str, args: &[String], cwd: &Path, cols: u16, rows: u16) -> Result<Spawned, String> {
        use conpty::*;
        use std::os::windows::ffi::OsStrExt;
        use std::os::windows::io::FromRawHandle;
        use std::ptr::{null, null_mut};

        let failed = |what: &str| format!("开不了伪终端（{what}）：{}", std::io::Error::last_os_error());
        let wide = |text: &std::ffi::OsStr| text.encode_wide().chain(std::iter::once(0)).collect::<Vec<u16>>();
        let (mut input_read, mut input_write, mut output_read, mut output_write): (HANDLE, HANDLE, HANDLE, HANDLE) =
            (null_mut(), null_mut(), null_mut(), null_mut());
        unsafe {
            if CreatePipe(&mut input_read, &mut input_write, null(), 0) == 0
                || CreatePipe(&mut output_read, &mut output_write, null(), 0) == 0
            {
                return Err(failed("CreatePipe"));
            }
            let mut console: HPCON = null_mut();
            let size = COORD { X: cols as i16, Y: rows as i16 };
            let result = CreatePseudoConsole(size, input_read, output_write, 0, &mut console);
            if result < 0 {
                return Err(format!("开不了伪终端（CreatePseudoConsole 0x{result:08x}，要 Windows 10 1809 以上）"));
            }
            // 伪终端那头的句柄交给它了，这边关掉（不然读不到结束）
            CloseHandle(input_read);
            CloseHandle(output_write);
            let mut list_size = 0usize;
            InitializeProcThreadAttributeList(null_mut(), 1, 0, &mut list_size);
            let mut list = vec![0u8; list_size];
            let list_pointer = list.as_mut_ptr() as *mut std::ffi::c_void;
            if InitializeProcThreadAttributeList(list_pointer, 1, 0, &mut list_size) == 0
                || UpdateProcThreadAttribute(
                    list_pointer,
                    0,
                    PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE,
                    console as *const std::ffi::c_void,
                    std::mem::size_of::<HPCON>(),
                    null_mut(),
                    null_mut(),
                ) == 0
            {
                ClosePseudoConsole(console);
                return Err(failed("UpdateProcThreadAttribute"));
            }
            let mut startup: STARTUPINFOEXW = std::mem::zeroed();
            startup.StartupInfo.cb = std::mem::size_of::<STARTUPINFOEXW>() as u32;
            // 标准输入输出不继承这个进程的（大脑没有控制台），全走伪终端
            startup.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
            startup.lpAttributeList = list_pointer;
            let mut line = std::ffi::OsString::from(format!("\"{program}\""));
            for arg in args {
                line.push(" ");
                line.push(arg);
            }
            let mut line = wide(&line);
            let directory = wide(cwd.as_os_str());
            let mut information: PROCESS_INFORMATION = std::mem::zeroed();
            let created = CreateProcessW(
                null(),
                line.as_mut_ptr(),
                null(),
                null(),
                0,
                EXTENDED_STARTUPINFO_PRESENT | CREATE_UNICODE_ENVIRONMENT,
                null(),
                directory.as_ptr(),
                &mut startup,
                &mut information,
            );
            DeleteProcThreadAttributeList(list_pointer);
            if created == 0 {
                ClosePseudoConsole(console);
                return Err(failed("CreateProcessW"));
            }
            CloseHandle(information.hThread);
            struct Windows {
                input: Mutex<std::fs::File>,
                console: usize,
                process: usize,
            }
            // 句柄只在这个结构里用，几个线程经锁、按值传
            unsafe impl Send for Windows {}
            unsafe impl Sync for Windows {}
            impl Pty for Windows {
                fn write(&self, data: &[u8]) -> std::io::Result<()> {
                    let mut input = self.input.lock().unwrap();
                    input.write_all(data).and_then(|_| input.flush())
                }
                fn resize(&self, cols: u16, rows: u16) {
                    unsafe { ResizePseudoConsole(self.console as HPCON, COORD { X: cols as i16, Y: rows as i16 }) };
                }
                fn kill(&self) {
                    unsafe {
                        TerminateProcess(self.process as HANDLE, 1);
                        ClosePseudoConsole(self.console as HPCON);
                    }
                }
            }
            let process = information.hProcess as usize;
            let pty = Windows { input: Mutex::new(std::fs::File::from_raw_handle(input_write)), console: console as usize, process };
            let reader = std::fs::File::from_raw_handle(output_read);
            let waiter = Box::new(move || {
                let handle = process as HANDLE;
                WaitForSingleObject(handle, INFINITE);
                let mut code = 0u32;
                let known = GetExitCodeProcess(handle, &mut code) != 0;
                CloseHandle(handle);
                known.then_some(code as i32)
            });
            Ok((Box::new(pty), Box::new(reader), waiter))
        }
    }

    #[cfg(all(test, unix))]
    mod tests {
        use super::*;

        #[test]
        fn runs_a_shell_and_replays() {
            let id = format!("test-{}", std::process::id());
            let dir = std::env::temp_dir();
            open(&id, "s1", &dir, 80, 24, Some("echo edith-$((40+2))"), "nobody").unwrap();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            let replay = loop {
                let replay = attach(&id, "nobody").unwrap();
                let data = {
                    use base64::Engine;
                    base64::engine::general_purpose::STANDARD.decode(replay["data"].as_str().unwrap()).unwrap()
                };
                if String::from_utf8_lossy(&data).contains("edith-42") || std::time::Instant::now() > deadline {
                    break String::from_utf8_lossy(&data).into_owned();
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
            };
            assert!(replay.contains("edith-42"), "{replay}");
            resize(&id, 100, 30).unwrap();
            assert_eq!(list().iter().find(|item| item["id"] == json!(id)).unwrap()["cols"], 100);
            close(&id).unwrap();
            assert!(list().iter().all(|item| item["id"] != json!(id)));
        }
    }
}

// MARK: - 内置浏览器（无头 Chrome，经 DevTools 协议）

mod browser {
    //! 内置浏览器（Friday 的 browser.*，Friday 仓库的 qt/docs/BROWSER.md §9；照 Friday Qt 版没有界面时用的 CdpBrowser）：常驻的那份起这台装着的
    //! Chrome / Chromium / Edge 的无头模式，经 DevTools 协议开标签、推画面（Page.startScreencast）、转鼠标键盘。标签在大脑里：
    //! 客户端断了照常在，接上来接着看；大脑重起后按记下的网址重新打开（`<项目数据目录>/browser-tabs.json`）。
    //! Linux / macOS 走 `--remote-debugging-pipe`（fd 3 进、fd 4 出，一条一条以 \0 分隔，不开端口）；Windows 走本机随机端口 + WebSocket。
    //! 看着一个标签的连接（browser.watch）收它的 `browser.frame`；标签开了、关了、标题网址加载状态、对话框变了推 `browser`（全部标签）给全权的连接。

    use std::collections::{HashMap, HashSet};
    #[cfg(unix)]
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use std::process::Child;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::mpsc::{self, Sender};
    use std::sync::{LazyLock, Mutex};
    use std::time::Duration;

    use serde_json::{Value, json};

    const DEFAULT_VIEWPORT: (u32, u32) = (1280, 800);
    /// 等浏览器回话最多多久
    const CALL_TIMEOUT: Duration = Duration::from_secs(30);

    #[derive(Clone, Debug, Default)]
    struct Tab {
        id: String,
        session: String,
        target: String,
        /// CDP 的会话（attachToTarget 回的）：空 = 还没接上
        cdp: String,
        frame: String,
        url: String,
        title: String,
        loading: bool,
        navigating: bool,
        /// 接上了要打开的
        pending: Option<String>,
        container: (u32, u32),
        fixed: Option<(u32, u32)>,
        dialog: Option<Value>,
        /// 看着它的连接 → 画面最大多大
        watchers: HashMap<String, (u32, u32)>,
        /// 智能体用的隔离世界（edith）的执行上下文：0 = 还没有 / 换页了
        world: u64,
        /// 智能体请用户在这里做的事（browser_hand_over）
        hand_over: Option<String>,
        /// 下载完、还没告诉智能体的文件
        downloads: Vec<String>,
        /// 用户最近一次在卡片里动（智能体让开）
        user_input: Option<std::time::Instant>,
        /// 哪个标签开出来的（网页开的新窗口）
        opener: String,
    }

    impl Tab {
        fn viewport(&self) -> (u32, u32) {
            self.fixed.unwrap_or(if self.container.0 >= 10 && self.container.1 >= 10 { self.container } else { DEFAULT_VIEWPORT })
        }

        /// 画面最大多大：看着的里面最大的
        fn frame_size(&self) -> Option<(u32, u32)> {
            self.watchers.values().copied().reduce(|a, b| (a.0.max(b.0), a.1.max(b.1)))
        }
    }

    type Reply = Sender<Result<Value, String>>;

    /// 往浏览器写的那一头
    enum Writer {
        #[cfg(unix)]
        Pipe(std::io::PipeWriter),
        #[cfg(windows)]
        Socket(socket::Socket),
    }

    impl Writer {
        fn send(&mut self, text: &str) -> std::io::Result<()> {
            match self {
                #[cfg(unix)]
                Writer::Pipe(pipe) => {
                    pipe.write_all(text.as_bytes())?;
                    pipe.write_all(&[0])?;
                    pipe.flush()
                }
                #[cfg(windows)]
                Writer::Socket(socket) => socket.send_text(text),
            }
        }
    }

    #[derive(Default)]
    struct Inner {
        tabs: Vec<Tab>,
        /// 会话 → 选中的标签
        current: HashMap<String, String>,
        loaded: bool,
        start_error: String,
        downloads: HashMap<String, (String, String)>,
    }

    struct Browser {
        inner: Mutex<Inner>,
        writer: Mutex<Option<Writer>>,
        process: Mutex<Option<Child>>,
        /// 一次只起一个
        starting: Mutex<()>,
        next: AtomicU64,
        pending: Mutex<HashMap<u64, Reply>>,
        dirty: AtomicBool,
        flusher: AtomicBool,
    }

    static BROWSER: LazyLock<Browser> = LazyLock::new(|| Browser {
        inner: Mutex::new(Inner::default()),
        writer: Mutex::new(None),
        process: Mutex::new(None),
        starting: Mutex::new(()),
        next: AtomicU64::new(1),
        pending: Mutex::new(HashMap::new()),
        dirty: AtomicBool::new(false),
        flusher: AtomicBool::new(false),
    });

    // MARK: - 找浏览器、起浏览器

    /// 这台装着的 Chrome / Chromium / Edge（EDITH_BROWSER 指定的优先）
    fn executable() -> Option<PathBuf> {
        if let Some(path) = std::env::var_os("EDITH_BROWSER").filter(|path| !path.is_empty()).map(PathBuf::from) {
            return path.is_file().then_some(path);
        }
        let mut candidates: Vec<PathBuf> = Vec::new();
        if cfg!(windows) {
            for base in ["ProgramFiles", "ProgramFiles(x86)", "LOCALAPPDATA"].iter().filter_map(std::env::var_os) {
                let base = PathBuf::from(base);
                candidates.push(base.join("Google/Chrome/Application/chrome.exe"));
                candidates.push(base.join("Microsoft/Edge/Application/msedge.exe"));
                candidates.push(base.join("Chromium/Application/chrome.exe"));
            }
        } else if cfg!(target_os = "macos") {
            for app in [
                "Google Chrome.app/Contents/MacOS/Google Chrome",
                "Chromium.app/Contents/MacOS/Chromium",
                "Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
            ] {
                candidates.push(Path::new("/Applications").join(app));
                candidates.push(crate::user_home().join("Applications").join(app));
            }
        } else {
            for name in ["google-chrome", "google-chrome-stable", "chromium", "chromium-browser", "microsoft-edge", "microsoft-edge-stable"]
            {
                if let Some(found) = find_in_path(name) {
                    candidates.push(found);
                }
            }
            candidates.push(PathBuf::from("/snap/bin/chromium"));
        }
        candidates.into_iter().find(|path| path.is_file())
    }

    fn unavailable_reason() -> String {
        if cfg!(windows) {
            "这台机器上没有 Chrome 或 Edge：装一个后再试".into()
        } else if cfg!(target_os = "macos") {
            "这台 Mac 上没有 Chrome、Chromium 或 Edge：装一个后再试".into()
        } else {
            "这台机器上没有 Chrome / Chromium：装一个后再试（Ubuntu / Debian：sudo apt install chromium-browser 或 chromium）".into()
        }
    }

    fn find_in_path(name: &str) -> Option<PathBuf> {
        std::env::split_paths(&std::env::var_os("PATH")?).map(|dir| dir.join(name)).find(|path| path.is_file())
    }

    fn read_json(path: &Path) -> Option<Value> {
        serde_json::from_slice(&std::fs::read(path).ok()?).ok()
    }

    fn write_json(path: &Path, value: &Value) -> std::io::Result<()> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        std::fs::write(path, serde_json::to_string_pretty(value).unwrap_or_default())
    }

    fn data_dir() -> PathBuf {
        crate::serve_dir().join("browser")
    }

    fn downloads_dir() -> PathBuf {
        crate::edith_home().join("downloads")
    }

    fn running() -> bool {
        BROWSER.writer.lock().unwrap().is_some()
    }

    /// 起浏览器（起着的不再起）
    fn ensure_started() -> Result<(), String> {
        let _once = BROWSER.starting.lock().unwrap();
        if running() {
            return Ok(());
        }
        let program = executable().ok_or_else(unavailable_reason)?;
        let profile = data_dir().join("profile");
        std::fs::create_dir_all(&profile).map_err(|error| format!("建不了 {}：{error}", profile.display()))?;
        let _ = std::fs::create_dir_all(downloads_dir());
        let mut args: Vec<String> = [
            "--headless=new",
            "--no-first-run",
            "--no-default-browser-check",
            "--mute-audio",
            "--hide-scrollbars",
            "--window-size=1280,800",
            "--lang=zh-CN",
        ]
        .iter()
        .map(|arg| arg.to_string())
        .collect();
        args.push(format!("--user-data-dir={}", profile.display()));
        if cfg!(target_os = "linux") {
            // 容器里 /dev/shm 很小；root、AppArmor 限制了非特权用户命名空间时沙箱起不来
            args.push("--disable-dev-shm-usage".into());
            let flag = |path: &str| std::fs::read_to_string(path).map(|text| text.trim().to_string()).unwrap_or_default();
            #[cfg(unix)]
            let root = unsafe { libc::geteuid() } == 0;
            #[cfg(not(unix))]
            let root = false;
            if root
                || flag("/proc/sys/kernel/apparmor_restrict_unprivileged_userns") == "1"
                || flag("/proc/sys/kernel/unprivileged_userns_clone") == "0"
            {
                args.push("--no-sandbox".into());
            }
        }
        let started = start_transport(&program, args, &profile);
        let mut inner = BROWSER.inner.lock().unwrap();
        match started {
            Ok(()) => {
                inner.start_error.clear();
                drop(inner);
                post("Target.setDiscoverTargets", json!({ "discover": true }), "");
                post(
                    "Browser.setDownloadBehavior",
                    json!({ "behavior": "allow", "downloadPath": downloads_dir().to_string_lossy(), "eventsEnabled": true }),
                    "",
                );
                Ok(())
            }
            Err(error) => {
                inner.start_error = error.clone();
                Err(error)
            }
        }
    }

    #[cfg(unix)]
    fn start_transport(program: &Path, mut args: Vec<String>, _profile: &Path) -> Result<(), String> {
        use std::os::fd::AsRawFd;
        use std::os::unix::process::CommandExt;
        args.push("--remote-debugging-pipe".into());
        args.push("about:blank".into());
        let (child_read, to_browser) = std::io::pipe().map_err(|error| format!("建不了管道：{error}"))?;
        let (from_browser, child_write) = std::io::pipe().map_err(|error| format!("建不了管道：{error}"))?;
        let (read_fd, write_fd) = (child_read.as_raw_fd(), child_write.as_raw_fd());
        let mut command = std::process::Command::new(program);
        command.args(&args).stdin(std::process::Stdio::null()).stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null());
        unsafe {
            // 子进程里把两头接到 fd 3、4（dup2 出来的不带 CLOEXEC）
            command.pre_exec(move || {
                if libc::dup2(read_fd, 3) < 0 || libc::dup2(write_fd, 4) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let child = command.spawn().map_err(|error| format!("起不来浏览器（{}）：{error}", program.display()))?;
        drop(child_read);
        drop(child_write);
        *BROWSER.process.lock().unwrap() = Some(child);
        *BROWSER.writer.lock().unwrap() = Some(Writer::Pipe(to_browser));
        std::thread::spawn(move || {
            let mut reader = std::io::BufReader::new(from_browser);
            let mut message = Vec::new();
            while std::io::BufRead::read_until(&mut reader, 0, &mut message).is_ok_and(|count| count > 0) {
                if message.last() == Some(&0) {
                    message.pop();
                }
                if let Ok(value) = serde_json::from_slice::<Value>(&message) {
                    on_message(value);
                }
                message.clear();
            }
            closed();
        });
        Ok(())
    }

    #[cfg(windows)]
    fn start_transport(program: &Path, mut args: Vec<String>, profile: &Path) -> Result<(), String> {
        let port_file = profile.join("DevToolsActivePort");
        let _ = std::fs::remove_file(&port_file);
        args.push("--remote-debugging-port=0".into());
        args.push("--remote-debugging-address=127.0.0.1".into());
        args.push("about:blank".into());
        let child = std::process::Command::new(program)
            .args(&args)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .map_err(|error| format!("起不来浏览器（{}）：{error}", program.display()))?;
        *BROWSER.process.lock().unwrap() = Some(child);
        // 等端口文件出来（最多 15 秒）再连
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        let lines = loop {
            let lines: Vec<String> = std::fs::read_to_string(&port_file)
                .unwrap_or_default()
                .lines()
                .map(str::trim)
                .filter(|line| !line.is_empty())
                .map(str::to_string)
                .collect();
            if lines.len() >= 2 {
                break lines;
            }
            if std::time::Instant::now() > deadline {
                stop();
                return Err("浏览器起来了但连不上（没有 DevToolsActivePort）".into());
            }
            std::thread::sleep(Duration::from_millis(50));
        };
        let (writer, mut reader) = socket::connect(&format!("127.0.0.1:{}", lines[0]), &lines[1])?;
        *BROWSER.writer.lock().unwrap() = Some(Writer::Socket(writer));
        std::thread::spawn(move || {
            while let Some(text) = reader.next_text() {
                if let Ok(value) = serde_json::from_str::<Value>(&text) {
                    on_message(value);
                }
            }
            closed();
        });
        Ok(())
    }

    /// 浏览器退出了：标签都当没接上（下次用时重起、按网址重新打开），等着的回话都失败
    fn closed() {
        *BROWSER.writer.lock().unwrap() = None;
        if let Some(mut child) = BROWSER.process.lock().unwrap().take() {
            let _ = child.wait();
        }
        {
            let mut inner = BROWSER.inner.lock().unwrap();
            for tab in inner.tabs.iter_mut() {
                tab.cdp.clear();
                tab.target.clear();
                if tab.pending.is_none() && !tab.url.is_empty() {
                    tab.pending = Some(tab.url.clone());
                }
            }
        }
        let pending: Vec<Reply> = BROWSER.pending.lock().unwrap().drain().map(|(_, reply)| reply).collect();
        for reply in pending {
            let _ = reply.send(Err("浏览器退出了".into()));
        }
        changed();
    }

    /// 大脑退出时：关掉浏览器（Browser.close 让它自己退，Cookie、登录才写到硬盘）
    pub fn stop() {
        if running() {
            post("Browser.close", json!({}), "");
            let deadline = std::time::Instant::now() + Duration::from_secs(3);
            while running() && std::time::Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(20));
            }
        }
        if let Some(mut child) = BROWSER.process.lock().unwrap().take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        *BROWSER.writer.lock().unwrap() = None;
    }

    // MARK: - 收发

    fn send_raw(method: &str, params: Value, cdp: &str, id: u64) -> bool {
        let mut message = json!({ "id": id, "method": method, "params": params });
        if !cdp.is_empty() {
            message["sessionId"] = json!(cdp);
        }
        let mut writer = BROWSER.writer.lock().unwrap();
        let Some(pipe) = writer.as_mut() else { return false };
        pipe.send(&message.to_string()).is_ok()
    }

    /// 发了不等回话
    fn post(method: &str, params: Value, cdp: &str) {
        let id = BROWSER.next.fetch_add(1, Ordering::Relaxed);
        send_raw(method, params, cdp, id);
    }

    /// 发了等回话（别在读的那个线程里调）
    fn call(method: &str, params: Value, cdp: &str) -> Result<Value, String> {
        let id = BROWSER.next.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = mpsc::channel();
        BROWSER.pending.lock().unwrap().insert(id, sender);
        if !send_raw(method, params, cdp, id) {
            BROWSER.pending.lock().unwrap().remove(&id);
            return Err("浏览器没在跑".into());
        }
        match receiver.recv_timeout(CALL_TIMEOUT) {
            Ok(outcome) => outcome,
            Err(_) => {
                BROWSER.pending.lock().unwrap().remove(&id);
                Err(format!("{method} 等了 {} 秒没回话", CALL_TIMEOUT.as_secs()))
            }
        }
    }

    fn on_message(message: Value) {
        if let Some(id) = message.get("id").and_then(Value::as_u64) {
            if let Some(reply) = BROWSER.pending.lock().unwrap().remove(&id) {
                let outcome = match message.get("error") {
                    Some(error) => Err(error["message"].as_str().unwrap_or("浏览器回了错误").to_string()),
                    None => Ok(message.get("result").cloned().unwrap_or(Value::Null)),
                };
                let _ = reply.send(outcome);
            }
            return;
        }
        let method = message["method"].as_str().unwrap_or("");
        let params = &message["params"];
        let cdp = message["sessionId"].as_str().unwrap_or("");
        on_event(method, params, cdp);
    }

    /// 网页没有标题时 Chrome 报网址（去掉协议）：当作没有
    fn display_title(title: &str, url: &str) -> String {
        let title = title.trim();
        let bare = url.split_once("://").map_or(url, |(_, rest)| rest);
        if title.is_empty() || title == url || title == bare { String::new() } else { title.to_string() }
    }

    fn on_event(method: &str, params: &Value, cdp: &str) {
        let text = |value: &Value| value.as_str().unwrap_or("").to_string();
        let mut inner = BROWSER.inner.lock().unwrap();
        match method {
            "Target.targetCreated" => {
                // 网页开的新窗口：同一段会话的新标签
                let info = &params["targetInfo"];
                let opener = text(&info["openerId"]);
                let target = text(&info["targetId"]);
                if info["type"] != "page" || opener.is_empty() || inner.tabs.iter().any(|tab| tab.target == target) {
                    return;
                }
                let Some(parent) = inner.tabs.iter().find(|tab| tab.target == opener).cloned() else { return };
                let tab = Tab {
                    id: uuid::Uuid::new_v4().to_string(),
                    session: parent.session.clone(),
                    target,
                    url: text(&info["url"]),
                    container: parent.container,
                    opener: parent.id.clone(),
                    ..Default::default()
                };
                let id = tab.id.clone();
                inner.current.insert(tab.session.clone(), id.clone());
                inner.tabs.push(tab);
                drop(inner);
                std::thread::spawn(move || {
                    let _ = attach(&id);
                });
                save();
                changed();
                return;
            }
            "Target.targetInfoChanged" => {
                let info = &params["targetInfo"];
                let target = text(&info["targetId"]);
                if let Some(tab) = inner.tabs.iter_mut().find(|tab| tab.target == target) {
                    let url = text(&info["url"]);
                    let title = display_title(info["title"].as_str().unwrap_or(""), &url);
                    if url != tab.url || title != tab.title {
                        tab.url = url;
                        tab.title = title;
                        drop(inner);
                        changed();
                        save();
                    }
                }
                return;
            }
            "Target.targetDestroyed" => {
                let target = text(&params["targetId"]);
                if let Some(index) = inner.tabs.iter().position(|tab| tab.target == target) {
                    let tab = inner.tabs.remove(index);
                    if inner.current.get(&tab.session) == Some(&tab.id) {
                        inner.current.remove(&tab.session);
                    }
                    drop(inner);
                    changed();
                    save();
                }
                return;
            }
            "Target.detachedFromTarget" => {
                let session = text(&params["sessionId"]);
                if let Some(tab) = inner.tabs.iter_mut().find(|tab| tab.cdp == session) {
                    tab.cdp.clear();
                }
                return;
            }
            "Browser.downloadWillBegin" | "Browser.downloadProgress" => {
                let guid = text(&params["guid"]);
                let (tab, name) = if method == "Browser.downloadWillBegin" {
                    let frame = text(&params["frameId"]);
                    let Some(owner) = inner.tabs.iter().find(|tab| tab.frame == frame).map(|tab| tab.id.clone()) else { return };
                    let name = text(&params["suggestedFilename"]);
                    inner.downloads.insert(guid.clone(), (owner.clone(), name.clone()));
                    (owner, name)
                } else {
                    let Some(known) = inner.downloads.get(&guid).cloned() else { return };
                    known
                };
                let state = match params["state"].as_str() {
                    Some("completed") => "done",
                    Some("canceled") => "failed",
                    _ => "progress",
                };
                if state != "progress" {
                    inner.downloads.remove(&guid);
                }
                if state == "done"
                    && let Some(owner) = inner.tabs.iter_mut().find(|item| item.id == tab)
                {
                    owner.downloads.push(downloads_dir().join(&name).to_string_lossy().into_owned());
                }
                let total = params["totalBytes"].as_f64().unwrap_or(0.0);
                let fraction = if total > 0.0 { params["receivedBytes"].as_f64().unwrap_or(0.0) / total } else { 0.0 };
                let path = downloads_dir().join(&name);
                drop(inner);
                crate::notify_all_clients(
                    "browser.download",
                    json!({ "tab": tab, "name": name, "path": path.to_string_lossy(), "state": state, "fraction": fraction }),
                );
                return;
            }
            _ => {}
        }
        let Some(tab) = inner.tabs.iter_mut().find(|tab| !cdp.is_empty() && tab.cdp == cdp) else { return };
        match method {
            "Page.frameNavigated" => {
                let frame = &params["frame"];
                if frame["parentId"].as_str().unwrap_or("").is_empty() {
                    tab.frame = text(&frame["id"]);
                    tab.url = text(&frame["url"]);
                    let cdp = tab.cdp.clone();
                    drop(inner);
                    // 后退 / 前进从缓存拿回来的页面不触发加载事件：标题自己读一次
                    refresh_title(&cdp);
                    changed();
                }
            }
            "Page.frameStartedLoading" | "Page.frameStoppedLoading" => {
                if text(&params["frameId"]) == tab.frame {
                    tab.loading = method == "Page.frameStartedLoading";
                    drop(inner);
                    changed();
                }
            }
            "Page.loadEventFired" | "Page.domContentEventFired" => {
                let cdp = tab.cdp.clone();
                drop(inner);
                refresh_title(&cdp);
            }
            "Runtime.executionContextCreated" => {
                let context = &params["context"];
                if context["name"] == "edith" && context["auxData"]["frameId"].as_str() == Some(tab.frame.as_str()) {
                    tab.world = context["id"].as_u64().unwrap_or(0);
                }
            }
            "Runtime.executionContextsCleared" => tab.world = 0,
            "Runtime.bindingCalled" => {
                if params["name"] == "__edithTitle" {
                    let title = display_title(params["payload"].as_str().unwrap_or(""), &tab.url);
                    if title != tab.title {
                        tab.title = title;
                        drop(inner);
                        changed();
                        save();
                    }
                }
            }
            "Page.javascriptDialogOpening" => {
                tab.dialog = Some(json!({ "type": params["type"], "message": params["message"], "defaultText": params["defaultPrompt"] }));
                drop(inner);
                changed();
            }
            "Page.javascriptDialogClosed" => {
                tab.dialog = None;
                drop(inner);
                changed();
            }
            "Page.screencastFrame" => {
                let (id, viewport, watchers) = (tab.id.clone(), tab.viewport(), tab.watchers.keys().cloned().collect::<Vec<_>>());
                let cdp = tab.cdp.clone();
                drop(inner);
                post("Page.screencastFrameAck", json!({ "sessionId": params["sessionId"] }), &cdp);
                if !watchers.is_empty() {
                    crate::notify_clients(
                        &watchers,
                        "browser.frame",
                        json!({ "tab": id, "data": params["data"], "viewport": [viewport.0, viewport.1] }),
                    );
                }
            }
            _ => {}
        }
    }

    /// 读一次标题、网址（targetInfoChanged 有时不报）
    fn refresh_title(cdp: &str) {
        let cdp = cdp.to_string();
        std::thread::spawn(move || {
            let Ok(result) =
                call("Runtime.evaluate", json!({ "expression": "[document.title, location.href]", "returnByValue": true }), &cdp)
            else {
                return;
            };
            let value = &result["result"]["value"];
            let url = value[1].as_str().unwrap_or("").to_string();
            let title = display_title(value[0].as_str().unwrap_or(""), &url);
            let mut inner = BROWSER.inner.lock().unwrap();
            if let Some(tab) = inner.tabs.iter_mut().find(|tab| tab.cdp == cdp)
                && (tab.title != title || (!url.is_empty() && tab.url != url))
            {
                tab.title = title;
                if !url.is_empty() {
                    tab.url = url;
                }
                drop(inner);
                changed();
                save();
            }
        });
    }

    /// 网页标题变了（网页用脚本改的）：在 edith 这个隔离世界里盯着 document.title，经绑定报给这边
    const TITLE_WATCH: &str = r#"(() => {
  let last = null;
  const send = () => { const title = document.title; if (title === last) return; last = title; try { __edithTitle(title); } catch (e) {} };
  const watch = () => { send(); new MutationObserver(send).observe(document.documentElement || document, { subtree: true, childList: true, characterData: true }); };
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', watch, { once: true }); else watch();
})()"#;

    // MARK: - 标签

    fn with_tab<T>(id: &str, change: impl FnOnce(&mut Tab) -> T) -> Result<T, String> {
        let mut inner = BROWSER.inner.lock().unwrap();
        load_tabs(&mut inner);
        inner.tabs.iter_mut().find(|tab| tab.id == id).map(change).ok_or_else(|| format!("没有标签 {id}"))
    }

    /// 接上一个标签（没开的先开；接上了打开等着的网址、要画面的开始推）
    /// 正在接上的标签：别的线程同时要接同一个的等它接完（不然建出两个页面）
    static ATTACHING: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(|| Mutex::new(HashSet::new()));

    fn attach(id: &str) -> Result<(), String> {
        let deadline = std::time::Instant::now() + CALL_TIMEOUT;
        loop {
            if with_tab(id, |tab| !tab.cdp.is_empty())? {
                return Ok(());
            }
            if ATTACHING.lock().unwrap().insert(id.to_string()) {
                break;
            }
            if std::time::Instant::now() > deadline {
                return Err("接不上这个标签".into());
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        let outcome = attach_once(id);
        ATTACHING.lock().unwrap().remove(id);
        outcome
    }

    fn attach_once(id: &str) -> Result<(), String> {
        ensure_started()?;
        let (target, cdp) = with_tab(id, |tab| (tab.target.clone(), tab.cdp.clone()))?;
        if !cdp.is_empty() {
            return Ok(());
        }
        let target = if target.is_empty() {
            call("Target.createTarget", json!({ "url": "about:blank" }), "")?["targetId"].as_str().unwrap_or("").to_string()
        } else {
            target
        };
        let cdp = call("Target.attachToTarget", json!({ "targetId": target, "flatten": true }), "")?["sessionId"]
            .as_str()
            .unwrap_or("")
            .to_string();
        with_tab(id, |tab| {
            tab.target = target.clone();
            tab.cdp = cdp.clone();
        })?;
        post("Page.enable", json!({}), &cdp);
        post("Runtime.enable", json!({}), &cdp);
        post("Runtime.addBinding", json!({ "name": "__edithTitle", "executionContextName": "edith" }), &cdp);
        post("Page.addScriptToEvaluateOnNewDocument", json!({ "source": TITLE_WATCH, "worldName": "edith" }), &cdp);
        apply_viewport(id);
        let tree = call("Page.getFrameTree", json!({}), &cdp)?;
        let pending = with_tab(id, |tab| {
            tab.frame = tree["frameTree"]["frame"]["id"].as_str().unwrap_or("").to_string();
            tab.pending.take()
        })?;
        if let Some(url) = pending {
            navigate_to(id, &url);
        }
        screencast(id);
        changed();
        Ok(())
    }

    fn apply_viewport(id: &str) {
        let Ok((cdp, (width, height))) = with_tab(id, |tab| (tab.cdp.clone(), tab.viewport())) else { return };
        if cdp.is_empty() {
            return;
        }
        post(
            "Emulation.setDeviceMetricsOverride",
            json!({ "width": width, "height": height, "deviceScaleFactor": 1, "mobile": false }),
            &cdp,
        );
        screencast(id);
    }

    /// 有人看着就推画面（不超过看着的里面最大的那个），没人看就停
    fn screencast(id: &str) {
        let Ok((cdp, size, viewport)) = with_tab(id, |tab| (tab.cdp.clone(), tab.frame_size(), tab.viewport())) else { return };
        if cdp.is_empty() {
            return;
        }
        match size {
            Some((width, height)) => post(
                "Page.startScreencast",
                json!({ "format": "jpeg", "quality": 70, "maxWidth": width.clamp(64, viewport.0.max(64)), "maxHeight": height.clamp(64, viewport.1.max(64)), "everyNthFrame": 1 }),
                &cdp,
            ),
            None => post("Page.stopScreencast", json!({}), &cdp),
        }
    }

    fn navigate_to(id: &str, url: &str) {
        let Ok(cdp) = with_tab(id, |tab| {
            tab.navigating = true;
            tab.cdp.clone()
        }) else {
            return;
        };
        changed();
        let (id, url) = (id.to_string(), url.to_string());
        // Page.navigate 在新页面提交（或失败）后才回，之后的加载状态由 frameStartedLoading / StoppedLoading 管
        std::thread::spawn(move || {
            let _ = call("Page.navigate", json!({ "url": url }), &cdp);
            let _ = with_tab(&id, |tab| tab.navigating = false);
            changed();
        });
    }

    /// 地址栏风格的输入 → 网址（像网址的补协议，别的用必应搜；同 Friday 的 normalize）
    pub fn normalize(input: &str) -> String {
        let text = input.trim();
        if text.is_empty() {
            return String::new();
        }
        let lower = text.to_lowercase();
        let has_scheme = text.split_once(':').is_some_and(|(scheme, rest)| {
            !scheme.is_empty()
                && scheme.chars().next().is_some_and(|c| c.is_ascii_alphabetic())
                && scheme.chars().all(|c| c.is_ascii_alphanumeric() || "+.-".contains(c))
                && !rest.split('/').next().is_some_and(|port| !port.is_empty() && port.chars().all(|c| c.is_ascii_digit()))
        }) || lower.starts_with("about:");
        if has_scheme {
            return text.to_string();
        }
        let host = text.split('/').next().unwrap_or("");
        let bare = host.split(':').next().unwrap_or("");
        if ["localhost", "127.0.0.1", "[::1]"].contains(&bare)
            || bare.split('.').count() == 4 && bare.split('.').all(|part| part.parse::<u8>().is_ok())
        {
            return format!("http://{text}");
        }
        let domain = !text.contains(' ')
            && bare.rsplit_once('.').is_some_and(|(_, top)| top.len() >= 2 && top.chars().all(|c| c.is_ascii_alphabetic()));
        if domain {
            return format!("https://{text}");
        }
        let encoded: String =
            text.bytes()
                .map(|byte| {
                    if byte.is_ascii_alphanumeric() || b"-_.~".contains(&byte) {
                        (byte as char).to_string()
                    } else {
                        format!("%{byte:02X}")
                    }
                })
                .collect();
        format!("https://www.bing.com/search?q={encoded}")
    }

    fn tabs_file() -> PathBuf {
        data_dir().join("tabs.json")
    }

    /// 第一次用时读回记下的标签（没接上，用到时按网址重新打开）
    fn load_tabs(inner: &mut Inner) {
        if inner.loaded {
            return;
        }
        inner.loaded = true;
        let Some(saved) = read_json(&tabs_file()) else { return };
        for item in saved["tabs"].as_array().into_iter().flatten() {
            let text = |key: &str| item[key].as_str().unwrap_or("").to_string();
            if text("id").is_empty() || text("session").is_empty() {
                continue;
            }
            let fixed = item["viewport"]
                .as_array()
                .filter(|size| size.len() == 2)
                .map(|size| (size[0].as_u64().unwrap_or(0) as u32, size[1].as_u64().unwrap_or(0) as u32));
            let url = text("url");
            inner.tabs.push(Tab {
                id: text("id"),
                session: text("session"),
                title: text("title"),
                pending: (!url.is_empty()).then(|| url.clone()),
                url,
                fixed,
                ..Default::default()
            });
        }
        for (session, tab) in saved["current"].as_object().into_iter().flatten() {
            inner.current.insert(session.clone(), tab.as_str().unwrap_or("").to_string());
        }
    }

    fn save() {
        let inner = BROWSER.inner.lock().unwrap();
        let tabs: Vec<Value> = inner
        .tabs
        .iter()
        .map(|tab| {
            let mut item = json!({ "id": tab.id, "session": tab.session, "url": tab.pending.clone().unwrap_or_else(|| tab.url.clone()), "title": tab.title });
            if let Some((width, height)) = tab.fixed {
                item["viewport"] = json!([width, height]);
            }
            item
        })
        .collect();
        let value = json!({ "tabs": tabs, "current": inner.current });
        drop(inner);
        let _ = write_json(&tabs_file(), &value);
    }

    /// 这段会话选中的标签：选过的（还在的），不然最后一个
    fn current_of(inner: &Inner, session: &str) -> String {
        let chosen = inner.current.get(session).filter(|id| inner.tabs.iter().any(|tab| tab.id == **id && tab.session == session));
        chosen.cloned().or_else(|| inner.tabs.iter().rev().find(|tab| tab.session == session).map(|tab| tab.id.clone())).unwrap_or_default()
    }

    /// BrowserState
    pub fn state() -> Value {
        if executable().is_none() {
            return json!({ "available": false, "reason": unavailable_reason(), "tabs": [] });
        }
        let mut inner = BROWSER.inner.lock().unwrap();
        load_tabs(&mut inner);
        let starting = running();
        let tabs: Vec<Value> = inner
            .tabs
            .iter()
            .map(|tab| {
                let host = tab.url.split_once("://").map(|(_, rest)| rest.split('/').next().unwrap_or("")).unwrap_or("");
                let title = if !tab.title.is_empty() {
                    tab.title.clone()
                } else if host.is_empty() {
                    "新标签".into()
                } else {
                    host.to_string()
                };
                let (width, height) = tab.viewport();
                let mut item = json!({
                    "id": tab.id, "conversationID": tab.session, "title": title,
                    "url": if tab.url == "about:blank" { "" } else { tab.url.as_str() },
                    "loading": tab.loading || tab.navigating || (tab.cdp.is_empty() && tab.pending.is_some() && starting),
                    "current": current_of(&inner, &tab.session) == tab.id, "viewport": [width, height], "fixed": tab.fixed.is_some(),
                });
                if let Some(dialog) = &tab.dialog {
                    item["dialog"] = dialog.clone();
                }
                if let Some(reason) = &tab.hand_over {
                    item["handOver"] = json!(reason);
                }
                item
            })
            .collect();
        let mut value = json!({ "available": true, "tabs": tabs });
        if !inner.start_error.is_empty() {
            value["reason"] = json!(inner.start_error);
        }
        value
    }

    /// 标签变了：80 毫秒内合成一次推给全权的连接
    fn changed() {
        BROWSER.dirty.store(true, Ordering::Relaxed);
        if !BROWSER.flusher.swap(true, Ordering::Relaxed) {
            std::thread::spawn(|| {
                loop {
                    std::thread::sleep(Duration::from_millis(80));
                    if BROWSER.dirty.swap(false, Ordering::Relaxed) {
                        crate::notify_all_clients("browser", state());
                    }
                }
            });
        }
    }

    /// 给这段会话开一个标签（选中它）
    pub fn open(session: &str, url: &str) -> Result<Value, String> {
        ensure_started()?;
        let target = normalize(url);
        let tab = Tab {
            id: uuid::Uuid::new_v4().to_string(),
            session: session.to_string(),
            // 空白的不用另外打开（新页面本来就是 about:blank）：设了的话接上后才去打开，会盖掉紧接着打开的网址
            pending: (!target.is_empty()).then(|| target.clone()),
            url: target,
            ..Default::default()
        };
        let id = tab.id.clone();
        {
            let mut inner = BROWSER.inner.lock().unwrap();
            load_tabs(&mut inner);
            inner.current.insert(session.to_string(), id.clone());
            inner.tabs.push(tab);
        }
        changed();
        save();
        let attaching = id.clone();
        std::thread::spawn(move || {
            if let Err(error) = attach(&attaching) {
                crate::serve_log("browser", &format!("接不上标签：{error}"));
            }
        });
        Ok(json!({ "tab": id }))
    }

    /// 用户在卡片里的操作、看画面这些都要标签接着：没接上的先接上（大脑重起后第一次看）
    fn wake(id: &str) {
        if with_tab(id, |tab| tab.cdp.is_empty()).unwrap_or(false) {
            let id = id.to_string();
            std::thread::spawn(move || {
                let _ = attach(&id);
            });
        }
    }

    /// Qt 的修饰键 → CDP 的（Alt 1、Ctrl 2、Meta 4、Shift 8）
    fn modifiers(qt: u64) -> u64 {
        let mut bits = 0;
        if qt & 0x0800_0000 != 0 {
            bits |= 1;
        }
        if qt & 0x0400_0000 != 0 {
            bits |= 2;
        }
        if qt & 0x1000_0000 != 0 {
            bits |= 4;
        }
        if qt & 0x0200_0000 != 0 {
            bits |= 8;
        }
        bits
    }

    /// DOM 的 code → Windows 虚拟键码（Chrome 按它做回车、退格、方向键这类的默认动作）
    fn virtual_key(code: &str, key: &str) -> u32 {
        let named = [
            ("Enter", 13),
            ("NumpadEnter", 13),
            ("Backspace", 8),
            ("Tab", 9),
            ("Escape", 27),
            ("Space", 32),
            ("PageUp", 33),
            ("PageDown", 34),
            ("End", 35),
            ("Home", 36),
            ("ArrowLeft", 37),
            ("ArrowUp", 38),
            ("ArrowRight", 39),
            ("ArrowDown", 40),
            ("Delete", 46),
            ("Insert", 45),
            ("ShiftLeft", 16),
            ("ShiftRight", 16),
            ("ControlLeft", 17),
            ("ControlRight", 17),
            ("AltLeft", 18),
            ("AltRight", 18),
            ("MetaLeft", 91),
            ("MetaRight", 92),
        ];
        if let Some((_, value)) = named.iter().find(|(name, _)| *name == code) {
            return *value;
        }
        if let Some(letter) = code.strip_prefix("Key").filter(|rest| rest.len() == 1) {
            return letter.to_ascii_uppercase().chars().next().unwrap_or('A') as u32;
        }
        if let Some(digit) = code.strip_prefix("Digit").filter(|rest| rest.len() == 1) {
            return digit.chars().next().unwrap_or('0') as u32;
        }
        if let Some(number) = code.strip_prefix('F').and_then(|rest| rest.parse::<u32>().ok()).filter(|number| *number >= 1) {
            return 111 + number;
        }
        let mut chars = key.chars();
        match (chars.next(), chars.next()) {
            (Some(only), None) => only.to_ascii_uppercase() as u32,
            _ => 0,
        }
    }

    /// browser.* 里除了 open、state 的（tab：标签 id；watcher：发请求的连接）
    pub fn request(method: &str, params: &Value, watcher: &str) -> Result<Value, String> {
        let id = params["tab"].as_str().ok_or("要给 tab")?.to_string();
        let number = |key: &str| params[key].as_f64().unwrap_or(0.0);
        let size = |key: &str, default: u32, top: u32| (params[key].as_u64().unwrap_or(default as u64) as u32).clamp(64, top);
        match method {
            "browser.select" => {
                let session = with_tab(&id, |tab| tab.session.clone())?;
                BROWSER.inner.lock().unwrap().current.insert(session, id);
                changed();
                save();
            }
            "browser.close" => close_tab(&id)?,
            "browser.navigate" => {
                let (cdp, attached) = with_tab(&id, |tab| (tab.cdp.clone(), !tab.cdp.is_empty()))?;
                if let Some(url) = params["url"].as_str().map(normalize).filter(|url| !url.is_empty()) {
                    if attached {
                        navigate_to(&id, &url);
                    } else {
                        with_tab(&id, |tab| tab.pending = Some(url))?;
                        wake(&id);
                    }
                    return Ok(json!({}));
                }
                if !attached {
                    return Ok(json!({}));
                }
                match params["action"].as_str().unwrap_or("reload") {
                    "stop" => post("Page.stopLoading", json!({}), &cdp),
                    "back" | "forward" => {
                        let step: i64 = if params["action"] == "back" { -1 } else { 1 };
                        std::thread::spawn(move || {
                            let Ok(history) = call("Page.getNavigationHistory", json!({}), &cdp) else { return };
                            let index = history["currentIndex"].as_i64().unwrap_or(0) + step;
                            if let Some(entry) = history["entries"].as_array().and_then(|entries| entries.get(usize::try_from(index).ok()?))
                            {
                                post("Page.navigateToHistoryEntry", json!({ "entryId": entry["id"] }), &cdp);
                            }
                        });
                    }
                    _ => post("Page.reload", json!({}), &cdp),
                }
            }
            "browser.watch" | "browser.unwatch" => {
                // 一个连接同时只看一个标签：换标签时别的标签不再推给它
                let watching = method == "browser.watch";
                {
                    let mut inner = BROWSER.inner.lock().unwrap();
                    for tab in inner.tabs.iter_mut() {
                        if tab.id != id || !watching {
                            tab.watchers.remove(watcher);
                        }
                    }
                    if watching && let Some(tab) = inner.tabs.iter_mut().find(|tab| tab.id == id) {
                        tab.watchers.insert(watcher.to_string(), (size("width", 1280, 3840), size("height", 800, 2160)));
                    }
                }
                let all: Vec<String> = BROWSER.inner.lock().unwrap().tabs.iter().map(|tab| tab.id.clone()).collect();
                for tab in all {
                    screencast(&tab);
                }
                if watching {
                    wake(&id);
                }
            }
            "browser.container" => {
                let (width, height) = (number("width") as u32, number("height") as u32);
                if width >= 10
                    && height >= 10
                    && with_tab(&id, |tab| {
                        std::mem::replace(&mut tab.container, (width, height)) != (width, height) && tab.fixed.is_none()
                    })?
                {
                    apply_viewport(&id);
                    changed();
                }
            }
            "browser.fit" => {
                with_tab(&id, |tab| tab.fixed = None)?;
                apply_viewport(&id);
                changed();
                save();
            }
            "browser.dialog" => {
                let cdp = with_tab(&id, |tab| {
                    tab.dialog = None;
                    tab.cdp.clone()
                })?;
                post(
                    "Page.handleJavaScriptDialog",
                    json!({ "accept": params["accept"].as_bool().unwrap_or(true), "promptText": params["text"].as_str().unwrap_or("") }),
                    &cdp,
                );
                changed();
            }
            // 智能体请用户做事（交给用户）：EDITH 的智能体还不会，点了「好了」照常
            // 用户在卡片上点了「好了」：替用户往这段会话说一句，智能体接着做
            "browser.handOverDone" => {
                let (session, reason) = with_tab(&id, |tab| (tab.session.clone(), tab.hand_over.take()))?;
                if let Some(reason) = reason {
                    changed();
                    // 替用户说的那句由网关照这段会话选的权限模式、模型交给大脑
                    return Ok(json!({ "session": session, "text": format!("浏览器里弄好了（{reason}）") }));
                }
            }
            "browser.input" => {
                let cdp = with_tab(&id, |tab| {
                    tab.user_input = Some(std::time::Instant::now());
                    tab.cdp.clone()
                })?;
                if cdp.is_empty() {
                    wake(&id);
                    return Ok(json!({}));
                }
                let qt = params["modifiers"].as_u64().unwrap_or(0);
                match params["kind"].as_str().unwrap_or("") {
                    kind @ ("mouse" | "wheel") => {
                        let kind_type = params["type"].as_str().unwrap_or("move");
                        let (event, pressed) = match (kind, kind_type) {
                            ("wheel", _) => ("mouseWheel", false),
                            (_, "press") => ("mousePressed", true),
                            (_, "release") => ("mouseReleased", true),
                            _ => ("mouseMoved", false),
                        };
                        let button = params["button"].as_str().unwrap_or("none");
                        let mut mouse = json!({
                            "type": event, "x": number("x"), "y": number("y"),
                            "button": if pressed || button != "none" { button } else { "none" },
                            "clickCount": if pressed { params["clicks"].as_u64().unwrap_or(1).max(1) } else { 0 },
                            "modifiers": modifiers(qt),
                        });
                        if event == "mouseWheel" {
                            mouse["deltaX"] = json!(number("dx"));
                            mouse["deltaY"] = json!(number("dy"));
                        }
                        post("Input.dispatchMouseEvent", mouse, &cdp);
                    }
                    "key" => {
                        let press = params["press"].as_bool().unwrap_or(true);
                        let key = params["key"].as_str().unwrap_or("");
                        let code = params["code"].as_str().unwrap_or("");
                        let text = params["text"].as_str().unwrap_or("");
                        let kind = if !press {
                            "keyUp"
                        } else if text.is_empty() {
                            "rawKeyDown"
                        } else {
                            "keyDown"
                        };
                        let mut event = json!({ "type": kind, "key": key, "code": code, "windowsVirtualKeyCode": virtual_key(code, key), "modifiers": modifiers(qt) });
                        if press && !text.is_empty() {
                            event["text"] = json!(text);
                        }
                        post("Input.dispatchKeyEvent", event, &cdp);
                    }
                    "text" => post("Input.insertText", json!({ "text": params["text"].as_str().unwrap_or("") }), &cdp),
                    _ => {}
                }
            }
            other => return Err(format!("没有 {other}")),
        }
        Ok(json!({}))
    }

    /// 连接断了：它看着的不再推给它
    pub fn forget(watcher: &str) {
        let ids: Vec<String> = {
            let mut inner = BROWSER.inner.lock().unwrap();
            inner.tabs.iter_mut().filter_map(|tab| tab.watchers.remove(watcher).map(|_| tab.id.clone())).collect()
        };
        for id in ids {
            screencast(&id);
        }
    }

    /// 关掉一个标签
    pub fn close_tab(id: &str) -> Result<(), String> {
        let tab = {
            let mut inner = BROWSER.inner.lock().unwrap();
            let index = inner.tabs.iter().position(|tab| tab.id == id).ok_or_else(|| format!("没有标签 {id}"))?;
            let tab = inner.tabs.remove(index);
            if inner.current.get(&tab.session) == Some(&tab.id) {
                inner.current.remove(&tab.session);
            }
            tab
        };
        if !tab.target.is_empty() {
            post("Target.closeTarget", json!({ "targetId": tab.target }), "");
        }
        changed();
        save();
        Ok(())
    }

    // MARK: - 智能体用的（browser_tools.rs 的工具）

    /// 这台有没有浏览器（智能体才有浏览器工具）
    pub fn available() -> bool {
        executable().is_some()
    }

    /// 一个标签给智能体看的样子
    pub struct TabInfo {
        pub id: String,
        pub title: String,
        pub url: String,
        pub loading: bool,
        pub current: bool,
        pub opener: String,
        pub hand_over: bool,
    }

    fn tab_info(inner: &Inner, tab: &Tab) -> TabInfo {
        TabInfo {
            id: tab.id.clone(),
            title: tab.title.clone(),
            url: if tab.url == "about:blank" { String::new() } else { tab.url.clone() },
            loading: tab.loading || tab.navigating || (tab.cdp.is_empty() && tab.pending.is_some()),
            current: current_of(inner, &tab.session) == tab.id,
            opener: tab.opener.clone(),
            hand_over: tab.hand_over.is_some(),
        }
    }

    /// 这段会话的标签（开的先后）
    pub fn tabs_of(session: &str) -> Vec<TabInfo> {
        let mut inner = BROWSER.inner.lock().unwrap();
        load_tabs(&mut inner);
        inner.tabs.iter().filter(|tab| tab.session == session).map(|tab| tab_info(&inner, tab)).collect()
    }

    pub fn info(id: &str) -> Option<TabInfo> {
        let inner = BROWSER.inner.lock().unwrap();
        inner.tabs.iter().find(|tab| tab.id == id).map(|tab| tab_info(&inner, tab))
    }

    /// 这段会话选中的标签
    pub fn current_tab(session: &str) -> Option<String> {
        let mut inner = BROWSER.inner.lock().unwrap();
        load_tabs(&mut inner);
        Some(current_of(&inner, session)).filter(|id| !id.is_empty())
    }

    /// 智能体开的标签（这一轮结束时关掉）、留给用户的
    static AGENT_TABS: LazyLock<Mutex<HashMap<String, HashSet<String>>>> = LazyLock::new(|| Mutex::new(HashMap::new()));
    static KEPT: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(|| Mutex::new(HashSet::new()));

    /// 给这段会话开一个标签、等它接上（智能体开的：这一轮结束时关掉）
    pub fn open_for_agent(session: &str, url: &str) -> Result<String, String> {
        let id = open(session, "")?["tab"].as_str().unwrap_or("").to_string();
        AGENT_TABS.lock().unwrap().entry(session.to_string()).or_default().insert(id.clone());
        attach(&id)?;
        if !url.trim().is_empty() {
            load(&id, url);
        }
        Ok(id)
    }

    /// 接上（大脑重起后第一次用、浏览器退出过）
    pub fn wake_now(id: &str) -> Result<(), String> {
        attach(id)
    }

    pub fn keep(id: &str) {
        KEPT.lock().unwrap().insert(id.to_string());
    }

    /// 这一轮结束时会关掉：智能体开的、没留给用户的
    pub fn closes_at_turn_end(session: &str, id: &str) -> bool {
        AGENT_TABS.lock().unwrap().get(session).is_some_and(|tabs| tabs.contains(id)) && !KEPT.lock().unwrap().contains(id)
    }

    /// 一轮结束：关掉智能体开的标签（连同它们开出来的新窗口）；正交给用户操作的下一轮结束再关
    pub fn close_agent_tabs(session: &str) {
        let Some(agent) = AGENT_TABS.lock().unwrap().remove(session) else { return };
        let kept = KEPT.lock().unwrap().clone();
        let list = tabs_of(session);
        let mut waiting = HashSet::new();
        let mut closing = HashSet::new();
        for tab in list.iter().filter(|tab| agent.contains(&tab.id) && !kept.contains(&tab.id)) {
            if tab.hand_over {
                waiting.insert(tab.id.clone());
            } else {
                closing.insert(tab.id.clone());
            }
        }
        let mut grew = true;
        while grew {
            grew = false;
            for tab in &list {
                if !closing.contains(&tab.id) && !waiting.contains(&tab.id) && !kept.contains(&tab.id) && closing.contains(&tab.opener) {
                    closing.insert(tab.id.clone());
                    grew = true;
                }
            }
        }
        for id in &closing {
            let _ = close_tab(id);
        }
        if !waiting.is_empty() {
            AGENT_TABS.lock().unwrap().entry(session.to_string()).or_default().extend(waiting);
        }
    }

    /// 打开网址（地址栏风格的输入）
    pub fn load(id: &str, url: &str) {
        let url = normalize(url);
        if url.is_empty() {
            return;
        }
        let attached = with_tab(id, |tab| !tab.cdp.is_empty()).unwrap_or(false);
        if attached {
            navigate_to(id, &url);
        } else if with_tab(id, |tab| tab.pending = Some(url)).is_ok() {
            let _ = attach(id);
        }
    }

    /// 后退 / 前进 / 刷新 / 停下
    pub fn navigate_action(id: &str, action: &str) -> Result<(), String> {
        request("browser.navigate", &json!({ "tab": id, "action": action }), "").map(|_| ())
    }

    pub fn select(id: &str) -> Result<(), String> {
        request("browser.select", &json!({ "tab": id }), "").map(|_| ())
    }

    /// 在页面里跑一段脚本（isolated：在 edith 这个隔离世界里跑，不和网页自己的变量打架；元素编号表也存在那里）
    pub fn evaluate(id: &str, script: &str, isolated: bool) -> Result<Value, String> {
        attach(id)?;
        for attempt in 0..2 {
            let (cdp, frame, world) = with_tab(id, |tab| (tab.cdp.clone(), tab.frame.clone(), tab.world))?;
            let mut params = json!({ "expression": script, "returnByValue": true, "awaitPromise": true });
            if isolated {
                let context = if world != 0 {
                    world
                } else {
                    let created = call("Page.createIsolatedWorld", json!({ "frameId": frame, "worldName": "edith" }), &cdp)?;
                    let context = created["executionContextId"].as_u64().unwrap_or(0);
                    with_tab(id, |tab| tab.world = context)?;
                    context
                };
                params["contextId"] = json!(context);
            }
            match call("Runtime.evaluate", params, &cdp) {
                Ok(result) => {
                    if let Some(exception) = result.get("exceptionDetails") {
                        let text = exception["exception"]["description"].as_str().or(exception["text"].as_str()).unwrap_or("脚本出错了");
                        return Err(text.to_string());
                    }
                    return Ok(result["result"]["value"].clone());
                }
                // 隔离世界过期了（换了页面）：重建一次再跑
                Err(_) if isolated && attempt == 0 => {
                    with_tab(id, |tab| tab.world = 0)?;
                }
                Err(error) => return Err(error),
            }
        }
        Err("脚本没跑成".into())
    }

    fn cdp_of(id: &str) -> Result<String, String> {
        let cdp = with_tab(id, |tab| tab.cdp.clone())?;
        if cdp.is_empty() { Err("网页还没准备好，稍后再试".into()) } else { Ok(cdp) }
    }

    /// 鼠标：type 是 mouseMoved / mousePressed / mouseReleased / mouseWheel
    pub fn mouse(id: &str, kind: &str, x: f64, y: f64, button: &str, clicks: u64, delta: (f64, f64)) -> Result<(), String> {
        let cdp = cdp_of(id)?;
        let mut event = json!({ "type": kind, "x": x, "y": y, "button": button, "clickCount": clicks, "modifiers": 0 });
        if kind == "mouseWheel" {
            event["deltaX"] = json!(delta.0);
            event["deltaY"] = json!(delta.1);
        }
        call("Input.dispatchMouseEvent", event, &cdp).map(|_| ())
    }

    /// 点一下（clicks 次）
    pub fn click(id: &str, x: f64, y: f64, button: &str, clicks: u64) -> Result<(), String> {
        mouse(id, "mouseMoved", x, y, "none", 0, (0.0, 0.0))?;
        for count in 1..=clicks.max(1) {
            mouse(id, "mousePressed", x, y, button, count, (0.0, 0.0))?;
            mouse(id, "mouseReleased", x, y, button, count, (0.0, 0.0))?;
        }
        Ok(())
    }

    /// 智能体按的键（Enter、Tab、Ctrl+A…）→ CDP 的 key / code / 文字 / 修饰键 / 编辑命令（无头浏览器不认系统快捷键）
    fn parse_combo(combo: &str) -> Option<(String, String, String, u64, Vec<&'static str>)> {
        let parts: Vec<&str> = combo.split('+').map(str::trim).filter(|part| !part.is_empty()).collect();
        let (name, modifiers_part) = parts.split_last()?;
        let mut modifiers = 0;
        for modifier in modifiers_part {
            modifiers |= match modifier.to_lowercase().as_str() {
                "ctrl" | "control" | "cmd" | "command" => 2,
                "shift" => 8,
                "alt" | "option" => 1,
                "meta" | "win" => 4,
                _ => return None,
            };
        }
        let lower = name.to_lowercase();
        let named: &[(&str, &str, &str)] = &[
            ("enter", "Enter", "\r"),
            ("return", "Enter", "\r"),
            ("tab", "Tab", ""),
            ("escape", "Escape", ""),
            ("esc", "Escape", ""),
            ("backspace", "Backspace", ""),
            ("delete", "Delete", ""),
            ("space", " ", " "),
            ("arrowup", "ArrowUp", ""),
            ("up", "ArrowUp", ""),
            ("arrowdown", "ArrowDown", ""),
            ("down", "ArrowDown", ""),
            ("arrowleft", "ArrowLeft", ""),
            ("left", "ArrowLeft", ""),
            ("arrowright", "ArrowRight", ""),
            ("right", "ArrowRight", ""),
            ("home", "Home", ""),
            ("end", "End", ""),
            ("pageup", "PageUp", ""),
            ("pagedown", "PageDown", ""),
        ];
        if let Some((_, key, text)) = named.iter().find(|(alias, _, _)| *alias == lower) {
            let code = if *key == " " { "Space".to_string() } else { key.to_string() };
            let text = if modifiers & 2 != 0 { String::new() } else { text.to_string() };
            return Some((key.to_string(), code, text, modifiers, Vec::new()));
        }
        let mut chars = name.chars();
        if let (Some(character), None) = (chars.next(), chars.next()) {
            let key = if modifiers & 8 != 0 { character.to_uppercase().to_string() } else { character.to_lowercase().to_string() };
            let code =
                if character.is_ascii_digit() { format!("Digit{character}") } else { format!("Key{}", character.to_ascii_uppercase()) };
            let text = if modifiers & (1 | 2 | 4) == 0 { key.clone() } else { String::new() };
            let commands = if modifiers & 2 != 0 {
                match lower.as_str() {
                    "a" => vec!["selectAll"],
                    "c" => vec!["copy"],
                    "v" => vec!["paste"],
                    "x" => vec!["cut"],
                    "z" => vec!["undo"],
                    _ => Vec::new(),
                }
            } else {
                Vec::new()
            };
            return Some((key, code, text, modifiers, commands));
        }
        let number = lower.strip_prefix('f').and_then(|rest| rest.parse::<u32>().ok()).filter(|number| (1..=12).contains(number))?;
        let key = format!("F{number}");
        Some((key.clone(), key, String::new(), modifiers, Vec::new()))
    }

    /// 按一个键（组合键）；认不出回 false
    pub fn press_key(id: &str, combo: &str) -> Result<bool, String> {
        let Some((key, code, text, modifiers, commands)) = parse_combo(combo) else { return Ok(false) };
        let cdp = cdp_of(id)?;
        let mut down = json!({
            "type": if text.is_empty() { "rawKeyDown" } else { "keyDown" }, "key": key, "code": code,
            "windowsVirtualKeyCode": virtual_key(&code, &key), "modifiers": modifiers,
        });
        if !text.is_empty() {
            down["text"] = json!(text);
        }
        if !commands.is_empty() {
            down["commands"] = json!(commands);
        }
        call("Input.dispatchKeyEvent", down.clone(), &cdp)?;
        let mut up = down;
        up["type"] = json!("keyUp");
        if let Some(object) = up.as_object_mut() {
            object.remove("text");
            object.remove("commands");
        }
        call("Input.dispatchKeyEvent", up, &cdp).map(|_| Ok(true))?
    }

    pub fn insert_text(id: &str, text: &str) -> Result<(), String> {
        let cdp = cdp_of(id)?;
        call("Input.insertText", json!({ "text": text }), &cdp).map(|_| ())
    }

    /// 视口截图（JPEG，按视口的 CSS 像素）
    pub fn screenshot(id: &str) -> Result<Vec<u8>, String> {
        use base64::Engine;
        attach(id)?;
        let cdp = cdp_of(id)?;
        let shot = call("Page.captureScreenshot", json!({ "format": "jpeg", "quality": 85 }), &cdp)?;
        base64::engine::general_purpose::STANDARD
            .decode(shot["data"].as_str().unwrap_or(""))
            .map_err(|error| format!("截图读不了：{error}"))
    }

    pub fn viewport_of(id: &str) -> (u32, u32) {
        with_tab(id, |tab| tab.viewport()).unwrap_or(DEFAULT_VIEWPORT)
    }

    /// 智能体定的固定视口（None：回到跟着卡片）
    pub fn set_fixed(id: &str, size: Option<(u32, u32)>) -> Result<(), String> {
        with_tab(id, |tab| tab.fixed = size)?;
        apply_viewport(id);
        changed();
        save();
        Ok(())
    }

    /// 没回答的对话框：「确认框「…」」这样的一句
    pub fn pending_dialog(id: &str) -> String {
        with_tab(id, |tab| {
            tab.dialog.as_ref().map(|dialog| {
                let kind = match dialog["type"].as_str().unwrap_or("") {
                    "confirm" => "确认框",
                    "prompt" => "输入框",
                    "beforeunload" => "离开确认",
                    _ => "提示框",
                };
                format!("{kind}「{}」", dialog["message"].as_str().unwrap_or(""))
            })
        })
        .ok()
        .flatten()
        .unwrap_or_default()
    }

    pub fn answer_dialog(id: &str, accept: bool, text: &str) -> Result<bool, String> {
        if with_tab(id, |tab| tab.dialog.is_none())? {
            return Ok(false);
        }
        request("browser.dialog", &json!({ "tab": id, "accept": accept, "text": text }), "")?;
        Ok(true)
    }

    pub fn take_downloads(id: &str) -> Vec<String> {
        with_tab(id, |tab| std::mem::take(&mut tab.downloads)).unwrap_or_default()
    }

    /// 请用户在这个标签里做件事（登录、人机验证……）：卡片上显示、推 browser.handOver 给客户端；用户点「好了」后替用户说一句
    pub fn hand_over(id: &str, reason: &str) -> Result<(), String> {
        let session = with_tab(id, |tab| {
            tab.hand_over = Some(reason.to_string());
            tab.session.clone()
        })?;
        BROWSER.inner.lock().unwrap().current.insert(session.clone(), id.to_string());
        crate::notify_all_clients("browser.handOver", json!({ "conversationID": session, "tab": id, "reason": reason }));
        changed();
        Ok(())
    }

    /// 用户这 3 秒里在卡片里动过（智能体让开）
    pub fn user_active(id: &str) -> bool {
        with_tab(id, |tab| tab.user_input.is_some_and(|at| at.elapsed() < Duration::from_secs(3))).unwrap_or(false)
    }

    /// Windows：DevTools 的 WebSocket（只连本机，不用 TLS；够 CDP 用的那一点：文本帧、分片、ping）
    #[cfg(windows)]
    mod socket {
        use std::io::{BufRead, BufReader, Read, Write};
        use std::net::TcpStream;

        pub struct Socket(TcpStream);

        pub struct Reader(BufReader<TcpStream>, TcpStream);

        pub fn connect(address: &str, path: &str) -> Result<(Socket, Reader), String> {
            use base64::Engine;
            let mut stream = TcpStream::connect(address).map_err(|error| format!("连不上浏览器 {address}：{error}"))?;
            let key = base64::engine::general_purpose::STANDARD.encode(uuid::Uuid::new_v4().as_bytes());
            let request = format!(
                "GET {path} HTTP/1.1\r\nHost: {address}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n"
            );
            stream.write_all(request.as_bytes()).map_err(|error| error.to_string())?;
            let mut reader = BufReader::new(stream.try_clone().map_err(|error| error.to_string())?);
            let mut status = String::new();
            reader.read_line(&mut status).map_err(|error| error.to_string())?;
            if !status.contains(" 101 ") {
                return Err(format!("浏览器不肯升级成 WebSocket：{}", status.trim()));
            }
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line).map_err(|error| error.to_string())? == 0 || line == "\r\n" {
                    break;
                }
            }
            let pong = stream.try_clone().map_err(|error| error.to_string())?;
            Ok((Socket(stream), Reader(reader, pong)))
        }

        fn frame(opcode: u8, payload: &[u8]) -> Vec<u8> {
            let mut out = vec![0x80 | opcode];
            let length = payload.len();
            if length < 126 {
                out.push(0x80 | length as u8);
            } else if length <= u16::MAX as usize {
                out.push(0x80 | 126);
                out.extend_from_slice(&(length as u16).to_be_bytes());
            } else {
                out.push(0x80 | 127);
                out.extend_from_slice(&(length as u64).to_be_bytes());
            }
            let mask = *uuid::Uuid::new_v4().as_bytes();
            out.extend_from_slice(&mask[..4]);
            out.extend(payload.iter().enumerate().map(|(index, byte)| byte ^ mask[index % 4]));
            out
        }

        impl Socket {
            pub fn send_text(&mut self, text: &str) -> std::io::Result<()> {
                self.0.write_all(&frame(0x1, text.as_bytes()))?;
                self.0.flush()
            }
        }

        impl Reader {
            /// 下一条文本消息（分片的拼好）；断了是 None
            pub fn next_text(&mut self) -> Option<String> {
                let mut message = Vec::new();
                loop {
                    let mut head = [0u8; 2];
                    self.0.read_exact(&mut head).ok()?;
                    let (fin, opcode) = (head[0] & 0x80 != 0, head[0] & 0x0f);
                    let mut length = (head[1] & 0x7f) as u64;
                    if length == 126 {
                        let mut bytes = [0u8; 2];
                        self.0.read_exact(&mut bytes).ok()?;
                        length = u16::from_be_bytes(bytes) as u64;
                    } else if length == 127 {
                        let mut bytes = [0u8; 8];
                        self.0.read_exact(&mut bytes).ok()?;
                        length = u64::from_be_bytes(bytes);
                    }
                    let mask = if head[1] & 0x80 != 0 {
                        let mut bytes = [0u8; 4];
                        self.0.read_exact(&mut bytes).ok()?;
                        Some(bytes)
                    } else {
                        None
                    };
                    let mut payload = vec![0u8; length as usize];
                    self.0.read_exact(&mut payload).ok()?;
                    if let Some(mask) = mask {
                        for (index, byte) in payload.iter_mut().enumerate() {
                            *byte ^= mask[index % 4];
                        }
                    }
                    match opcode {
                        0x8 => return None,
                        0x9 => {
                            let _ = self.1.write_all(&frame(0xA, &payload));
                            continue;
                        }
                        0xA => continue,
                        _ => message.extend(payload),
                    }
                    if fin {
                        return String::from_utf8(message).ok();
                    }
                }
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn address_bar_and_keys() {
            assert_eq!(normalize("example.com/a"), "https://example.com/a");
            assert_eq!(normalize("localhost:8080"), "http://localhost:8080");
            assert_eq!(normalize("192.168.1.2/x"), "http://192.168.1.2/x");
            assert_eq!(normalize("https://a.b/c"), "https://a.b/c");
            assert_eq!(normalize("about:blank"), "about:blank");
            assert_eq!(normalize("怎么 煮饭"), "https://www.bing.com/search?q=%E6%80%8E%E4%B9%88%20%E7%85%AE%E9%A5%AD");
            assert_eq!((virtual_key("Enter", "Enter"), virtual_key("KeyA", "a"), virtual_key("F5", "F5")), (13, 65, 116));
            assert_eq!(modifiers(0x0400_0000 | 0x0200_0000), 2 | 8);
            assert_eq!(display_title("example.com/", "https://example.com/"), "");
            // 智能体按的键
            let (key, code, text, modifiers, commands) = parse_combo("Ctrl+A").unwrap();
            assert_eq!((key.as_str(), code.as_str(), text.as_str(), modifiers, commands), ("a", "KeyA", "", 2, vec!["selectAll"]));
            let (key, _, text, _, _) = parse_combo("Enter").unwrap();
            assert_eq!((key.as_str(), text.as_str()), ("Enter", "\r"));
            assert_eq!(parse_combo("Shift+F5").map(|stroke| (stroke.0, stroke.3)), Some(("F5".into(), 8)));
            assert!(parse_combo("Hyper+X").is_none());
        }
    }
}

// MARK: - 智能体的浏览器工具（照 Friday 的 BrowserTools）

mod browser_tools {
    //! 智能体的浏览器工具（照 Friday 的 BrowserTools，Friday 仓库的 qt/docs/BROWSER.md §6）：browser_navigate / read / act / screenshot /
    //! resize / tabs / hand_over / run_js，操作的是这段会话在大脑里的浏览器标签（browser.rs）——用户在 Friday 的浏览器卡片里看得见、能一起操作。
    //! 定义和说明在 friday-core（tool_defs、prompts-tools.md 的 tool.browser_*）；按需加载（deferred.rs 的「浏览器」一组）。
    //! 智能体开的标签这一轮结束时关掉（带 keep 的留给用户）；截图随下一段请求给能看图的模型。

    use std::collections::HashMap;
    use std::sync::{LazyLock, Mutex};
    use std::time::{Duration, Instant};

    use serde_json::{Map, Value, json};

    use crate::browser;

    /// 动作的结果（act 的回话）：截图随下一段请求给能看图的模型（media）
    pub struct Outcome {
        pub ok: bool,
        pub text: String,
        pub image: Option<String>,
    }

    /// 定义（friday-core 的 tool_defs + prompts-tools.md 的 tool.browser_*，展开好的；改了那边要同步这里）
    const DEFINITIONS: &str = r##"[
  {
    "name": "browser_navigate",
    "permission": "network",
    "spec": {
      "function": {
        "description": "在这段对话的内置浏览器里打开网页（用户在浏览器卡片里看得见、能一起操作，登录状态一直留着），等加载完返回标题、网址和页面开头的文字。要看动态网页、要登录才能看、要点按钮填表时用它；只是读一篇公开文章用 fetch_url 更快。不给 url 给 action 时是后退 / 前进 / 刷新。你开的标签这一轮结束时自动关掉；用户让你打开给他看、要接着用的带上 keep。",
        "name": "browser_navigate",
        "parameters": {
          "properties": {
            "action": {
              "description": "back / forward / reload（不给 url 时用）",
              "type": "string"
            },
            "keep": {
              "description": "true：这个标签留给用户，这一轮结束时不关（用户让你打开给他看、要接着用的）；只给 tab 和 keep 是把已经开着的标签留下",
              "type": "boolean"
            },
            "new_tab": {
              "description": "在新标签里打开；不填在当前标签里打开（还没有标签时自动开一个）",
              "type": "boolean"
            },
            "tab": {
              "description": "在哪个标签：编号（结果里给的 8 位）、标题里的字或第几个；不填是当前标签",
              "type": "string"
            },
            "url": {
              "description": "要打开的网址",
              "type": "string"
            }
          },
          "required": [],
          "type": "object"
        }
      },
      "type": "function"
    },
    "title": "打开网页"
  },
  {
    "name": "browser_read",
    "permission": "readOnly",
    "spec": {
      "function": {
        "description": "读当前网页：mode=text 是页面上的文字；mode=elements 列出可交互的元素（链接、按钮、输入框、下拉框…），每个一行带编号如 [e12]，之后用 browser_act 的 ref 指它。要点、要填之前先读 elements。",
        "name": "browser_read",
        "parameters": {
          "properties": {
            "mode": {
              "description": "text（默认，页面文字）或 elements（可交互的元素和编号）",
              "type": "string"
            },
            "tab": {
              "description": "读哪个标签；不填是当前标签",
              "type": "string"
            }
          },
          "required": [],
          "type": "object"
        }
      },
      "type": "function"
    },
    "title": "读网页"
  },
  {
    "name": "browser_act",
    "permission": "network",
    "spec": {
      "function": {
        "description": "在网页上做一个动作（真的鼠标键盘事件，网页当成用户在操作）：click / double_click / right_click 点 ref 指的元素或 x、y；type 往 ref 指的输入框（或焦点处）输入 text，默认先清空，submit=true 输完按回车；key 按一个键或组合键；scroll 往 direction 滚 amount 格，给 ref 时滚到它；hover 把鼠标移上去；select 在下拉框 ref 里选 text 那一项；dialog 回答网页弹的对话框（accept、text）。返回动作后网址、标题、焦点有没有变。用户正在卡片里操作时会让开。要用户亲手做的（登录、验证码、付款确认）用 browser_hand_over，不要硬闯。",
        "name": "browser_act",
        "parameters": {
          "properties": {
            "accept": {
              "description": "dialog 时：true 确定、false 取消，默认 true",
              "type": "boolean"
            },
            "action": {
              "description": "click / double_click / right_click / type / key / scroll / hover / select / dialog",
              "type": "string"
            },
            "amount": {
              "description": "scroll 滚几格，默认 5（大约半屏到一屏）",
              "type": "integer"
            },
            "append": {
              "description": "type 时接在原有内容后面，不先清空",
              "type": "boolean"
            },
            "direction": {
              "description": "scroll 的方向：up / down / left / right，默认 down",
              "type": "string"
            },
            "key": {
              "description": "key 时按的键：Enter、Tab、Escape、Backspace、ArrowDown、PageDown、Ctrl+A 这类",
              "type": "string"
            },
            "ref": {
              "description": "元素编号（browser_read elements 列出的，如 e12）；优先用它，比坐标可靠",
              "type": "string"
            },
            "submit": {
              "description": "type 输完按回车",
              "type": "boolean"
            },
            "tab": {
              "description": "在哪个标签；不填是当前标签",
              "type": "string"
            },
            "text": {
              "description": "type 时要输入的文字；select 时选项的文字或值；dialog 时 prompt 对话框要填的字",
              "type": "string"
            },
            "x": {
              "description": "视口里的横坐标（CSS 像素，截图上的位置）；没有 ref 时用",
              "type": "number"
            },
            "y": {
              "description": "视口里的纵坐标（CSS 像素）",
              "type": "number"
            }
          },
          "required": [
            "action"
          ],
          "type": "object"
        }
      },
      "type": "function"
    },
    "title": "操作网页"
  },
  {
    "name": "browser_screenshot",
    "permission": "readOnly",
    "spec": {
      "function": {
        "description": "截当前网页的视口给你看（坐标就是 browser_act 的 x、y）。读文字、找元素先用 browser_read，看布局、图表、验证码这类只有图才看得出的再截图；截图也会显示给用户。",
        "name": "browser_screenshot",
        "parameters": {
          "properties": {
            "tab": {
              "description": "截哪个标签；不填是当前标签",
              "type": "string"
            }
          },
          "required": [],
          "type": "object"
        }
      },
      "type": "function"
    },
    "title": "网页截图"
  },
  {
    "name": "browser_resize",
    "permission": "network",
    "spec": {
      "function": {
        "description": "改网页的视口大小（网页认为的窗口大小，如 375×812 看手机版），用户的卡片大小不变、按比例缩小显示；fit=true 回到跟随卡片。",
        "name": "browser_resize",
        "parameters": {
          "properties": {
            "fit": {
              "description": "true：回到跟随卡片的大小",
              "type": "boolean"
            },
            "height": {
              "description": "视口高（CSS 像素，240–2160）",
              "type": "integer"
            },
            "tab": {
              "description": "改哪个标签；不填是当前标签",
              "type": "string"
            },
            "width": {
              "description": "视口宽（CSS 像素，320–3840）",
              "type": "integer"
            }
          },
          "required": [],
          "type": "object"
        }
      },
      "type": "function"
    },
    "title": "调整网页视口"
  },
  {
    "name": "browser_tabs",
    "permission": "network",
    "spec": {
      "function": {
        "description": "列出这段对话的浏览器标签（编号、标题、网址），或切换、关闭一个。开新标签用 browser_navigate 的 new_tab。你开的标签这一轮结束时会自动关掉，不用一个个关。",
        "name": "browser_tabs",
        "parameters": {
          "properties": {
            "action": {
              "description": "list（默认）/ select / close",
              "type": "string"
            },
            "tab": {
              "description": "select、close 的标签：编号、标题里的字或第几个",
              "type": "string"
            }
          },
          "required": [],
          "type": "object"
        }
      },
      "type": "function"
    },
    "title": "浏览器标签"
  },
  {
    "name": "browser_hand_over",
    "permission": "readOnly",
    "spec": {
      "function": {
        "description": "请用户在浏览器里亲手做一件事（登录、扫码、人机验证、两步验证、确认付款）：打开浏览器卡片、停在这个标签，顶上显示 reason 和「好了」按钮，马上返回。之后结束这一轮、告诉用户要做什么；用户点「好了」会发来消息，再接着做。",
        "name": "browser_hand_over",
        "parameters": {
          "properties": {
            "reason": {
              "description": "请用户做什么，一句话（例如「登录 GitHub」「完成滑块验证」）",
              "type": "string"
            },
            "tab": {
              "description": "哪个标签；不填是当前标签",
              "type": "string"
            }
          },
          "required": [
            "reason"
          ],
          "type": "object"
        }
      },
      "type": "function"
    },
    "title": "请你在浏览器里操作"
  },
  {
    "name": "browser_run_js",
    "permission": "execute",
    "spec": {
      "function": {
        "description": "在当前网页里运行一段 JavaScript（网页自己的环境），返回最后一个表达式的值（JSON）。读不到、点不到的情况再用它；能用 browser_read / browser_act 的别用它。",
        "name": "browser_run_js",
        "parameters": {
          "properties": {
            "script": {
              "description": "要运行的脚本",
              "type": "string"
            },
            "tab": {
              "description": "在哪个标签；不填是当前标签",
              "type": "string"
            }
          },
          "required": [
            "script"
          ],
          "type": "object"
        }
      },
      "type": "function"
    },
    "title": "在网页里运行脚本"
  }
]"##;

    pub const TOOLS: &[&str] = &[
        "browser_navigate",
        "browser_read",
        "browser_act",
        "browser_screenshot",
        "browser_resize",
        "browser_tabs",
        "browser_hand_over",
        "browser_run_js",
    ];

    /// 结果只留这么多字
    const MAX_TEXT: usize = 16000;
    /// 元素列表最多列这么多个
    const MAX_ELEMENTS: usize = 300;

    pub fn is_tool(name: &str) -> bool {
        TOOLS.contains(&name)
    }

    /// 这几个工具的定义（这台有浏览器才给）：`[{name, title, permission, spec}]`
    pub fn definitions() -> Vec<Value> {
        if !browser::available() {
            return Vec::new();
        }
        serde_json::from_str(DEFINITIONS).unwrap_or_default()
    }

    fn clip(text: &str, max: usize) -> String {
        let count = text.chars().count();
        if count <= max {
            return text.to_string();
        }
        format!("{}\n…（后面还有 {} 个字符没列出）", text.chars().take(max).collect::<String>(), count - max)
    }

    fn short(id: &str) -> String {
        id.chars().take(8).collect::<String>().to_lowercase()
    }

    fn text_arg(arguments: &Map<String, Value>, key: &str) -> Option<String> {
        arguments.get(key).and_then(Value::as_str).map(str::trim).filter(|value| !value.is_empty()).map(str::to_string)
    }

    fn bool_arg(arguments: &Map<String, Value>, key: &str, fallback: bool) -> bool {
        match arguments.get(key) {
            Some(Value::Bool(value)) => *value,
            Some(Value::String(text)) => match text.trim().to_lowercase().as_str() {
                "true" | "1" | "yes" => true,
                "false" | "0" | "no" => false,
                _ => fallback,
            },
            Some(Value::Number(number)) => number.as_f64().is_some_and(|value| value != 0.0),
            _ => fallback,
        }
    }

    fn number_arg(arguments: &Map<String, Value>, key: &str) -> Option<f64> {
        match arguments.get(key) {
            Some(Value::Number(number)) => number.as_f64(),
            Some(Value::String(text)) => text.trim().parse().ok(),
            _ => None,
        }
    }

    fn quoted(text: &str) -> String {
        serde_json::to_string(text).unwrap_or_else(|_| "\"\"".into())
    }

    // MARK: - 页面上的脚本（edith 隔离世界）

    /// 元素列表：可交互、看得见的元素编上号（编号表存在隔离世界里，同一个元素一直是同一个号）
    fn elements_script() -> String {
        r#"
(function (maxCount) {
  const W = window;
  if (!W.__edithRefs) { W.__edithRefs = new Map(); W.__edithIds = new WeakMap(); W.__edithNext = 1; }
  const selector = 'a[href],button,input,textarea,select,summary,[role=button],[role=link],[role=checkbox],[role=radio],[role=tab],' +
    '[role=menuitem],[role=option],[role=switch],[role=combobox],[role=textbox],[role=searchbox],[contenteditable=""],[contenteditable=true],' +
    '[onclick],[tabindex]:not([tabindex="-1"])';
  const vw = W.innerWidth, vh = W.innerHeight;
  const out = [];
  let total = 0;
  const textOf = (el) => (el.innerText || el.textContent || '').replace(/\s+/g, ' ').trim();
  for (const el of document.querySelectorAll(selector)) {
    const r = el.getBoundingClientRect();
    if (r.width < 1 || r.height < 1) continue;
    const st = getComputedStyle(el);
    if (st.visibility === 'hidden' || st.display === 'none' || Number(st.opacity) === 0) continue;
    total++;
    if (out.length >= maxCount) continue;
    let id = W.__edithIds.get(el);
    if (!id) { id = 'e' + (W.__edithNext++); W.__edithIds.set(el, id); }
    W.__edithRefs.set(id, new WeakRef(el));
    const tag = el.tagName.toLowerCase();
    const type = (el.getAttribute('type') || '').toLowerCase();
    let label = el.getAttribute('aria-label') || '';
    if (!label && el.id) { const l = document.querySelector('label[for="' + CSS.escape(el.id) + '"]'); if (l) label = textOf(l); }
    if (!label && (tag === 'input' || tag === 'textarea' || tag === 'select')) { const l = el.closest('label'); if (l) label = textOf(l); }
    let name = label || textOf(el) || el.getAttribute('title') || el.getAttribute('alt') || '';
    if (!name && tag === 'a') { const img = el.querySelector('img[alt]'); if (img) name = img.getAttribute('alt'); }
    const item = { ref: id, tag: tag, role: el.getAttribute('role') || '', type: type, name: name.slice(0, 80),
      inView: r.bottom > 0 && r.top < vh && r.right > 0 && r.left < vw };
    if (tag === 'input' || tag === 'textarea') {
      item.placeholder = (el.getAttribute('placeholder') || '').slice(0, 60);
      item.value = type === 'password' ? (el.value ? '••••' : '') : String(el.value || '').slice(0, 60);
      if (type === 'checkbox' || type === 'radio') item.checked = el.checked;
      item.autocomplete = el.getAttribute('autocomplete') || '';
      item.field = (el.getAttribute('name') || '').slice(0, 40);
    }
    if (tag === 'select') {
      item.value = el.selectedOptions.length ? textOf(el.selectedOptions[0]).slice(0, 60) : '';
      item.options = Array.from(el.options).slice(0, 20).map(o => textOf(o).slice(0, 40));
    }
    if (el.isContentEditable) item.editable = true;
    if (el.disabled) item.disabled = true;
    if (tag === 'a') item.href = (el.getAttribute('href') || '').slice(0, 120);
    out.push(item);
  }
  const d = document.documentElement;
  return { url: location.href, title: document.title, elements: out, total: total,
    scrollY: Math.round(W.scrollY), scrollHeight: Math.round(d.scrollHeight), viewportHeight: vh, viewportWidth: vw };
})(MAX)
"#
    .replace("MAX", &MAX_ELEMENTS.to_string())
    }

    const TEXT_SCRIPT: &str = r#"
(function () {
  const d = document.documentElement;
  return { url: location.href, title: document.title, text: document.body ? document.body.innerText : '',
    scrollY: Math.round(window.scrollY), scrollHeight: Math.round(d.scrollHeight), viewportHeight: window.innerHeight };
})()
"#;

    /// 按编号找元素：滚到视口中间，返回中心点（视口里的 CSS 像素）、是不是被别的东西盖着
    fn locate_script(reference: &str) -> String {
        r#"
(function (ref) {
  const map = window.__edithRefs;
  const weak = map && map.get(ref);
  const el = weak && weak.deref();
  if (!el || !el.isConnected) return { missing: true };
  el.scrollIntoView({ block: 'center', inline: 'center', behavior: 'instant' });
  const r = el.getBoundingClientRect();
  const x = r.left + r.width / 2, y = r.top + r.height / 2;
  const top = document.elementFromPoint(x, y);
  const covered = !!top && top !== el && !el.contains(top) && !top.contains(el);
  return { x: x, y: y, covered: covered, tag: el.tagName.toLowerCase(), type: (el.getAttribute('type') || '').toLowerCase(),
    name: (el.getAttribute('aria-label') || el.innerText || el.value || el.getAttribute('placeholder') || '').replace(/\s+/g, ' ').trim().slice(0, 60) };
})(REF)
"#
    .replace("REF", &quoted(reference))
    }

    /// 下拉框选一项（按选项文字或值）
    fn select_script(reference: &str, option: &str) -> String {
        r#"
(function (ref, wanted) {
  const map = window.__edithRefs;
  const weak = map && map.get(ref);
  const el = weak && weak.deref();
  if (!el || !el.isConnected) return { missing: true };
  if (el.tagName.toLowerCase() !== 'select') return { error: 'not-select' };
  const text = (o) => (o.textContent || '').replace(/\s+/g, ' ').trim();
  let match = Array.from(el.options).find(o => text(o) === wanted || o.value === wanted);
  if (!match) match = Array.from(el.options).find(o => text(o).includes(wanted));
  if (!match) return { error: 'no-option', options: Array.from(el.options).map(text).slice(0, 30) };
  el.value = match.value;
  el.dispatchEvent(new Event('input', { bubbles: true }));
  el.dispatchEvent(new Event('change', { bubbles: true }));
  return { selected: text(match) };
})(REF, WANTED)
"#
        .replace("REF", &quoted(reference))
        .replace("WANTED", &quoted(option))
    }

    const STATE_SCRIPT: &str = r#"
(function () {
  const a = document.activeElement;
  let focus = '';
  if (a && a !== document.body) {
    const ids = window.__edithIds;
    focus = a.tagName.toLowerCase() + (a.getAttribute('type') ? '[' + a.getAttribute('type') + ']' : '') + (ids && ids.get(a) ? ' ' + ids.get(a) : '');
  }
  return { url: location.href, title: document.title, focus: focus };
})()
"#;

    // MARK: - 元素的说明（给模型看）、审批用的缓存

    #[derive(Clone, Default)]
    struct Element {
        kind: String,
        name: String,
        autocomplete: String,
        field: String,
    }

    /// 标签 → 编号 → 元素（最近一次 browser_read elements 列出的）
    static ELEMENTS: LazyLock<Mutex<HashMap<String, HashMap<String, Element>>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

    fn field<'a>(item: &'a Value, key: &str) -> &'a str {
        item.get(key).and_then(Value::as_str).unwrap_or("")
    }

    fn role_name(item: &Value) -> String {
        let (tag, role, kind) = (field(item, "tag"), field(item, "role"), field(item, "type"));
        let roles = [
            ("button", "按钮"),
            ("link", "链接"),
            ("checkbox", "复选框"),
            ("radio", "单选"),
            ("tab", "标签页"),
            ("menuitem", "菜单项"),
            ("option", "选项"),
            ("switch", "开关"),
            ("combobox", "下拉框"),
            ("textbox", "输入框"),
            ("searchbox", "搜索框"),
        ];
        if let Some((_, title)) = roles.iter().find(|(name, _)| *name == role) {
            return title.to_string();
        }
        match tag {
            "a" => "链接".into(),
            "button" | "summary" => "按钮".into(),
            "select" => "下拉框".into(),
            "textarea" => "多行输入框".into(),
            "input" => match kind {
                "checkbox" => "复选框".into(),
                "radio" => "单选".into(),
                "submit" | "button" | "reset" | "image" => "按钮".into(),
                "password" => "密码框".into(),
                "file" => "选文件".into(),
                "" | "text" => "输入框".into(),
                other => format!("输入框（{other}）"),
            },
            _ if item.get("editable").and_then(Value::as_bool) == Some(true) => "可编辑区".into(),
            _ => format!("可点的{tag}"),
        }
    }

    fn describe(item: &Value) -> String {
        let mut line = format!("[{}] {}", field(item, "ref"), role_name(item));
        let name = field(item, "name");
        if !name.is_empty() {
            line += &format!("「{name}」");
        }
        let placeholder = field(item, "placeholder");
        if !placeholder.is_empty() && placeholder != name {
            line += &format!(" 提示「{placeholder}」");
        }
        if !field(item, "value").is_empty() {
            line += &format!(" 现在是「{}」", field(item, "value"));
        }
        if let Some(checked) = item.get("checked").and_then(Value::as_bool) {
            line += if checked { " 已勾" } else { " 没勾" };
        }
        if let Some(options) = item.get("options").and_then(Value::as_array) {
            let options: Vec<&str> = options.iter().filter_map(Value::as_str).collect();
            line += &format!(" 选项：{}", options.join(" / "));
        }
        let href = field(item, "href");
        if !href.is_empty() && !href.starts_with("javascript:") {
            line += &format!(" → {href}");
        }
        if item.get("disabled").and_then(Value::as_bool) == Some(true) {
            line += "（不可用）";
        }
        if item.get("inView").and_then(Value::as_bool) != Some(true) {
            line += "（不在视口里）";
        }
        line
    }

    fn remember(tab: &str, elements: &[Value]) {
        let map = elements
            .iter()
            .map(|item| {
                let element = Element {
                    kind: field(item, "type").into(),
                    name: field(item, "name").into(),
                    autocomplete: field(item, "autocomplete").into(),
                    field: field(item, "field").into(),
                };
                (field(item, "ref").to_string(), element)
            })
            .collect();
        ELEMENTS.lock().unwrap().insert(tab.to_string(), map);
    }

    // MARK: - 标签、等待

    /// 工具要操作的标签：tab 参数（编号前几位、标题里的字、第几个）或当前标签；没有标签时 create 为真就开一个空白的
    fn resolve(session: &str, arguments: &Map<String, Value>, create: bool) -> Result<String, String> {
        let tabs = browser::tabs_of(session);
        if let Some(wanted) = text_arg(arguments, "tab") {
            if let Ok(index) = wanted.parse::<usize>()
                && (1..=tabs.len()).contains(&index)
            {
                return Ok(tabs[index - 1].id.clone());
            }
            let lower = wanted.to_lowercase();
            return tabs
                .iter()
                .find(|tab| tab.id.to_lowercase().starts_with(&lower) || tab.title.to_lowercase().contains(&lower))
                .map(|tab| tab.id.clone())
                .ok_or_else(|| format!("没有叫「{wanted}」的标签；用 browser_tabs 看这段对话的标签"));
        }
        if let Some(current) = browser::current_tab(session) {
            browser::wake_now(&current)?;
            return Ok(current);
        }
        if !create {
            return Err("这段对话还没有打开网页：先用 browser_navigate 打开".into());
        }
        browser::open_for_agent(session, "")
    }

    /// 等页面加载完再稳一会儿（最多 limit；加载慢的不算错，结果里说一声）
    fn wait_for_load(tab: &str, limit: Duration) -> bool {
        let start = Instant::now();
        std::thread::sleep(Duration::from_millis(150));
        let mut quiet = 0;
        while start.elapsed() < limit {
            let Some(info) = browser::info(tab) else { return true };
            quiet = if info.loading { 0 } else { quiet + 1 };
            if quiet >= 4 {
                return true;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        false
    }

    /// 用户在卡片里动着：等一会儿（最多 10 秒），还在动就让模型稍后再来
    fn yield_to_user(tab: &str) -> Result<(), String> {
        let start = Instant::now();
        while browser::user_active(tab) {
            if start.elapsed() > Duration::from_secs(10) {
                return Err("用户正在操作浏览器，过一会儿再试（或者先问问用户在做什么）".into());
            }
            std::thread::sleep(Duration::from_millis(250));
        }
        Ok(())
    }

    /// 动作之后附上的现状：网址、标题、焦点、没回答的对话框、刚下载完的文件
    fn after_action(tab: &str, url_before: &str) -> String {
        let mut text = String::new();
        if let Ok(state) = browser::evaluate(tab, STATE_SCRIPT, true) {
            let url = field(&state, "url");
            if !url_before.is_empty() && url != url_before {
                text += &format!("\n网址变了：{url}");
            }
            text += &format!("\n标题：{}", field(&state, "title"));
            if !field(&state, "focus").is_empty() {
                text += &format!("\n焦点在：{}", field(&state, "focus"));
            }
        }
        let dialog = browser::pending_dialog(tab);
        if !dialog.is_empty() {
            text += &format!("\n网页弹了对话框，还没回答：{dialog}（用 browser_act 的 dialog 回答）");
        }
        for path in browser::take_downloads(tab) {
            text += &format!("\n下载完了：{path}");
        }
        text
    }

    /// 动作的目标：ref（先滚到它）或 x、y
    fn target(tab: &str, arguments: &Map<String, Value>, required: bool) -> Result<Option<(f64, f64, String)>, String> {
        if let Some(reference) = text_arg(arguments, "ref") {
            let located = browser::evaluate(tab, &locate_script(&reference), true)?;
            if located.get("missing").and_then(Value::as_bool) == Some(true) {
                return Err(format!("找不到元素 {reference}（页面变了）：先用 browser_read 重新看元素"));
            }
            std::thread::sleep(Duration::from_millis(120));
            let mut what = format!("[{reference}]");
            if !field(&located, "name").is_empty() {
                what += &format!("「{}」", field(&located, "name"));
            }
            if located.get("covered").and_then(Value::as_bool) == Some(true) {
                what += "（它被别的东西盖着，点到的可能是盖在上面的那个：弹窗、遮罩先关掉）";
            }
            return Ok(Some((located["x"].as_f64().unwrap_or(0.0), located["y"].as_f64().unwrap_or(0.0), what)));
        }
        if let (Some(x), Some(y)) = (number_arg(arguments, "x"), number_arg(arguments, "y")) {
            return Ok(Some((x, y, format!("({}, {})", x as i64, y as i64))));
        }
        if required {
            return Err("要给 ref（browser_read elements 列出的编号）或 x、y（视口里的坐标）".into());
        }
        Ok(None)
    }

    // MARK: - 各个工具

    fn navigate(session: &str, arguments: &Map<String, Value>) -> Result<String, String> {
        let url = text_arg(arguments, "url");
        let action = text_arg(arguments, "action");
        let keep = bool_arg(arguments, "keep", false);
        if url.is_none() && action.is_none() {
            // 只给 keep：把已经开着的标签留给用户（这一轮结束时不关）
            if keep {
                let tab = resolve(session, arguments, false)?;
                browser::keep(&tab);
                return Ok(format!("标签 {} 留给用户，这一轮结束时不关。", short(&tab)));
            }
            return Err("要给 url（要打开的网址），或 action（back / forward / reload）".into());
        }
        let tab = if let (Some(url), true) = (&url, bool_arg(arguments, "new_tab", false)) {
            browser::open_for_agent(session, url)?
        } else {
            let tab = resolve(session, arguments, true)?;
            yield_to_user(&tab)?;
            match (&url, &action) {
                (Some(url), _) => browser::load(&tab, url),
                (None, Some(action)) => browser::navigate_action(&tab, &action.to_lowercase())?,
                _ => {}
            }
            tab
        };
        let loaded = wait_for_load(&tab, Duration::from_secs(30));
        let page = browser::evaluate(&tab, TEXT_SCRIPT, true).unwrap_or(Value::Null);
        let url_now = if field(&page, "url").is_empty() {
            browser::info(&tab).map(|info| info.url).unwrap_or_default()
        } else {
            field(&page, "url").to_string()
        };
        let mut text = format!("标签 {}：{}\n网址：{url_now}", short(&tab), field(&page, "title"));
        if !loaded {
            text += "\n（30 秒还没加载完，下面是现在的样子）";
        }
        let body = field(&page, "text").trim();
        text += &format!("\n\n页面开头的文字：\n{}", if body.is_empty() { "（没有文字）".to_string() } else { clip(body, 2000) });
        text += "\n\n要点、要填就先用 browser_read（mode=elements）看可交互的元素和编号。";
        if keep {
            browser::keep(&tab);
        }
        if browser::closes_at_turn_end(session, &tab) {
            text += "\n这个标签是你开的，这一轮结束时自动关掉；用户要看、要接着用的带上 keep=true（只给 tab 和 keep 也行）。";
        }
        let after = after_action(&tab, "");
        // 标题上面已经有了
        let after: Vec<&str> = after.lines().filter(|line| !line.starts_with("标题：")).collect();
        text += &after.join("\n");
        Ok(text)
    }

    fn screens(scroll: i64, height: i64, viewport: i64) -> (i64, i64) {
        let viewport = viewport.max(1);
        (scroll / viewport + 1, ((height + viewport - 1) / viewport).max(1))
    }

    fn read(session: &str, arguments: &Map<String, Value>) -> Result<String, String> {
        let tab = resolve(session, arguments, false)?;
        wait_for_load(&tab, Duration::from_secs(8));
        let mode = text_arg(arguments, "mode").unwrap_or_else(|| "text".into()).to_lowercase();
        let number = |value: &Value, key: &str| value.get(key).and_then(Value::as_i64).unwrap_or(0);
        if mode == "elements" {
            let object = browser::evaluate(&tab, &elements_script(), true)?;
            let elements = object["elements"].as_array().cloned().unwrap_or_default();
            remember(&tab, &elements);
            let (screen, total_screens) =
                screens(number(&object, "scrollY"), number(&object, "scrollHeight"), number(&object, "viewportHeight"));
            let mut text = format!(
                "标签 {}：{}\n网址：{}\n视口 {}×{}，滚到第 {screen} 屏（共 {total_screens} 屏）\n",
                short(&tab),
                field(&object, "title"),
                field(&object, "url"),
                number(&object, "viewportWidth"),
                number(&object, "viewportHeight")
            );
            let total = number(&object, "total") as usize;
            let partial = if total > elements.len() { format!("（只列前 {} 个）", elements.len()) } else { String::new() };
            text += &format!("可交互的元素 {total} 个{partial}：\n");
            let lines: Vec<String> = elements.iter().map(describe).collect();
            text += &if lines.is_empty() { "（没有）".to_string() } else { lines.join("\n") };
            text += "\n\n用 browser_act 的 ref 填编号来点、输入；编号在这个页面里一直有效，页面换了再读一次。";
            let dialog = browser::pending_dialog(&tab);
            if !dialog.is_empty() {
                text += &format!("\n网页弹了对话框，还没回答：{dialog}");
            }
            return Ok(clip(&text, MAX_TEXT));
        }
        let object = browser::evaluate(&tab, TEXT_SCRIPT, true)?;
        let (screen, total_screens) =
            screens(number(&object, "scrollY"), number(&object, "scrollHeight"), number(&object, "viewportHeight"));
        let mut text = format!(
            "标签 {}：{}\n网址：{}\n页面共 {total_screens} 屏，现在在第 {screen} 屏\n\n",
            short(&tab),
            field(&object, "title"),
            field(&object, "url")
        );
        let body = field(&object, "text").trim();
        text += if body.is_empty() { "（页面上没有文字）" } else { body };
        Ok(clip(&text, MAX_TEXT))
    }

    fn act(session: &str, arguments: &Map<String, Value>) -> Result<String, String> {
        let action = text_arg(arguments, "action").unwrap_or_default().to_lowercase();
        if action.is_empty() {
            return Err("要给 action：click / double_click / right_click / type / key / scroll / hover / select / dialog".into());
        }
        let tab = resolve(session, arguments, false)?;
        if action == "dialog" {
            let accept = bool_arg(arguments, "accept", true);
            let text = arguments.get("text").and_then(Value::as_str).unwrap_or("");
            if !browser::answer_dialog(&tab, accept, text)? {
                return Err("这个标签没有等着回答的对话框".into());
            }
            wait_for_load(&tab, Duration::from_secs(5));
            return Ok(format!("{}{}", if accept { "已确认对话框" } else { "已取消对话框" }, after_action(&tab, "")));
        }
        yield_to_user(&tab)?;
        let url_before = browser::info(&tab).map(|info| info.url).unwrap_or_default();
        let done = match action.as_str() {
            "click" | "double_click" | "right_click" => {
                let (x, y, what) = target(&tab, arguments, true)?.unwrap_or_default();
                let button = if action == "right_click" { "right" } else { "left" };
                browser::click(&tab, x, y, button, if action == "double_click" { 2 } else { 1 })?;
                let verb = match action.as_str() {
                    "click" => "点击",
                    "double_click" => "双击",
                    _ => "右键点了",
                };
                format!("已{verb} {what}")
            }
            "type" => {
                let text = arguments.get("text").and_then(Value::as_str).ok_or("type 要给 text")?;
                let point = target(&tab, arguments, false)?;
                if let Some((x, y, _)) = &point {
                    browser::click(&tab, *x, *y, "left", 1)?;
                    std::thread::sleep(Duration::from_millis(80));
                }
                if !bool_arg(arguments, "append", false) {
                    browser::press_key(&tab, "Ctrl+A")?;
                    browser::press_key(&tab, "Backspace")?;
                }
                browser::insert_text(&tab, text)?;
                let place = point.map(|(_, _, what)| what).unwrap_or_else(|| "焦点处".into());
                let mut done = format!("已在 {place} 输入 {} 个字", text.chars().count());
                if bool_arg(arguments, "submit", false) {
                    std::thread::sleep(Duration::from_millis(80));
                    browser::press_key(&tab, "Enter")?;
                    done += "，按了回车";
                }
                done
            }
            "key" => {
                let key = text_arg(arguments, "key").ok_or("key 要给按键，如 Enter、Tab、Escape、ArrowDown、Ctrl+A")?;
                if !browser::press_key(&tab, &key)? {
                    return Err(format!("认不出按键「{key}」"));
                }
                format!("已按 {key}")
            }
            "scroll" => {
                let direction = text_arg(arguments, "direction").unwrap_or_else(|| "down".into()).to_lowercase();
                let amount = number_arg(arguments, "amount").unwrap_or(5.0).clamp(1.0, 50.0) as i64;
                if text_arg(arguments, "ref").is_some() {
                    let (_, _, what) = target(&tab, arguments, true)?.unwrap_or_default();
                    format!("已滚到 {what}")
                } else {
                    let (width, height) = browser::viewport_of(&tab);
                    let (x, y) =
                        target(&tab, arguments, false)?.map(|(x, y, _)| (x, y)).unwrap_or((width as f64 / 2.0, height as f64 / 2.0));
                    // CDP 的滚轮向下、向右为正
                    let step = 120.0 * amount as f64;
                    let delta = match direction.as_str() {
                        "up" => (0.0, -step),
                        "left" => (-step, 0.0),
                        "right" => (step, 0.0),
                        _ => (0.0, step),
                    };
                    browser::mouse(&tab, "mouseWheel", x, y, "none", 0, delta)?;
                    let word = match direction.as_str() {
                        "up" => "上",
                        "left" => "左",
                        "right" => "右",
                        _ => "下",
                    };
                    format!("已往{word}滚了 {amount} 格")
                }
            }
            "hover" => {
                let (x, y, what) = target(&tab, arguments, true)?.unwrap_or_default();
                browser::mouse(&tab, "mouseMoved", x, y, "none", 0, (0.0, 0.0))?;
                format!("鼠标移到了 {what}")
            }
            "select" => {
                let (Some(reference), Some(option)) = (text_arg(arguments, "ref"), arguments.get("text").and_then(Value::as_str)) else {
                    return Err("select 要给 ref（下拉框）和 text（选项的文字或值）".into());
                };
                let result = browser::evaluate(&tab, &select_script(&reference, option), true)?;
                if result.get("missing").and_then(Value::as_bool) == Some(true) {
                    return Err(format!("找不到元素 {reference}：先用 browser_read 重新看元素"));
                }
                match field(&result, "error") {
                    "not-select" => return Err(format!("{reference} 不是下拉框（<select>）：自绘的下拉框先 click 打开再点选项")),
                    "no-option" => {
                        let options: Vec<&str> = result["options"].as_array().into_iter().flatten().filter_map(Value::as_str).collect();
                        return Err(format!("没有「{option}」这一项；有：{}", options.join(" / ")));
                    }
                    _ => {}
                }
                format!("已在 [{reference}] 选了「{}」", field(&result, "selected"))
            }
            other => return Err(format!("不认识的 action「{other}」")),
        };
        // 点击、回车可能引起导航：稍等再看
        std::thread::sleep(Duration::from_millis(300));
        wait_for_load(&tab, Duration::from_secs(10));
        Ok(done + &after_action(&tab, &url_before))
    }

    fn screenshot(session: &str, arguments: &Map<String, Value>) -> Result<String, String> {
        let tab = resolve(session, arguments, false)?;
        wait_for_load(&tab, Duration::from_secs(8));
        std::thread::sleep(Duration::from_millis(200));
        let bytes = browser::screenshot(&tab)?;
        let dir = crate::serve_dir().join("browser").join("shots");
        std::fs::create_dir_all(&dir).map_err(|error| format!("存不了截图：{error}"))?;
        let path = dir.join(format!("{}-{}.jpg", short(&tab), chrono::Local::now().format("%Y%m%d-%H%M%S%3f")));
        std::fs::write(&path, &bytes).map_err(|error| format!("存不了截图：{error}"))?;
        let (width, height) = browser::viewport_of(&tab);
        let title = browser::info(&tab).map(|info| info.title).unwrap_or_default();
        let mut text = format!(
            "截了标签 {}「{title}」的视口（{width}×{height}，坐标就是 browser_act 的 x、y），存在 {}。",
            short(&tab),
            path.display()
        );
        IMAGE.with(|image| *image.borrow_mut() = Some(path.to_string_lossy().into_owned()));
        text += "截图跟在后面（这个模型看不了图的话只有这句：用 browser_read 读页面文字和元素）。";
        Ok(text)
    }

    fn resize(session: &str, arguments: &Map<String, Value>) -> Result<String, String> {
        let tab = resolve(session, arguments, true)?;
        if bool_arg(arguments, "fit", false) {
            browser::set_fixed(&tab, None)?;
            std::thread::sleep(Duration::from_millis(300));
            let (width, height) = browser::viewport_of(&tab);
            return Ok(format!("视口改回跟随卡片，现在是 {width}×{height}"));
        }
        let (Some(width), Some(height)) = (number_arg(arguments, "width"), number_arg(arguments, "height")) else {
            return Err("要给 width、height（CSS 像素），或 fit=true 回到跟随卡片".into());
        };
        let size = ((width as u32).clamp(320, 3840), (height as u32).clamp(240, 2160));
        browser::set_fixed(&tab, Some(size))?;
        std::thread::sleep(Duration::from_millis(300));
        Ok(format!("视口改成了 {}×{}（网页按这个大小排版；卡片大小不变，卡片里按比例缩小显示）", size.0, size.1))
    }

    fn tabs(session: &str, arguments: &Map<String, Value>) -> Result<String, String> {
        let action = text_arg(arguments, "action").unwrap_or_else(|| "list".into()).to_lowercase();
        match action.as_str() {
            "select" | "close" => {
                if text_arg(arguments, "tab").is_none() {
                    return Err(format!("{action} 要给 tab（编号、标题里的字或第几个）"));
                }
                let tab = resolve(session, arguments, false)?;
                if action == "select" {
                    browser::select(&tab)?;
                } else {
                    browser::close_tab(&tab)?;
                }
            }
            "list" => {}
            _ => return Err("action 是 list / select / close（开新标签用 browser_navigate 的 new_tab）".into()),
        }
        let list = browser::tabs_of(session);
        if list.is_empty() {
            return Ok("这段对话没有打开的网页".into());
        }
        let lines: Vec<String> = list
            .iter()
            .enumerate()
            .map(|(index, tab)| {
                format!("{}. {}「{}」{}{}", index + 1, short(&tab.id), tab.title, tab.url, if tab.current { "（当前）" } else { "" })
            })
            .collect();
        Ok(format!("这段对话的标签：\n{}", lines.join("\n")))
    }

    fn hand_over(session: &str, arguments: &Map<String, Value>) -> Result<String, String> {
        let reason = text_arg(arguments, "reason").ok_or("要给 reason：请用户在浏览器里做什么（例如「登录 GitHub」「完成人机验证」）")?;
        let tab = resolve(session, arguments, false)?;
        browser::hand_over(&tab, &reason)?;
        Ok(format!("已打开浏览器卡片、请用户「{reason}」。现在结束这一轮、告诉用户要做什么；用户点「好了」后会发来消息，再接着做。"))
    }

    fn run_js(session: &str, arguments: &Map<String, Value>) -> Result<String, String> {
        let script = arguments.get("script").and_then(Value::as_str).filter(|script| !script.trim().is_empty()).ok_or("要给 script")?;
        let tab = resolve(session, arguments, false)?;
        yield_to_user(&tab)?;
        let value = browser::evaluate(&tab, script, false)?;
        let text = match &value {
            Value::Null => "（脚本没有返回值）".to_string(),
            Value::String(text) => text.clone(),
            other => other.to_string(),
        };
        Ok(clip(&text, MAX_TEXT) + &after_action(&tab, ""))
    }

    thread_local! {
        /// 这一步截的图（随结果的 media 交给大脑）
        static IMAGE: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
    }

    /// 跑一个浏览器工具（不是浏览器工具回 None）
    pub fn run(name: &str, arguments: &Map<String, Value>, session: &str) -> Option<Outcome> {
        if !is_tool(name) {
            return None;
        }
        if !browser::available() {
            return Some(Outcome {
                ok: false,
                text: "这台机器上没有 Chrome / Chromium / Edge，用不了浏览器工具（看网页用 fetch_url）".into(),
                image: None,
            });
        }
        if session.is_empty() {
            return Some(Outcome {
                ok: false, text: "这次调用不属于任何一段会话，没有浏览器可用".into(), image: None
            });
        }
        IMAGE.with(|image| image.borrow_mut().take());
        let outcome = match name {
            "browser_navigate" => navigate(session, arguments),
            "browser_read" => read(session, arguments),
            "browser_act" => act(session, arguments),
            "browser_screenshot" => screenshot(session, arguments),
            "browser_resize" => resize(session, arguments),
            "browser_tabs" => tabs(session, arguments),
            "browser_hand_over" => hand_over(session, arguments),
            _ => run_js(session, arguments),
        };
        let image = IMAGE.with(|image| image.borrow_mut().take());
        Some(match outcome {
            Ok(text) => Outcome { ok: true, text, image },
            Err(text) => Outcome { ok: false, text, image },
        })
    }

    fn host_of(url: &str) -> Option<(String, u16)> {
        let (scheme, rest) = url.split_once("://")?;
        let authority = rest.split(['/', '?', '#']).next()?;
        let authority = authority.rsplit_once('@').map_or(authority, |(_, host)| host);
        let default = if scheme.eq_ignore_ascii_case("https") { 443 } else { 80 };
        if let Some(rest) = authority.strip_prefix('[') {
            let (host, port) = rest.split_once(']')?;
            return Some((host.to_string(), port.trim_start_matches(':').parse().unwrap_or(default)));
        }
        match authority.rsplit_once(':') {
            Some((host, port)) => Some((host.to_lowercase(), port.parse().unwrap_or(default))),
            None => Some((authority.to_lowercase(), default)),
        }
    }

    /// 本机、内网、链路本地（同 EDITH 的 approval::private_address）
    fn private_address(address: &std::net::IpAddr) -> bool {
        use std::net::IpAddr;
        match address {
            IpAddr::V4(v4) => {
                v4.is_loopback()
                    || v4.is_private()
                    || v4.is_link_local()
                    || v4.is_unspecified()
                    || v4.is_broadcast()
                    || v4.octets()[0] == 100 && (64..128).contains(&v4.octets()[1])
                    || v4.octets()[0] == 0
            }
            IpAddr::V6(v6) => {
                let first = v6.segments()[0];
                v6.is_loopback()
                    || v6.is_unspecified()
                    || (first & 0xfe00) == 0xfc00
                    || (first & 0xffc0) == 0xfe80
                    || v6.to_ipv4_mapped().is_some_and(|v4| private_address(&IpAddr::V4(v4)))
            }
        }
    }

    fn private_url(url: &str) -> bool {
        use std::net::ToSocketAddrs;
        let Some((host, port)) = host_of(url) else { return false };
        if host == "localhost" || host.ends_with(".localhost") || host.ends_with(".local") || host.ends_with(".internal") {
            return true;
        }
        (host.as_str(), port)
            .to_socket_addrs()
            .map(|mut addresses| addresses.any(|address| private_address(&address.ip())))
            .unwrap_or(false)
    }

    fn clip_line(text: &str, limit: usize) -> String {
        if text.chars().count() > limit { format!("{}…", text.chars().take(limit).collect::<String>()) } else { text.to_string() }
    }

    /// 这个动作要不要批准、怎么问（PreToolUse 钩子回的 need，EDITH 照它问；协议 §6.7）：打开新网站按网站问（本机 / 内网另说）；
    /// 点、输入、跑脚本问一声（答「都行」这个会话里就不再问），点付款登录、往密码框和卡号输入自动模式下也问；看、截图、滚动不问。
    /// tool 是大脑里的工具名（friday__browser_navigate），答「都行」记的规则用它
    pub fn need(tool: &str, action: &str, arguments: &Map<String, Value>, session: &str) -> Value {
        let text = |key: &str| arguments.get(key).and_then(Value::as_str).unwrap_or("").to_string();
        match action {
            "browser_navigate" => {
                let url = browser::normalize(&text("url"));
                if url.is_empty() {
                    return json!(false);
                }
                let host = host_of(&url).map(|(host, _)| host).unwrap_or_else(|| url.clone());
                if private_url(&url) {
                    return json!({ "target": host, "summary": format!("在浏览器里打开本机 / 内网地址：{}", clip_line(&url, 300)),
                    "rule": format!("{tool}({host})"), "privateNetwork": true });
                }
                json!({ "target": host, "summary": format!("在浏览器里打开：{}", clip_line(&url, 300)), "rule": format!("{tool}({host})") })
            }
            "browser_act" => {
                let kind = text("action").to_lowercase();
                if matches!(kind.as_str(), "" | "hover" | "scroll" | "dialog") {
                    return json!(false);
                }
                let what =
                    [text("ref"), text("text"), text("key")].into_iter().filter(|part| !part.is_empty()).collect::<Vec<_>>().join(" ");
                let mut need = json!({ "target": what, "summary": format!("在网页里 {kind}：{}", clip_line(&what, 200)), "rule": tool });
                if let Some(stop) = needs_stop(arguments, session) {
                    need["stop"] = json!(true);
                    need["summary"] = json!(format!("{}（{stop}）", need["summary"].as_str().unwrap_or("")));
                }
                need
            }
            "browser_run_js" => {
                let script = text("script");
                json!({ "target": clip_line(&script, 200), "summary": format!("在网页里跑脚本：{}", clip_line(&script, 300)), "rule": tool })
            }
            _ => json!(false),
        }
    }

    /// 审批：这一下点 / 输入的是付款、登录、密码、卡号这类（同 Friday 的 stopFor）——什么模式下都要用户确认
    pub fn needs_stop(arguments: &Map<String, Value>, session: &str) -> Option<&'static str> {
        let action = text_arg(arguments, "action").unwrap_or_default().to_lowercase();
        let reference = text_arg(arguments, "ref")?;
        let tab = match text_arg(arguments, "tab") {
            Some(wanted) => {
                let lower = wanted.to_lowercase();
                browser::tabs_of(session)
                    .into_iter()
                    .find(|tab| tab.id.to_lowercase().starts_with(&lower) || tab.title.to_lowercase().contains(&lower))?
                    .id
            }
            None => browser::current_tab(session)?,
        };
        let element = ELEMENTS.lock().unwrap().get(&tab)?.get(&reference)?.clone();
        let has = |text: &str, words: &[&str]| {
            let lower = text.to_lowercase();
            words.iter().any(|word| lower.contains(word))
        };
        let money = [
            "支付",
            "付款",
            "付钱",
            "下单",
            "购买",
            "结算",
            "立即购买",
            "确认订单",
            "充值",
            "pay",
            "checkout",
            "place order",
            "buy now",
            "purchase",
            "subscribe",
        ];
        let account = ["登录", "登入", "注册", "sign in", "log in", "login", "sign up", "register", "create account"];
        let card = ["cc-", "card", "卡号", "cvv", "cvc"];
        match action.as_str() {
            "type" if element.kind == "password" => Some("在密码框里输入"),
            "type" if has(&format!("{} {} {}", element.autocomplete, element.field, element.name), &card) => Some("填卡号这类付款信息"),
            "click" | "double_click" if has(&element.name, &money) => Some("点付款、下单这类按钮"),
            "click" | "double_click" if has(&element.name, &account) => Some("点登录、注册这类按钮"),
            _ => None,
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn describes_elements_like_friday() {
            let item = json!({ "ref": "e3", "tag": "input", "type": "password", "name": "密码", "placeholder": "", "value": "••••", "inView": true });
            assert_eq!(describe(&item), "[e3] 密码框「密码」 现在是「••••」");
            let link = json!({ "ref": "e1", "tag": "a", "name": "首页", "href": "/home", "inView": false });
            assert_eq!(describe(&link), "[e1] 链接「首页」 → /home（不在视口里）");
            assert_eq!(screens(1600, 4000, 800), (3, 5));
            assert!(elements_script().contains("(300)"));
            assert!(locate_script("e\"1").contains(r#""e\"1""#));
        }
    }
}

fn main() {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    if arguments.iter().any(|argument| argument == "--version" || argument == "-V") {
        println!("edith-friday {VERSION}");
        return;
    }
    // 常驻的那份（网关拉起它，整台机器一个）
    if arguments.first().map(String::as_str) == Some("serve") {
        if let Err(message) = serve() {
            log(&message);
            std::process::exit(1);
        }
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
