//! edith-connect：共享设备（docs/DEVICES.md）。麦克风、摄像头、串口、机器人这类同一时间只能被一个进程打开的设备，
//! 整台机器只起一个**设备服务**持有它，它连到每个要用这个设备的大脑的套接字（大脑唯一的接口，edith 仓库的 docs/CAPABILITY-PROTOCOL.md §1），
//! 在每个大脑那里都是一个一级能力。不用大脑的代码。
//!
//! - `edith-connect <设备名>`（启动器）：大脑按 capabilities.json 起它（`{"name": "mic", "command": "edith-connect", "args": ["mic"]}`），
//!   给它 EDITH_ACCESS / EDITH_ACCESS_TOKEN / EDITH_CAPABILITY；它确认这个设备的服务在跑（`~/.edith/devices/<设备名>.json`，没起就拉起），
//!   告诉服务「去连这个大脑、名字叫 EDITH_CAPABILITY」，然后退出。服务那条连接断了，大脑过一会儿重起它，它再让服务连。
//! - `edith-connect --hub <设备名>`（设备服务，一般由启动器拉起）：按 `~/.edith/devices.json` 起设备程序——一个普通的、
//!   只管一条连接的能力，经 stdio 和设备服务讲协议——自己当它的「大脑」；对每个大脑各握一次手、报设备的感知和动作（外加 claim），并仲裁：
//!   - 当前大脑：设备主动推的（sense、ask、sessions.*）只交给它。第一个连上的是；调了设备动作的、调了 claim 的成为当前大脑；
//!     它断开了换最近连上的那个。设备订的事件只转当前大脑的。
//!   - 设备忙：动作标了 exclusive 的，做的时候别的大脑调设备的动作回 -32005；标了 holds 的，成功后这个大脑一直占着设备，
//!     直到它调标了 releases 的动作或断开。回话里写明被谁占着（error.data.holder = {id, workspace}）。
//!   - 设备程序收到的 act 里多一个 brain（{id, workspace}），知道是谁要它做的。
//!   - 没有大脑连着 idleSeconds（默认 900）秒就退出；设备程序退出了它也退出（各个大脑那条连接断开、过一会儿重起启动器）。

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

const VERSION: &str = env!("CARGO_PKG_VERSION");
const BUSY: i64 = -32005;

fn home() -> PathBuf {
    match std::env::var_os("EDITH_HOME") {
        Some(dir) if !dir.is_empty() => PathBuf::from(dir),
        _ => {
            let user =
                std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE")).map(PathBuf::from).unwrap_or_else(|| ".".into());
            user.join(".edith")
        }
    }
}

fn devices_dir() -> PathBuf {
    home().join("devices")
}

fn registry_path(name: &str) -> PathBuf {
    devices_dir().join(format!("{name}.json"))
}

fn read_json(path: &std::path::Path) -> Option<Value> {
    serde_json::from_slice(&std::fs::read(path).ok()?).ok()
}

fn main() {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    let result = match arguments.iter().map(String::as_str).collect::<Vec<_>>().as_slice() {
        ["-V" | "--version"] => {
            println!("edith-connect {VERSION}");
            return;
        }
        ["--hub", name] => hub(name),
        [name] if !name.starts_with('-') => launch(name),
        _ => Err("用法：edith-connect <设备名>（启动器，大脑起它）；edith-connect --hub <设备名>（设备服务）".into()),
    };
    if let Err(message) = result {
        let _ = writeln!(std::io::stderr(), "edith-connect：{message}");
        std::process::exit(1);
    }
}

// MARK: - 启动器

/// 登记的设备服务还活着
fn alive(entry: &Value) -> bool {
    let Some(address) = entry["address"].as_str().and_then(|address| address.parse().ok()) else { return false };
    TcpStream::connect_timeout(&address, Duration::from_millis(500)).is_ok()
}

/// 启动器：确认设备服务在跑，让它连到起我的那个大脑，然后退出
fn launch(name: &str) -> Result<(), String> {
    let address = std::env::var("EDITH_ACCESS").ok().filter(|address| !address.is_empty()).ok_or("没有 EDITH_ACCESS：要由大脑起它")?;
    let token = std::env::var("EDITH_ACCESS_TOKEN").unwrap_or_default();
    let capability = std::env::var("EDITH_CAPABILITY").ok().filter(|capability| !capability.is_empty()).unwrap_or_else(|| name.to_string());
    let entry = match read_json(&registry_path(name)).filter(alive) {
        Some(entry) => entry,
        None => {
            spawn_hub(name)?;
            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                if let Some(entry) = read_json(&registry_path(name)).filter(alive) {
                    break entry;
                }
                if Instant::now() >= deadline {
                    return Err(format!("设备服务「{name}」没起来（日志在 {}）", devices_dir().join(format!("{name}.log")).display()));
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    };
    let mut stream = TcpStream::connect(entry["address"].as_str().unwrap_or("")).map_err(|error| format!("连不上设备服务：{error}"))?;
    writeln!(stream, "{}", json!({ "jsonrpc": "2.0", "method": "auth", "params": { "token": entry["token"] } }))
        .map_err(|error| error.to_string())?;
    let attach =
        json!({ "jsonrpc": "2.0", "id": 1, "method": "attach", "params": { "address": address, "token": token, "name": capability } });
    writeln!(stream, "{attach}").map_err(|error| error.to_string())?;
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line).map_err(|error| error.to_string())?;
    let reply: Value = serde_json::from_str(&line).map_err(|_| "设备服务没回话".to_string())?;
    if let Some(error) = reply["error"]["message"].as_str() {
        return Err(error.to_string());
    }
    Ok(())
}

/// 在后台起设备服务（脱离启动器：启动器退了它还在）
fn spawn_hub(name: &str) -> Result<(), String> {
    let exe = std::env::current_exe().map_err(|error| error.to_string())?;
    let mut process = Command::new(exe);
    process.args(["--hub", name]).stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null());
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
    process.spawn().map(|_| ()).map_err(|error| format!("起不了设备服务：{error}"))
}

// MARK: - 设备服务

struct Brain {
    writer: Mutex<TcpStream>,
    /// 在这个大脑那里叫什么（启动器给的 EDITH_CAPABILITY）
    name: String,
    /// 这个大脑的套接字地址（同一个大脑不连两次）
    address: String,
    /// initialize 里的 brain（{id, workspace, …}）
    info: Mutex<Value>,
    ready: Mutex<bool>,
}

impl Brain {
    fn send(&self, message: &Value) {
        let mut writer = self.writer.lock().unwrap();
        let _ = writeln!(writer, "{message}");
        let _ = writer.flush();
    }

    fn identity(&self) -> Value {
        let info = self.info.lock().unwrap();
        json!({ "id": info["id"], "workspace": info["workspace"] })
    }
}

/// 转给设备的一个动作：谁要的、它原来的编号、动作的标记
struct Forward {
    brain: u64,
    id: Value,
    exclusive: bool,
    holds: bool,
    releases: bool,
}

struct Hub {
    name: String,
    device: Mutex<ChildStdin>,
    /// 设备 initialize 回的（报给大脑时加上 claim）
    info: Value,
    brains: Mutex<BTreeMap<u64, Arc<Brain>>>,
    current: Mutex<Option<u64>>,
    /// holds 占着设备的大脑
    holder: Mutex<Option<u64>>,
    /// exclusive 动作正在做的大脑
    running: Mutex<Option<u64>>,
    /// 转给设备的请求：我们的编号 → Forward
    to_device: Mutex<HashMap<u64, Forward>>,
    /// 转给大脑的设备请求：我们的编号 → (大脑, 设备原来的编号)
    to_brain: Mutex<HashMap<u64, (u64, Value)>>,
    next: AtomicU64,
    next_brain: AtomicU64,
    log: Mutex<File>,
    idle_since: Mutex<Option<Instant>>,
}

impl Hub {
    fn log(&self, text: &str) {
        let _ = writeln!(self.log.lock().unwrap(), "{} {text}", chrono::Local::now().format("%Y-%m-%d %H:%M:%S"));
    }

    fn to_device(&self, message: &Value) {
        let mut device = self.device.lock().unwrap();
        let _ = writeln!(device, "{message}");
        let _ = device.flush();
    }

    fn brain(&self, id: u64) -> Option<Arc<Brain>> {
        self.brains.lock().unwrap().get(&id).cloned()
    }

    fn current_brain(&self) -> Option<(u64, Arc<Brain>)> {
        let current = (*self.current.lock().unwrap())?;
        self.brain(current).map(|brain| (current, brain))
    }

    fn action_flags(&self, action: &str) -> (bool, bool, bool) {
        let found = self.info["actions"].as_array().into_iter().flatten().find(|item| item["name"] == action);
        let flag = |key: &str| found.and_then(|item| item[key].as_bool()).unwrap_or(false);
        (flag("exclusive"), flag("holds"), flag("releases"))
    }

    /// 报给大脑的 initialize 回话：设备的感知、动作（加上 claim）、订阅、入口
    fn info_for_brain(&self, name: &str) -> Value {
        let mut info = self.info.clone();
        info["name"] = json!(name);
        let mut actions = info["actions"].as_array().cloned().unwrap_or_default();
        actions.push(json!({
            "name": "claim",
            "description": "让这个共享设备之后主动传来的内容都交给我（几个大脑共用这个设备时用；调过设备别的动作也会成为它的当前大脑）",
            "parameters": { "type": "object", "properties": {} },
        }));
        info["actions"] = Value::Array(actions);
        if let Some(capability) = info.get_mut("capability").and_then(Value::as_object_mut) {
            let description = capability.get("description").and_then(Value::as_str).unwrap_or("").to_string();
            capability.insert(
                "description".into(),
                json!(format!("{description}（共享设备：几个大脑共用，经 edith-connect）").trim_start().to_string()),
            );
        }
        info
    }

    fn busy(&self, id: &Value, holder: u64) -> Value {
        let who = self.brain(holder).map(|brain| brain.identity()).unwrap_or(Value::Null);
        let workspace = who["workspace"].as_str().unwrap_or("另一个目录").to_string();
        json!({ "jsonrpc": "2.0", "id": id, "error": {
            "code": BUSY,
            "message": format!("设备「{}」正被 {workspace} 的大脑占着", self.name),
            "data": { "holder": who },
        }})
    }

    /// 大脑发来的一条
    fn handle_brain(&self, brain_id: u64, brain: &Arc<Brain>, message: &Value) {
        let method = message["method"].as_str();
        let id = message.get("id").filter(|id| !id.is_null()).cloned();
        match (method, id) {
            (Some("initialize"), Some(id)) => {
                *brain.info.lock().unwrap() = message["params"]["brain"].clone();
                brain.send(&json!({ "jsonrpc": "2.0", "id": id, "result": self.info_for_brain(&brain.name) }));
            }
            (Some("initialized"), None) => {
                *brain.ready.lock().unwrap() = true;
                let mut current = self.current.lock().unwrap();
                if current.is_none() {
                    *current = Some(brain_id);
                }
                self.log(&format!("大脑连上了：{}", brain.identity()));
            }
            (Some("ping"), Some(id)) => brain.send(&json!({ "jsonrpc": "2.0", "id": id, "result": {} })),
            (Some("act"), Some(id)) => self.act(brain_id, brain, id, &message["params"]),
            (Some("act.cancel"), None) => {
                let call = message["params"]["call"].as_str().unwrap_or("");
                self.to_device(&json!({ "jsonrpc": "2.0", "method": "act.cancel", "params": { "call": format!("b{brain_id}:{call}") } }));
            }
            (Some("event"), None) => {
                // 设备订的事件只转当前大脑的
                if *self.current.lock().unwrap() == Some(brain_id) {
                    self.to_device(message);
                }
            }
            (Some(other), Some(id)) => {
                brain.send(&json!({ "jsonrpc": "2.0", "id": id, "error": { "code": -32601, "message": format!("共享设备没有 {other}") } }));
            }
            (Some(_), None) => {}
            (None, Some(id)) => {
                // 大脑回了设备的请求：还给设备、换回设备原来的编号
                let Some((_, original)) = id.as_u64().and_then(|id| self.to_brain.lock().unwrap().remove(&id)) else { return };
                let mut reply = message.clone();
                reply["id"] = original;
                self.to_device(&reply);
            }
            (None, None) => {}
        }
    }

    fn act(&self, brain_id: u64, brain: &Arc<Brain>, id: Value, params: &Value) {
        let action = params["action"].as_str().unwrap_or("");
        if action == "claim" {
            *self.current.lock().unwrap() = Some(brain_id);
            let workspace = brain.identity()["workspace"].as_str().unwrap_or("").to_string();
            brain.send(&json!({ "jsonrpc": "2.0", "id": id, "result": { "text": format!("好了：设备「{}」之后主动传来的内容交给这个大脑（{workspace}）", self.name) } }));
            self.log(&format!("{workspace} 认领了设备"));
            return;
        }
        let (exclusive, holds, releases) = self.action_flags(action);
        {
            let holder = *self.holder.lock().unwrap();
            let running = *self.running.lock().unwrap();
            if let Some(other) = holder.filter(|holder| *holder != brain_id).or(running.filter(|running| *running != brain_id)) {
                brain.send(&self.busy(&id, other));
                return;
            }
            if exclusive {
                *self.running.lock().unwrap() = Some(brain_id);
            }
        }
        *self.current.lock().unwrap() = Some(brain_id);
        let forwarded = self.next.fetch_add(1, Ordering::Relaxed);
        self.to_device.lock().unwrap().insert(forwarded, Forward { brain: brain_id, id, exclusive, holds, releases });
        let mut params = params.clone();
        params["call"] = json!(format!("b{brain_id}:{}", params["call"].as_str().unwrap_or("")));
        params["brain"] = brain.identity();
        self.to_device(&json!({ "jsonrpc": "2.0", "id": forwarded, "method": "act", "params": params }));
    }

    /// 设备发来的一条
    fn handle_device(&self, message: &Value) {
        let method = message["method"].as_str();
        let id = message.get("id").filter(|id| !id.is_null()).cloned();
        match (method, id) {
            (None, Some(id)) => {
                // 设备回了一个动作
                let Some(forward) = id.as_u64().and_then(|id| self.to_device.lock().unwrap().remove(&id)) else { return };
                let ok = message.get("error").is_none() && message["result"]["ok"].as_bool() != Some(false);
                if forward.exclusive {
                    let mut running = self.running.lock().unwrap();
                    if *running == Some(forward.brain) {
                        *running = None;
                    }
                }
                if ok && forward.holds {
                    *self.holder.lock().unwrap() = Some(forward.brain);
                }
                if ok && forward.releases {
                    let mut holder = self.holder.lock().unwrap();
                    if *holder == Some(forward.brain) {
                        *holder = None;
                    }
                }
                if let Some(brain) = self.brain(forward.brain) {
                    let mut reply = message.clone();
                    reply["id"] = forward.id;
                    brain.send(&reply);
                }
            }
            (Some("ping"), Some(id)) => self.to_device(&json!({ "jsonrpc": "2.0", "id": id, "result": {} })),
            (Some(method), Some(id)) => match self.current_brain() {
                // sense / ask / sessions.*：交给当前大脑
                Some((brain_id, brain)) => {
                    let forwarded = self.next.fetch_add(1, Ordering::Relaxed);
                    self.to_brain.lock().unwrap().insert(forwarded, (brain_id, id));
                    brain.send(&json!({ "jsonrpc": "2.0", "id": forwarded, "method": method, "params": message["params"] }));
                }
                None => {
                    self.to_device(&json!({ "jsonrpc": "2.0", "id": id, "error": { "code": -32603, "message": "没有大脑连着这个设备" } }))
                }
            },
            (Some("act.progress"), None) => {
                // 进度还给要这个动作的那个大脑（编号去掉 b<大脑>: 前缀）
                let call = message["params"]["call"].as_str().unwrap_or("");
                let Some((prefix, rest)) = call.split_once(':') else { return };
                let Some(brain) = prefix.strip_prefix('b').and_then(|id| id.parse().ok()).and_then(|id| self.brain(id)) else { return };
                let mut progress = message.clone();
                progress["params"]["call"] = json!(rest);
                brain.send(&progress);
            }
            (Some("log"), None) => self.log(&format!("[设备] {}", message["params"]["text"].as_str().unwrap_or(""))),
            (Some(_), None) => {
                // sense 等通知：交给当前大脑
                if let Some((_, brain)) = self.current_brain() {
                    brain.send(message);
                }
            }
            (None, None) => {}
        }
    }

    fn brain_left(&self, brain_id: u64) {
        let removed = self.brains.lock().unwrap().remove(&brain_id);
        if let Some(brain) = &removed {
            self.log(&format!("大脑断开了：{}", brain.identity()));
        }
        {
            let mut holder = self.holder.lock().unwrap();
            if *holder == Some(brain_id) {
                *holder = None;
            }
            let mut running = self.running.lock().unwrap();
            if *running == Some(brain_id) {
                *running = None;
            }
        }
        // 当前大脑走了：换最近连上的那个
        let brains = self.brains.lock().unwrap();
        let mut current = self.current.lock().unwrap();
        if *current == Some(brain_id) {
            *current = brains.keys().next_back().copied();
        }
        if brains.is_empty() {
            *self.idle_since.lock().unwrap() = Some(Instant::now());
        }
        drop(brains);
        // 设备请求它、还没回的：回设备一个错
        let orphaned: Vec<Value> = {
            let mut pending = self.to_brain.lock().unwrap();
            let keys: Vec<u64> = pending.iter().filter(|(_, (brain, _))| *brain == brain_id).map(|(key, _)| *key).collect();
            keys.into_iter().filter_map(|key| pending.remove(&key)).map(|(_, id)| id).collect()
        };
        for id in orphaned {
            self.to_device(&json!({ "jsonrpc": "2.0", "id": id, "error": { "code": -32603, "message": "那个大脑断开了" } }));
        }
    }
}

fn hub(name: &str) -> Result<(), String> {
    let dir = devices_dir();
    std::fs::create_dir_all(&dir).map_err(|error| error.to_string())?;
    // 一个设备只起一个服务（几个中转同时拉起时，后来的拿不到锁就退出）
    let lock = File::options()
        .create(true)
        .truncate(false)
        .write(true)
        .open(dir.join(format!("{name}.lock")))
        .map_err(|error| error.to_string())?;
    if lock.try_lock().is_err() {
        return Ok(());
    }
    let log = File::options().create(true).append(true).open(dir.join(format!("{name}.log"))).map_err(|error| error.to_string())?;
    let configs = read_json(&home().join("devices.json")).and_then(|value| value["devices"].as_array().cloned()).unwrap_or_default();
    let config = configs
        .into_iter()
        .find(|item| item["name"] == name)
        .ok_or(format!("{} 里没有设备「{name}」", home().join("devices.json").display()))?;
    let command = config["command"].as_str().ok_or("设备要给 command")?;
    let arguments: Vec<String> = config["args"].as_array().into_iter().flatten().filter_map(Value::as_str).map(str::to_string).collect();
    let mut process = Command::new(command);
    process.args(&arguments).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).env("EDITH_DEVICE", name);
    for (key, value) in config["env"].as_object().into_iter().flatten() {
        process.env(key, value.as_str().map(str::to_string).unwrap_or_else(|| value.to_string()));
    }
    if let Some(cwd) = config["cwd"].as_str().filter(|cwd| !cwd.is_empty()) {
        process.current_dir(cwd);
    }
    let mut child: Child = process.spawn().map_err(|error| format!("起不了设备程序 {command}：{error}"))?;
    let device_in = child.stdin.take().ok_or("拿不到设备程序的标准输入")?;
    let device_out = child.stdout.take().ok_or("拿不到设备程序的标准输出")?;
    if let Some(stderr) = child.stderr.take() {
        let mut log = log.try_clone().map_err(|error| error.to_string())?;
        std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                let _ = writeln!(log, "[设备] {line}");
            }
        });
    }
    // 自己当设备的「大脑」握手
    let mut reader = BufReader::new(device_out);
    let mut device_in = device_in;
    let initialize = json!({ "jsonrpc": "2.0", "id": 0, "method": "initialize", "params": {
        "protocolVersion": "1",
        "brain": { "name": "EDITH 设备服务", "version": VERSION, "id": format!("device-{name}"), "workspace": "" },
        "name": name,
        "focus": "",
    }});
    writeln!(device_in, "{initialize}").map_err(|error| error.to_string())?;
    let info = loop {
        let mut line = String::new();
        if reader.read_line(&mut line).map_err(|error| error.to_string())? == 0 {
            return Err("设备程序没握手就退出了".into());
        }
        let Ok(message) = serde_json::from_str::<Value>(&line) else { continue };
        if message["id"] == 0 && message.get("method").is_none() {
            break message["result"].clone();
        }
    };
    if !info.is_object() {
        return Err("设备程序的 initialize 回话不对".into());
    }
    writeln!(device_in, "{}", json!({ "jsonrpc": "2.0", "method": "initialized", "params": {} })).map_err(|error| error.to_string())?;
    let listener = TcpListener::bind("127.0.0.1:0").map_err(|error| error.to_string())?;
    let address = listener.local_addr().map_err(|error| error.to_string())?.to_string();
    let token = uuid::Uuid::new_v4().simple().to_string();
    let hub = Arc::new(Hub {
        name: name.to_string(),
        device: Mutex::new(device_in),
        info,
        brains: Mutex::new(BTreeMap::new()),
        current: Mutex::new(None),
        holder: Mutex::new(None),
        running: Mutex::new(None),
        to_device: Mutex::new(HashMap::new()),
        to_brain: Mutex::new(HashMap::new()),
        next: AtomicU64::new(1),
        next_brain: AtomicU64::new(1),
        log: Mutex::new(log),
        idle_since: Mutex::new(Some(Instant::now())),
    });
    let registry = registry_path(name);
    let entry = json!({ "name": name, "pid": std::process::id(), "version": VERSION, "address": address, "token": token,
        "started": chrono::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false) });
    std::fs::write(&registry, serde_json::to_string_pretty(&entry).unwrap()).map_err(|error| error.to_string())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&registry, std::fs::Permissions::from_mode(0o600));
    }
    hub.log(&format!("设备服务起来了：{address}"));
    let finish = {
        let registry = registry.clone();
        move |hub: &Hub, reason: &str| -> ! {
            hub.log(reason);
            let _ = std::fs::remove_file(&registry);
            std::process::exit(0);
        }
    };
    // 设备发来的
    {
        let hub = hub.clone();
        let finish = finish.clone();
        std::thread::spawn(move || {
            for line in reader.lines() {
                let Ok(line) = line else { break };
                if let Ok(message) = serde_json::from_str::<Value>(&line) {
                    hub.handle_device(&message);
                }
            }
            finish(&hub, "设备程序退出了");
        });
    }
    // 没人连着一阵就退出
    {
        let hub = hub.clone();
        let idle = Duration::from_secs(config["idleSeconds"].as_u64().unwrap_or(900));
        let finish = finish.clone();
        let mut child = child;
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(Duration::from_millis(500));
                if let Ok(Some(_)) = child.try_wait() {
                    finish(&hub, "设备程序退出了");
                }
                let since = *hub.idle_since.lock().unwrap();
                if since.is_some_and(|since| since.elapsed() >= idle) {
                    let _ = child.kill();
                    let _ = child.wait();
                    finish(&hub, "没有大脑连着，退出");
                }
            }
        });
    }
    for stream in listener.incoming().flatten() {
        let hub = hub.clone();
        let token = token.clone();
        std::thread::spawn(move || serve_control(&hub, stream, &token));
    }
    Ok(())
}

/// 启动器连上来：先报令牌，再发 attach {address, token, name}——让设备服务去连那个大脑
fn serve_control(hub: &Arc<Hub>, stream: TcpStream, token: &str) {
    let Ok(reading) = stream.try_clone() else { return };
    let mut reader = BufReader::new(reading);
    let mut line = String::new();
    let authorized = reader.read_line(&mut line).is_ok()
        && serde_json::from_str::<Value>(&line).is_ok_and(|message| message["method"] == "auth" && message["params"]["token"] == token);
    if !authorized {
        let _ = stream.shutdown(Shutdown::Both);
        return;
    }
    let mut writer = stream;
    for line in reader.lines().map_while(Result::ok) {
        let Ok(message) = serde_json::from_str::<Value>(&line) else { continue };
        let id = message["id"].clone();
        let reply = if message["method"] == "attach" {
            let params = &message["params"];
            let address = params["address"].as_str().unwrap_or("").to_string();
            let already = hub.brains.lock().unwrap().values().any(|brain| brain.address == address);
            if already {
                json!({ "jsonrpc": "2.0", "id": id, "result": { "attached": true, "already": true } })
            } else {
                match dial(hub, &address, params["token"].as_str().unwrap_or(""), params["name"].as_str().unwrap_or(&hub.name)) {
                    Ok(()) => json!({ "jsonrpc": "2.0", "id": id, "result": { "attached": true } }),
                    Err(message) => json!({ "jsonrpc": "2.0", "id": id, "error": { "code": -32603, "message": message } }),
                }
            }
        } else {
            json!({ "jsonrpc": "2.0", "id": id, "error": { "code": -32601, "message": "设备服务只认 attach" } })
        };
        let _ = writeln!(writer, "{reply}");
        let _ = writer.flush();
    }
}

/// 连到一个大脑的套接字、报令牌，之后这条连接上讲能力协议（大脑发 initialize……）
fn dial(hub: &Arc<Hub>, address: &str, token: &str, name: &str) -> Result<(), String> {
    let mut stream = TcpStream::connect(address).map_err(|error| format!("连不上大脑 {address}：{error}"))?;
    writeln!(stream, "{}", json!({ "jsonrpc": "2.0", "method": "auth", "params": { "token": token } }))
        .map_err(|error| error.to_string())?;
    let reading = stream.try_clone().map_err(|error| error.to_string())?;
    let brain_id = hub.next_brain.fetch_add(1, Ordering::Relaxed);
    let brain = Arc::new(Brain {
        writer: Mutex::new(stream),
        name: name.to_string(),
        address: address.to_string(),
        info: Mutex::new(Value::Null),
        ready: Mutex::new(false),
    });
    hub.brains.lock().unwrap().insert(brain_id, brain.clone());
    *hub.idle_since.lock().unwrap() = None;
    let hub = hub.clone();
    std::thread::spawn(move || {
        for line in BufReader::new(reading).lines() {
            let Ok(line) = line else { break };
            if let Ok(message) = serde_json::from_str::<Value>(&line) {
                hub.handle_brain(brain_id, &brain, &message);
            }
        }
        let _ = brain.writer.lock().unwrap().shutdown(Shutdown::Both);
        hub.brain_left(brain_id);
    });
    Ok(())
}
