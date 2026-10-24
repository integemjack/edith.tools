//! edith-connect：两个大脑共用一个设备（tests/fake/device.py）——启动器让设备服务连进各个大脑的套接字；
//! 只起一个设备程序、设备忙、动作带上是哪个大脑、主动推的交给当前大脑（claim）、没人连着自己退出

mod common;

use std::time::Duration;

use common::{FakeBrain, fake, python, scratch, wait_until};
use serde_json::{Value, json};

#[test]
fn two_brains_share_one_device() {
    let home = scratch("connect");
    let device_log = home.join("device.jsonl");
    std::fs::write(
        home.join("devices.json"),
        json!({ "devices": [{
            "name": "mic",
            "command": python(),
            "args": ["-u", fake("device.py").to_str().unwrap()],
            "env": { "DEVICE_LOG": device_log.to_str().unwrap(), "DEVICE_ANNOUNCE_DELAY": "1.5" },
            "idleSeconds": 2,
        }]})
        .to_string(),
    )
    .unwrap();
    let (a, b) = (FakeBrain::new(), FakeBrain::new());
    let launch = |brain: &FakeBrain| {
        let mut launcher = brain.spawn(env!("CARGO_BIN_EXE_edith-connect"), &["mic"], &home, "mic");
        assert!(launcher.wait().unwrap().success(), "启动器失败");
    };
    launch(&a);
    let mut link_a = a.accept(20).expect("设备服务没连到 a");
    let info = link_a.handshake(json!({ "id": "a", "workspace": "/a" }));
    assert_eq!(info["name"], "mic");
    let actions: Vec<&str> = info["actions"].as_array().unwrap().iter().filter_map(|action| action["name"].as_str()).collect();
    assert!(actions.contains(&"record") && actions.contains(&"claim"), "{actions:?}");
    launch(&b);
    let mut link_b = b.accept(20).expect("设备服务没连到 b");
    link_b.handshake(json!({ "id": "b", "workspace": "/b" }));
    // 同一个大脑再起启动器：不连第二次
    launch(&a);
    assert!(a.accept(2).is_none());
    // 只起了一个设备程序
    assert_eq!(std::fs::read_to_string(&device_log).unwrap().matches("\"initialize\"").count(), 1);
    let text = |reply: &Value| reply["result"]["text"].as_str().unwrap_or("").to_string();
    // 设备忙：a 开始录（holds），b 被拒、说清是谁占着；a 停了 b 就能用
    assert_eq!(text(&link_a.act("record", json!({}))), "开始录了");
    let refused = link_b.act("record", json!({}));
    assert_eq!(refused["error"]["code"], -32005);
    assert_eq!(refused["error"]["data"]["holder"]["workspace"], "/a");
    assert_eq!(text(&link_a.act("stop", json!({}))), "停了");
    assert_eq!(text(&link_b.act("record", json!({}))), "开始录了");
    assert_eq!(text(&link_b.act("stop", json!({}))), "停了");
    // 设备程序知道是哪个大脑要的
    let who = text(&link_b.act("who", json!({})));
    assert!(who.contains("/b") && who.contains("call=b"), "{who}");
    // exclusive：a 的哔还没完时 b 被拒
    let beep = link_a.start("act", json!({ "call": "c9", "action": "beep", "arguments": {} }));
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(link_b.act("who", json!({}))["error"]["code"], -32005);
    assert_eq!(text(&link_a.reply_to(beep, 10)), "哔");
    // 当前大脑：a 让设备过一会儿说话，b 马上 claim，设备说的就交给 b
    assert_eq!(text(&link_a.act("announce", json!({}))), "一会儿说");
    assert!(text(&link_b.act("claim", json!({}))).contains("交给这个大脑"));
    let sensed = link_b.wait_for(10, |message| message["method"] == "sense").expect("设备说的没交给 b");
    assert_eq!(sensed["params"]["text"], "只聊天");
    assert!(link_a.wait_for(1, |message| message["method"] == "sense").is_none());
    // 没有大脑连着：设备服务过 idleSeconds 自己退出、删掉登记
    drop(link_a);
    drop(link_b);
    assert!(wait_until(20, || !home.join("devices").join("mic.json").exists()), "设备服务没自己退出");
}
