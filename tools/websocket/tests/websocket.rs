//! edith-websocket：远程客户端经它连进大脑的套接字，每个客户端一条连接、原样转（补 name）

use edith_tools_support::{FakeBrain, scratch};
use serde_json::{Value, json};

fn read(socket: &mut tungstenite::WebSocket<tungstenite::stream::MaybeTlsStream<std::net::TcpStream>>) -> Value {
    loop {
        let message = socket.read().unwrap();
        if let Ok(value) = serde_json::from_str::<Value>(message.to_text().unwrap_or("")) {
            return value;
        }
    }
}

fn send(socket: &mut tungstenite::WebSocket<tungstenite::stream::MaybeTlsStream<std::net::TcpStream>>, value: Value) {
    socket.send(tungstenite::Message::text(value.to_string())).unwrap();
}

#[test]
fn relays_remote_capabilities_into_the_brain() {
    let home = scratch("websocket");
    let brain = FakeBrain::new();
    let mut child = brain.spawn(env!("CARGO_BIN_EXE_edith-websocket"), &["--token", "secret"], &home, "ws");
    // 它自己那条连接：报名字和入口
    let mut control = brain.accept(20).expect("edith-websocket 没连进来");
    let info = control.handshake(json!({ "id": "b", "workspace": "/w" }));
    assert_eq!(info["name"], "ws");
    let url = info["endpoints"][0]["url"].as_str().unwrap().to_string();
    // 令牌不对连不上
    assert!(tungstenite::connect(format!("{url}/?token=wrong")).is_err());
    // 远程客户端：替它另开一条连接，原样转
    let (mut socket, _) = tungstenite::connect(format!("{url}/?token=secret&name=phone")).unwrap();
    let mut link = brain.accept(20).expect("没替客户端连进来");
    // 替客户端连的报 as（大脑据此定名字、收窄权限）；它自己那条不报
    assert_eq!(link.relay_as.as_deref(), Some("phone"));
    assert_eq!(control.relay_as, None);
    let id = link.start("initialize", json!({ "protocolVersion": "1", "brain": { "id": "b" }, "name": "", "focus": "s1" }));
    let initialize = read(&mut socket);
    assert_eq!(initialize["method"], "initialize");
    // 客户端回话没写 name：补上 ?name= 的
    send(
        &mut socket,
        json!({ "jsonrpc": "2.0", "id": initialize["id"], "result": {
        "protocolVersion": "1", "actions": [{ "name": "photo", "description": "拍一张" }], "senses": [{ "kind": "vision" }] } }),
    );
    let reply = link.reply_to(id, 10);
    assert_eq!(reply["result"]["name"], "phone");
    assert_eq!(reply["result"]["actions"][0]["name"], "photo");
    link.send(json!({ "jsonrpc": "2.0", "method": "initialized", "params": {} }));
    assert_eq!(read(&mut socket)["method"], "initialized");
    // 客户端主动推
    send(&mut socket, json!({ "jsonrpc": "2.0", "method": "sense", "params": { "text": "门口有人" } }));
    let sensed = link.wait_for(10, |message| message["method"] == "sense").unwrap();
    assert_eq!(sensed["params"]["text"], "门口有人");
    // 大脑让它做动作、给它事件
    let act = link.start("act", json!({ "call": "c1", "action": "photo", "arguments": {} }));
    let asked = read(&mut socket);
    assert_eq!(asked["method"], "act");
    send(&mut socket, json!({ "jsonrpc": "2.0", "id": asked["id"], "result": { "text": "拍好了" } }));
    assert_eq!(link.reply_to(act, 10)["result"]["text"], "拍好了");
    link.send(json!({ "jsonrpc": "2.0", "method": "event", "params": { "type": "text", "data": { "text": "好" } } }));
    assert_eq!(read(&mut socket)["params"]["data"]["text"], "好");
    // 客户端走了：它那条连接也关掉
    socket.close(None).unwrap();
    assert!(link.closed(10));
    // 大脑叫它退出
    control.send(json!({ "jsonrpc": "2.0", "method": "shutdown", "params": {} }));
    assert!(edith_tools_support::wait_until(10, || matches!(child.try_wait(), Ok(Some(_)))));
}

#[test]
fn exits_when_the_brain_goes_away() {
    // 大脑被强行结束：它那条连接断了、日志也写不出去（标准错误的管子关了）——照样要退出，不能挂在那里
    let home = scratch("websocket-gone");
    let brain = FakeBrain::new();
    let mut child = brain.spawn_with_broken_stderr(env!("CARGO_BIN_EXE_edith-websocket"), &[], &home, "ws");
    let mut control = brain.accept(20).expect("edith-websocket 没连进来");
    control.handshake(json!({ "id": "b", "workspace": "/w" }));
    drop(control);
    assert!(edith_tools_support::wait_until(10, || matches!(child.try_wait(), Ok(Some(_)))), "大脑没了它还开着");
}
