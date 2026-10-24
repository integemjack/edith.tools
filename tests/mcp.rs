//! edith-mcp：MCP 服务的工具当成动作报给大脑（名字不合规的换掉）、调动作就调工具

mod common;

use common::{FakeBrain, fake, python, scratch};
use serde_json::json;

#[test]
fn bridges_mcp_tools_as_actions() {
    let home = scratch("mcp");
    let brain = FakeBrain::new();
    let server = fake("mcp_server.py");
    let mut child = brain.spawn(env!("CARGO_BIN_EXE_edith-mcp"), &[python(), "-u", server.to_str().unwrap()], &home, "fake");
    let mut link = brain.accept(30).expect("edith-mcp 没连进来");
    let info = link.handshake(json!({ "id": "b", "workspace": "/w" }));
    assert_eq!(info["name"], "fake");
    assert_eq!(info["actions"][0]["name"], "echo_text");
    assert_eq!(info["actions"][0]["description"], "原样返回 text");
    assert_eq!(info["senses"], json!([]));
    let reply = link.act("echo_text", json!({ "text": "hi" }));
    assert_eq!(reply["result"]["text"], "回声：hi");
    assert_eq!(reply["result"]["ok"], true);
    // MCP 说出错了：ok 是 false
    assert_eq!(link.act("echo_text", json!({ "text": "坏" }))["result"]["ok"], false);
    assert_eq!(link.act("nope", json!({}))["error"]["code"], -32602);
    // 大脑叫它退出
    link.send(json!({ "jsonrpc": "2.0", "method": "shutdown", "params": {} }));
    assert!(common::wait_until(10, || matches!(child.try_wait(), Ok(Some(_)))));
}
