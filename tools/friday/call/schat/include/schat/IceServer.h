#pragma once
#include <string>
#include <vector>

namespace schat {

// One ICE server the platform injects from the server's create/join ack — a TURN relay with
// short-lived coturn REST creds (username=<expiry>:group, credential=base64(HMAC-SHA1(...))),
// or a static STUN/TURN entry. Appended to DcMediaEngine's built-in STUN list at start(), so a
// call gets a relay path when direct P2P can't traverse the NAT (or a TUN proxy has swallowed
// STUN so there are no srflx candidates). `url` is a single ICE URL
// (stun:/turn:/turns: host:port[?transport=udp|tcp]); `username`/`credential` are empty for STUN.
struct IceServer {
    std::string url;
    std::string username;
    std::string credential;
};

// Parse a server `iceServers` JSON array `[{urls: string|[string], username?, credential?}]`
// (the WebRTC-standard shape chatserver returns in p2p_call_create/join acks) into a flat list —
// one IceServer per url, a multi-url entry sharing its creds. Anything malformed is skipped
// (fail-safe: the engine keeps its built-in STUN base regardless). Single source of truth so the
// Swift ABI and the Qt controller don't each re-implement the parse.
std::vector<IceServer> parseIceServersJson(const std::string& json);

}  // namespace schat
