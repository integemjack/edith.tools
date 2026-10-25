#pragma once
#include <string>
#include <functional>

// Portable, Qt-free P2P call state machine + media-engine seam. Lives in the shared
// core so all three desktop ends (Qt on Win/Linux, Swift on macOS) drive calls through
// the same logic; only the transport I/O and the media backend are platform-specific.
//
// This manager speaks the CORE vocabulary: p2p_signaling type ∈ {offer, answer, ice, hangup,
// request_cancel} with the SDP / ICE candidate in separate strings. The platform transport maps
// that to/from the canonical schat-ws.v1 WIRE dialect (what iOS + Android speak: `ice_candidate`
// not `ice`, everything folded into one `payload` string) via schat/CallSignalWire.h — do NOT
// put the core vocabulary straight on the wire (iOS strict-Codable would drop it).
//
// Signaling wire (schat-ws.v1, relayed by chatserver with fromUserId rewritten, see
// internal/server/p2p.go). Every frame MUST carry timestamp (epoch ms) — iOS decodes these
// with strict Codable (P2PTypes.swift) and silently drops any frame missing a field:
//   p2p_request   {toUserId, fromUserId, callType:'audio'|'video', requestId, timestamp, expiresAt, callerName?}
//   p2p_response  {targetUserId, fromUserId, requestId, accepted, reason?, timestamp}
//                  reason ∈ user_rejected|busy|timeout|offline|error (registered enum — nothing else)
//   p2p_signaling {toUserId, fromUserId, requestId, type:'offer'|'answer'|'ice_candidate'|'hangup'|'request_cancel',
//                  payload, timestamp, durationSec?(hangup after connect), reason?('timeout' on auto-cancel)}
namespace schat {

enum class CallState { Idle, Outgoing, Incoming, Connecting, Connected, Ended };

inline const char* callStateName(CallState s) {
    switch (s) {
        case CallState::Idle:       return "idle";
        case CallState::Outgoing:   return "outgoing";
        case CallState::Incoming:   return "incoming";
        case CallState::Connecting: return "connecting";
        case CallState::Connected:  return "connected";
        case CallState::Ended:      return "ended";
    }
    return "idle";
}

// Platform transport seam — the platform implements this to put frames on its socket.
struct P2PSignalSink {
    virtual ~P2PSignalSink() = default;
    virtual void sendRequest(int toUserId, const std::string& requestId, const std::string& callType) = 0;
    virtual void sendResponse(int targetUserId, const std::string& requestId, bool accepted, const std::string& reason) = 0;
    virtual void sendSignaling(int toUserId, const std::string& requestId, const std::string& type,
                               const std::string& sdp, const std::string& candidate) = 0;
};

// Media backend seam (capture/encode/transport/decode/render). NullMediaEngine = signaling
// only (no real A/V); a libdatachannel or GStreamer engine plugs in here unchanged later.
struct MediaEngine {
    virtual ~MediaEngine() = default;
    virtual bool start(bool video, bool asCaller) = 0;   // caller creates the offer
    virtual void stop() = 0;
    virtual void onRemoteSdp(const std::string& type, const std::string& sdp) { (void)type; (void)sdp; }
    virtual void onRemoteCandidate(const std::string& candidate) { (void)candidate; }
    virtual bool isReal() const { return false; }
    // engine → manager: locally produced SDP/ICE to relay to the peer, and lifecycle.
    std::function<void(const std::string& type, const std::string& sdp)> onLocalSdp;
    std::function<void(const std::string& candidate)> onLocalCandidate;
    std::function<void()> onConnected;
    std::function<void()> onClosed;
};

// UI observer — the platform UI implements this to react to state changes.
struct P2PCallObserver {
    virtual ~P2PCallObserver() = default;
    virtual void onCallStateChanged(CallState st) = 0;
    /// 服务端把这次呼叫**当场拒了**,并给了原因(`not_friends` / `blocked` / …)。
    ///
    /// ★ 2026-08-21:此前 `onResponse` 里写着 `(void)reason;` —— 原因被**明确丢弃**,
    ///   接着 `Ended → reset → Idle`,于是用户看到的是**通话窗一闪而过、什么都不说**。
    ///   四端都不接这个 reason(手机两端同样零命中),是一致的缺口。
    ///
    /// 带默认空实现:现有实现者不受影响,想说话的端自己覆盖。
    /// ⚠️ 只在**被拒**时触发;正常挂断/无人接听走既有的结局串那条链,别混。
    virtual void onCallRejected(const std::string& reason) { (void)reason; }
    virtual void onIncomingCall(int fromUserId, const std::string& requestId, bool video) = 0;
};

class P2PCallManager {
public:
    P2PCallManager(P2PSignalSink* sink, P2PCallObserver* obs) : m_sink(sink), m_obs(obs) {}

    void setMediaEngine(MediaEngine* m);   // null = signaling-only

    // local intents
    void startCall(int peerId, bool video);
    void acceptCall();
    void rejectCall();
    void hangup();
    void abortLocal();   // teardown without any wire send (ring timeout / glare loser)

    // inbound (platform calls these when WS p2p frames arrive; fromUserId is server-authenticated)
    void onRequest(int fromUserId, const std::string& requestId, const std::string& callType);
    void onResponse(int fromUserId, const std::string& requestId, bool accepted, const std::string& reason);
    void onSignaling(int fromUserId, const std::string& requestId, const std::string& type,
                     const std::string& sdp, const std::string& candidate);

    CallState state() const { return m_state; }
    int  peerId()  const { return m_peerId; }
    bool isVideo() const { return m_video; }
    bool isCaller() const { return m_caller; }
    const std::string& requestId() const { return m_requestId; }

    // function<string()> the platform sets so the core can mint UUID request ids without
    // pulling a UUID dep into the portable core.
    std::function<std::string()> makeRequestId;

    // Optional executor for the engine's lifecycle callbacks. onConnected/onClosed fire on
    // the media engine's worker threads, but this state machine is single-threaded (the
    // platform UI thread drives every other entry point) — and onClosed's teardown calls
    // engine->stop(), which must never run INSIDE one of the engine's own callbacks
    // (destroying the PeerConnection from its callback thread deadlocks). Set this to post
    // onto the platform main loop; unset = invoked inline (tests / signaling-only).
    std::function<void(std::function<void()>)> post;

private:
    void setState(CallState s);
    void reset();
    void wireMedia();
    void handleMediaClosed();   // engine reported the connection closed/failed

    P2PSignalSink* m_sink;
    P2PCallObserver* m_obs;
    MediaEngine* m_media = nullptr;
    CallState m_state = CallState::Idle;
    int m_peerId = 0;
    bool m_video = false;
    bool m_caller = false;
    std::string m_requestId;
};

}  // namespace schat
