#include "schat/P2PCall.h"

namespace schat {

void P2PCallManager::setMediaEngine(MediaEngine* m) { m_media = m; wireMedia(); }

void P2PCallManager::wireMedia() {
    if (!m_media) return;
    m_media->onLocalSdp = [this](const std::string& type, const std::string& sdp) {
        if (m_peerId) m_sink->sendSignaling(m_peerId, m_requestId, type, sdp, "");
    };
    m_media->onLocalCandidate = [this](const std::string& cand) {
        if (m_peerId) m_sink->sendSignaling(m_peerId, m_requestId, "ice", "", cand);
    };
    // Lifecycle callbacks hop through `post` (when set) — they arrive on engine worker
    // threads and mutate the state machine / tear the engine down (see P2PCall.h).
    auto dispatch = [this](std::function<void()> fn) {
        if (post) post(std::move(fn)); else fn();
    };
    m_media->onConnected = [this, dispatch]() { dispatch([this]() { setState(CallState::Connected); }); };
    m_media->onClosed    = [this, dispatch]() { dispatch([this]() { handleMediaClosed(); }); };
}

void P2PCallManager::handleMediaClosed() {
    // Only act on an unexpected drop. During our own hangup/reject the state is already
    // Ended/Idle, so this is a no-op (and the engine's close() won't re-enter teardown).
    if (m_state == CallState::Idle || m_state == CallState::Ended) return;
    if (m_peerId) m_sink->sendSignaling(m_peerId, m_requestId, "hangup", "", "");
    setState(CallState::Ended);
    reset();
    setState(CallState::Idle);
}

void P2PCallManager::setState(CallState s) {
    if (m_state == s) return;
    m_state = s;
    if (m_obs) m_obs->onCallStateChanged(s);
}

void P2PCallManager::reset() {
    if (m_media) m_media->stop();
    m_peerId = 0;
    m_video = false;
    m_caller = false;
    m_requestId.clear();
}

void P2PCallManager::startCall(int peerId, bool video) {
    if (m_state != CallState::Idle && m_state != CallState::Ended) return;
    m_peerId = peerId;
    m_video = video;
    m_caller = true;
    m_requestId = makeRequestId ? makeRequestId() : std::string("req");
    setState(CallState::Outgoing);
    m_sink->sendRequest(peerId, m_requestId, video ? "video" : "audio");
    // Do NOT create the offer yet — wait until the callee accepts (onResponse). Offering at
    // ring time would deliver the offer before the callee's PeerConnection exists.
}

void P2PCallManager::acceptCall() {
    if (m_state != CallState::Incoming) return;
    setState(CallState::Connecting);
    m_sink->sendResponse(m_peerId, m_requestId, true, "");
    if (m_media) m_media->start(m_video, /*asCaller=*/false);
    else setState(CallState::Connected);   // signaling-only: treat accept as established
}

void P2PCallManager::rejectCall() {
    if (m_state != CallState::Incoming) return;
    // "user_rejected" is the registered cross-end enum value (iOS P2PRejectReason is strict
    // Codable — an unknown reason string makes it drop the whole response frame).
    m_sink->sendResponse(m_peerId, m_requestId, false, "user_rejected");
    setState(CallState::Ended);
    reset();
    setState(CallState::Idle);
}

void P2PCallManager::abortLocal() {
    // Local-only teardown (ring timeout / glare bookkeeping): no wire send — the platform
    // has either already sent the terminal signal itself or the peer's cancel is authoritative.
    if (m_state == CallState::Idle) return;
    setState(CallState::Ended);
    reset();
    setState(CallState::Idle);
}

void P2PCallManager::hangup() {
    if (m_state == CallState::Idle) return;
    if (m_peerId) {
        // before connect, the caller cancels; otherwise it's a hangup. Either way the peer
        // sees a terminal setup signal (chatserver feeds its call-end fallback tracker).
        const char* type = (m_state == CallState::Outgoing) ? "request_cancel" : "hangup";
        m_sink->sendSignaling(m_peerId, m_requestId, type, "", "");
    }
    setState(CallState::Ended);
    reset();
    setState(CallState::Idle);
}

void P2PCallManager::onRequest(int fromUserId, const std::string& requestId, const std::string& callType) {
    if (m_state != CallState::Idle && m_state != CallState::Ended) {
        // busy — auto-decline so the caller isn't left ringing.
        m_sink->sendResponse(fromUserId, requestId, false, "busy");
        return;
    }
    m_peerId = fromUserId;
    m_requestId = requestId;
    m_video = (callType == "video");
    m_caller = false;
    setState(CallState::Incoming);
    if (m_obs) m_obs->onIncomingCall(fromUserId, requestId, m_video);
}

void P2PCallManager::onResponse(int fromUserId, const std::string& requestId, bool accepted, const std::string& reason) {
    if (m_state != CallState::Outgoing) return;
    if (fromUserId != m_peerId || (!requestId.empty() && requestId != m_requestId)) return;
    if (accepted) {
        setState(CallState::Connecting);
        // Callee has accepted — now the caller creates the offer (callee already started its
        // PeerConnection in acceptCall, so it's ready to receive it). Connected arrives via
        // the engine's onConnected after the answer.
        if (m_media) m_media->start(m_video, /*asCaller=*/true);
        else setState(CallState::Connected);   // signaling-only
    } else {
        // 先把原因交出去,再收窗 —— 收完窗状态已是 Idle,那时再说就没有承载它的界面了。
        if (m_obs && !reason.empty()) m_obs->onCallRejected(reason);
        setState(CallState::Ended);
        reset();
        setState(CallState::Idle);
    }
}

void P2PCallManager::onSignaling(int fromUserId, const std::string& requestId, const std::string& type,
                                 const std::string& sdp, const std::string& candidate) {
    if (m_peerId && fromUserId != m_peerId) return;
    if (!requestId.empty() && !m_requestId.empty() && requestId != m_requestId) return;

    if (type == "hangup" || type == "request_cancel") {
        setState(CallState::Ended);
        reset();
        setState(CallState::Idle);
        return;
    }
    if (!m_media) return;   // signaling-only: nothing to feed the (absent) engine
    if (type == "offer" || type == "answer") {
        // Connected comes from the engine's onConnected (ICE/DTLS actually up) — flipping on
        // the answer showed "connected" with dead media whenever ICE went on to fail.
        m_media->onRemoteSdp(type, sdp);
    } else if (type == "ice") {
        m_media->onRemoteCandidate(candidate);
    }
}

}  // namespace schat
