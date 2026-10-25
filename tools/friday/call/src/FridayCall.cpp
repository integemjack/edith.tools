// v2.8 助手通话的原生内核：FridayCall.h 的实现（说明在头文件里）。
// 通话照 Secret-Chat 桌面端 desktop-qt/core/src/CallABI.cpp 把 P2PSignalSink / P2PCallObserver 接到 C 回调表上；
// P2P 文字照 Qt 版 core/assistant/calls/AssistantTextSession.cpp 建只有 DataChannel 的 PeerConnection。
#include "FridayCall.h"

#include "schat/DcMediaEngine.h"
#include "schat/IceServer.h"
#include "schat/P2PCall.h"

#include <rtc/rtc.hpp>

#include <atomic>
#include <cstdio>
#include <functional>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <variant>
#include <vector>

using namespace schat;

namespace {

/// 引擎线程上来的回调排在这里，叫醒平台、在主线程上调出来（drain）。关了之后排进来的直接扔掉
class Inbox {
public:
    using Wake = std::function<void()>;

    explicit Inbox(Wake wake) : m_wake(std::move(wake)) {}

    void post(std::function<void()> fn)
    {
        bool first = false;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (m_closed)
                return;
            first = m_items.empty();
            m_items.push_back(std::move(fn));
        }
        if (first && m_wake)
            m_wake();
    }

    void drain()
    {
        std::vector<std::function<void()>> items;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            items.swap(m_items);
        }
        for (auto& item : items) {
            if (closed())
                return;
            item();
        }
    }

    void close()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_closed = true;
        m_items.clear();
    }

    bool closed()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_closed;
    }

private:
    std::mutex m_mutex;
    std::vector<std::function<void()>> m_items;
    bool m_closed = false;
    Wake m_wake;
};

std::string text(const char* value)
{
    return value ? std::string(value) : std::string();
}

std::vector<IceServer> iceServers(const char* const* urls, const char* const* usernames, const char* const* credentials, int count)
{
    std::vector<IceServer> list;
    for (int i = 0; i < count; ++i) {
        IceServer server;
        server.url = text(urls ? urls[i] : nullptr);
        server.username = text(usernames ? usernames[i] : nullptr);
        server.credential = text(credentials ? credentials[i] : nullptr);
        if (!server.url.empty())
            list.push_back(std::move(server));
    }
    return list;
}

/// UUID v4（requestId）
std::string makeUUID()
{
    static std::mutex mutex;
    static std::mt19937_64 random { std::random_device {}() };
    std::lock_guard<std::mutex> lock(mutex);
    uint64_t a = random();
    uint64_t b = random();
    a = (a & 0xffffffffffff0fffULL) | 0x0000000000004000ULL;
    b = (b & 0x3fffffffffffffffULL) | 0x8000000000000000ULL;
    char buffer[40];
    std::snprintf(buffer, sizeof(buffer), "%08x-%04x-%04x-%04x-%012llx", unsigned(a >> 32), unsigned((a >> 16) & 0xffff), unsigned(a & 0xffff),
                  unsigned(b >> 48), static_cast<unsigned long long>(b & 0xffffffffffffULL));
    return buffer;
}

struct CallBridge final : P2PSignalSink, P2PCallObserver {
    friday_call_callbacks cb {};
    Inbox inbox;
    DcMediaEngine media;
    P2PCallManager manager;

    explicit CallBridge(const friday_call_callbacks& callbacks)
        : cb(callbacks)
        , inbox([this] {
            if (cb.wake)
                cb.wake(cb.ctx);
        })
        , manager(static_cast<P2PSignalSink*>(this), static_cast<P2PCallObserver*>(this))
    {
        manager.makeRequestId = [] { return makeUUID(); };
        // 引擎的回调在 libdatachannel 的线程上：状态机只在主线程动
        manager.post = [this](std::function<void()> fn) { inbox.post(std::move(fn)); };
        manager.setMediaEngine(&media);
        media.onDecodedAudio = [this](const int16_t* pcm, int n) {
            if (cb.on_decoded_audio)
                cb.on_decoded_audio(cb.ctx, pcm, n);
        };
        media.onDecodedVideo = [this](const uint8_t* i420, int w, int h) {
            if (cb.on_decoded_video)
                cb.on_decoded_video(cb.ctx, i420, w, h);
        };
    }

    void sendRequest(int toUserId, const std::string& requestId, const std::string& callType) override
    {
        inbox.post([this, toUserId, requestId, callType] {
            if (cb.send_request)
                cb.send_request(cb.ctx, toUserId, requestId.c_str(), callType.c_str());
        });
    }
    void sendResponse(int targetUserId, const std::string& requestId, bool accepted, const std::string& reason) override
    {
        inbox.post([this, targetUserId, requestId, accepted, reason] {
            if (cb.send_response)
                cb.send_response(cb.ctx, targetUserId, requestId.c_str(), accepted ? 1 : 0, reason.c_str());
        });
    }
    void sendSignaling(int toUserId, const std::string& requestId, const std::string& type, const std::string& sdp,
                       const std::string& candidate) override
    {
        inbox.post([this, toUserId, requestId, type, sdp, candidate] {
            if (cb.send_signaling)
                cb.send_signaling(cb.ctx, toUserId, requestId.c_str(), type.c_str(), sdp.c_str(), candidate.c_str());
        });
    }
    void onCallStateChanged(CallState state) override
    {
        inbox.post([this, state] {
            if (cb.on_state)
                cb.on_state(cb.ctx, static_cast<int>(state));
        });
    }
    void onCallRejected(const std::string& reason) override
    {
        inbox.post([this, reason] {
            if (cb.on_rejected)
                cb.on_rejected(cb.ctx, reason.c_str());
        });
    }
    void onIncomingCall(int fromUserId, const std::string& requestId, bool video) override
    {
        inbox.post([this, fromUserId, requestId, video] {
            if (cb.on_incoming)
                cb.on_incoming(cb.ctx, fromUserId, requestId.c_str(), video ? 1 : 0);
        });
    }
};

CallBridge* bridge(friday_call* call)
{
    return reinterpret_cast<CallBridge*>(call);
}

/// 内置的 STUN（同 DcMediaEngine::start、iOS / 安卓的 defaultServers），后面接服务器给的 TURN
rtc::Configuration textConfiguration(const std::vector<IceServer>& servers)
{
    rtc::Configuration config;
    for (const char* url : { "stun:stun.l.google.com:19302", "stun:stun1.l.google.com:19302", "stun:stun2.l.google.com:19302",
                             "stun:stun3.l.google.com:19302", "stun:stun4.l.google.com:19302", "stun:stun.miwifi.com:3478",
                             "stun:stun.qq.com:3478", "stun:stun.syncthing.net:3478", "stun:stun.stunprotocol.org:3478" })
        config.iceServers.emplace_back(url);
    for (const IceServer& server : servers) {
        try {
            rtc::IceServer entry(server.url);
            if (!server.username.empty() || !server.credential.empty()) {
                entry.username = server.username;
                entry.password = server.credential;
            }
            config.iceServers.push_back(std::move(entry));
        } catch (const std::exception&) {
        }
    }
    // answer 自己出（setRemoteDescription(offer) 之后 setLocalDescription），同 DcMediaEngine
    config.disableAutoNegotiation = true;
    return config;
}

struct TextPeer {
    friday_text_callbacks cb {};
    Inbox inbox;
    std::shared_ptr<rtc::PeerConnection> pc;
    /// 主线程上：现在用的通道
    std::shared_ptr<rtc::DataChannel> channel;
    /// 认哪种通道（label 以它开头的）：P2P 文字是 p2p-text，远程工作通道是 friday-work（edith-friday 加的）
    std::string labelPrefix = "p2p-text";

    explicit TextPeer(const friday_text_callbacks& callbacks)
        : cb(callbacks)
        , inbox([this] {
            if (cb.wake)
                cb.wake(cb.ctx);
        })
    {
    }

    ~TextPeer()
    {
        inbox.close();
        // 先清回调（会等正在跑的回调跑完），再关：之后不会再有回调进来
        dropChannel();
        if (pc) {
            pc->resetCallbacks();
            try {
                pc->close();
            } catch (...) {
            }
            pc.reset();
        }
    }

    void dropChannel()
    {
        if (!channel)
            return;
        channel->resetCallbacks();
        try {
            channel->close();
        } catch (...) {
        }
        channel.reset();
    }

    void wire()
    {
        pc->onLocalDescription([this](rtc::Description description) {
            const std::string type = description.typeString();
            const std::string sdp = std::string(description);
            inbox.post([this, type, sdp] {
                if (cb.on_local_description)
                    cb.on_local_description(cb.ctx, type.c_str(), sdp.c_str());
            });
        });
        pc->onLocalCandidate([this](rtc::Candidate candidate) {
            const std::string line = candidate.candidate();
            const std::string mid = candidate.mid();
            inbox.post([this, line, mid] {
                if (cb.on_local_candidate)
                    cb.on_local_candidate(cb.ctx, line.c_str(), mid.c_str());
            });
        });
        pc->onStateChange([this](rtc::PeerConnection::State state) {
            using State = rtc::PeerConnection::State;
            if (state == State::Failed || state == State::Closed)
                inbox.post([this] {
                    if (cb.on_failed)
                        cb.on_failed(cb.ctx);
                });
        });
        // 通道是主叫建的（label p2p-text；手机重建时是 p2p-text-<时间>）：到这里时已经开了
        pc->onDataChannel([this](std::shared_ptr<rtc::DataChannel> incoming) {
            if (incoming->label().rfind(labelPrefix, 0) != 0)
                return;
            // 先排「换上这条通道」，再接消息：主线程上一定先换好、再处理消息
            inbox.post([this, incoming] {
                if (channel != incoming) {
                    dropChannel();
                    channel = incoming;
                }
                if (cb.on_channel_open)
                    cb.on_channel_open(cb.ctx);
            });
            watch(incoming);
        });
    }

    /// 通道关了、来消息：排回主线程
    void watch(const std::shared_ptr<rtc::DataChannel>& value)
    {
        const std::weak_ptr<rtc::DataChannel> weak = value;
        value->onClosed([this, weak] {
            inbox.post([this, weak] {
                if (const auto current = weak.lock(); current && channel == current && cb.on_channel_closed)
                    cb.on_channel_closed(cb.ctx);
            });
        });
        value->onMessage([this](rtc::message_variant data) {
            std::string frame;
            if (const auto* textValue = std::get_if<std::string>(&data))
                frame = *textValue;
            else if (const auto* binary = std::get_if<rtc::binary>(&data))
                frame.assign(reinterpret_cast<const char*>(binary->data()), binary->size());
            inbox.post([this, frame] {
                if (cb.on_message)
                    cb.on_message(cb.ctx, reinterpret_cast<const uint8_t*>(frame.data()), static_cast<int>(frame.size()));
            });
        });
    }
};

TextPeer* textPeer(friday_text_peer* peer)
{
    return reinterpret_cast<TextPeer*>(peer);
}

} // namespace

extern "C" {

// MARK: - 通话

friday_call* friday_call_create(const friday_call_callbacks* callbacks)
{
    if (!callbacks)
        return nullptr;
    return reinterpret_cast<friday_call*>(new CallBridge(*callbacks));
}

void friday_call_destroy(friday_call* call)
{
    if (!call)
        return;
    CallBridge* b = bridge(call);
    b->inbox.close();
    b->manager.abortLocal();
    b->media.stop();
    delete b;
}

void friday_call_drain(friday_call* call)
{
    if (call)
        bridge(call)->inbox.drain();
}

void friday_call_start(friday_call* call, int peer_id, int video)
{
    if (call)
        bridge(call)->manager.startCall(peer_id, video != 0);
}

void friday_call_accept(friday_call* call)
{
    if (call)
        bridge(call)->manager.acceptCall();
}

void friday_call_reject(friday_call* call)
{
    if (call)
        bridge(call)->manager.rejectCall();
}

void friday_call_hangup(friday_call* call)
{
    if (call)
        bridge(call)->manager.hangup();
}

void friday_call_abort_local(friday_call* call)
{
    if (call)
        bridge(call)->manager.abortLocal();
}

void friday_call_set_ice_servers(friday_call* call, const char* const* urls, const char* const* usernames,
                                 const char* const* credentials, int count)
{
    if (call)
        bridge(call)->media.setIceServers(iceServers(urls, usernames, credentials, count));
}

void friday_call_on_request(friday_call* call, int from_user_id, const char* request_id, const char* call_type)
{
    if (call)
        bridge(call)->manager.onRequest(from_user_id, text(request_id), text(call_type));
}

void friday_call_on_response(friday_call* call, int from_user_id, const char* request_id, int accepted, const char* reason)
{
    if (call)
        bridge(call)->manager.onResponse(from_user_id, text(request_id), accepted != 0, text(reason));
}

void friday_call_on_signaling(friday_call* call, int from_user_id, const char* request_id, const char* type, const char* sdp,
                              const char* candidate)
{
    if (call)
        bridge(call)->manager.onSignaling(from_user_id, text(request_id), text(type), text(sdp), text(candidate));
}

void friday_call_feed_capture_pcm(friday_call* call, const int16_t* pcm, int samples)
{
    if (call && pcm && samples > 0)
        bridge(call)->media.feedCapturePcm(pcm, samples);
}

void friday_call_feed_capture_frame(friday_call* call, const uint8_t* i420, int width, int height)
{
    if (call && i420 && width > 0 && height > 0)
        bridge(call)->media.feedCaptureFrame(i420, width, height);
}

void friday_call_set_video_encoding(friday_call* call, int bitrate_bps, float frame_rate, int threads)
{
    if (call)
        bridge(call)->media.setVideoEncoding(bitrate_bps, frame_rate, threads);
}

int friday_call_get_state(friday_call* call)
{
    return call ? static_cast<int>(bridge(call)->manager.state()) : 0;
}

int friday_call_is_video(friday_call* call)
{
    return call && bridge(call)->manager.isVideo() ? 1 : 0;
}

void friday_call_request_id(friday_call* call, char* out, int cap)
{
    if (!out || cap <= 0)
        return;
    out[0] = 0;
    if (!call)
        return;
    const std::string& id = bridge(call)->manager.requestId();
    std::snprintf(out, static_cast<size_t>(cap), "%s", id.c_str());
}

// MARK: - P2P 文字

friday_text_peer* friday_text_create(const friday_text_callbacks* callbacks, const char* const* urls, const char* const* usernames,
                                     const char* const* credentials, int count)
{
    return friday_text_create_labeled(callbacks, "p2p-text", urls, usernames, credentials, count);
}

friday_text_peer* friday_text_create_labeled(const friday_text_callbacks* callbacks, const char* label_prefix, const char* const* urls,
                                             const char* const* usernames, const char* const* credentials, int count)
{
    if (!callbacks)
        return nullptr;
    auto peer = std::make_unique<TextPeer>(*callbacks);
    if (label_prefix && *label_prefix)
        peer->labelPrefix = label_prefix;
    try {
        peer->pc = std::make_shared<rtc::PeerConnection>(textConfiguration(iceServers(urls, usernames, credentials, count)));
    } catch (const std::exception&) {
        return nullptr;
    }
    peer->wire();
    return reinterpret_cast<friday_text_peer*>(peer.release());
}

void friday_text_destroy(friday_text_peer* peer)
{
    delete textPeer(peer);
}

void friday_text_drain(friday_text_peer* peer)
{
    if (peer)
        textPeer(peer)->inbox.drain();
}

int friday_text_set_offer(friday_text_peer* peer, const char* sdp)
{
    if (!peer || !textPeer(peer)->pc)
        return 1;
    // 坏 SDP / 状态不对会抛：告诉平台，没连上的让看门狗收尾（同 DcMediaEngine::onRemoteSdp）
    try {
        textPeer(peer)->pc->setRemoteDescription(rtc::Description(text(sdp), "offer"));
        textPeer(peer)->pc->setLocalDescription();   // → answer
    } catch (const std::exception&) {
        return 1;
    }
    return 0;
}

void friday_text_add_candidate(friday_text_peer* peer, const char* candidate, const char* mid)
{
    if (!peer || !textPeer(peer)->pc)
        return;
    try {
        textPeer(peer)->pc->addRemoteCandidate(rtc::Candidate(text(candidate), text(mid)));
    } catch (...) {
    }
}

friday_text_peer* friday_text_create_offerer(const friday_text_callbacks* callbacks, const char* label)
{
    if (!callbacks)
        return nullptr;
    auto peer = std::make_unique<TextPeer>(*callbacks);
    try {
        peer->pc = std::make_shared<rtc::PeerConnection>(rtc::Configuration());   // 本机互连：不要 STUN
        peer->wire();
        TextPeer* raw = peer.get();
        raw->channel = raw->pc->createDataChannel(text(label));   // 自动出 offer
        raw->channel->onOpen([raw] {
            raw->inbox.post([raw] {
                if (raw->cb.on_channel_open)
                    raw->cb.on_channel_open(raw->cb.ctx);
            });
        });
        raw->watch(raw->channel);
    } catch (const std::exception&) {
        return nullptr;
    }
    return reinterpret_cast<friday_text_peer*>(peer.release());
}

int friday_text_set_answer(friday_text_peer* peer, const char* sdp)
{
    if (!peer || !textPeer(peer)->pc)
        return 1;
    try {
        textPeer(peer)->pc->setRemoteDescription(rtc::Description(text(sdp), "answer"));
    } catch (const std::exception&) {
        return 1;
    }
    return 0;
}

int friday_text_send_binary(friday_text_peer* peer, const uint8_t* data, int length)
{
    if (!peer || !textPeer(peer)->channel || !data || length < 0)
        return 1;
    try {
        textPeer(peer)->channel->send(reinterpret_cast<const std::byte*>(data), static_cast<size_t>(length));
        return 0;
    } catch (const std::exception&) {
        return 1;
    }
}

uint64_t friday_text_buffered(friday_text_peer* peer)
{
    if (!peer || !textPeer(peer)->channel)
        return 0;
    try {
        return static_cast<uint64_t>(textPeer(peer)->channel->bufferedAmount());
    } catch (...) {
        return 0;
    }
}

int friday_text_send(friday_text_peer* peer, const char* message)
{
    if (!peer || !textPeer(peer)->channel)
        return 1;
    try {
        // 文字帧（iOS 的 RTCDataBuffer isBinary:false）；send 回 false 是先进了缓冲、稍后发，也算发出去了
        textPeer(peer)->channel->send(text(message));
        return 0;
    } catch (const std::exception&) {
        return 1;
    }
}

} // extern "C"
