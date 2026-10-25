#include "schat/DcMediaEngine.h"
#include <rtc/rtc.hpp>
#include <opus.h>
#include <wels/codec_api.h>
#include <wels/codec_app_def.h>
#include <wels/codec_def.h>
#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstring>
#include <vector>

namespace schat {

namespace {
bool g_logInit = false;
constexpr int kChans = 1;
constexpr int kPayloadType = 111;   // dynamic PT for Opus (our own offer; callee reads the peer's)
constexpr int kVideoPT = 96;        // dynamic PT for H.264 (ditto)

bool sameFormat(const std::string& a, const char* b) {
    size_t n = std::strlen(b);
    if (a.size() != n) return false;
    for (size_t i = 0; i < n; ++i)
        if (std::tolower(static_cast<unsigned char>(a[i])) !=
            std::tolower(static_cast<unsigned char>(b[i]))) return false;
    return true;
}

// First payload type in `media` whose rtpmap format matches `fmt`. As callee we must
// packetize outbound RTP with the PEER's negotiated PT — hardcoding ours (96/111) made a
// libwebrtc peer demux our H.264 as whatever ITS offer mapped to 96 (usually VP8 → black
// remote video). 111 for Opus only ever worked because it happens to match libwebrtc's default.
int negotiatedPayloadType(const rtc::Description::Media& media, const char* fmt, int fallback) {
    for (int pt : media.payloadTypes())
        if (const auto* m = media.rtpMap(pt); m && sameFormat(m->format, fmt)) return pt;
    return fallback;
}

// Callee only: restrict the reciprocated answer to the one codec this engine can actually
// produce/consume, so a libwebrtc peer can't elect VP8/VP9 (we only have Opus + openh264).
// No-op when the offer doesn't carry the format at all (then the call is audio-or-nothing
// anyway and pruning would empty the m-line).
void pruneToFormat(rtc::Description::Media& media, const char* fmt) {
    bool has = false;
    for (int pt : media.payloadTypes())
        if (const auto* m = media.rtpMap(pt); m && sameFormat(m->format, fmt)) { has = true; break; }
    if (!has) return;
    std::vector<int> drop;
    for (int pt : media.payloadTypes()) {
        const auto* m = media.rtpMap(pt);
        if (!m || !sameFormat(m->format, fmt)) drop.push_back(pt);
    }
    for (int pt : drop) media.removeRtpMap(pt);
}

// Among surviving H.264 entries prefer baseline/constrained-baseline (profile-level-id=42xx):
// openh264 can't decode High, and phones (Android enables H264 High Profile) offer both.
// Drop every RTP header extension from a reciprocated (callee) description — we implement
// NONE of them. Echoing the phone's `urn:3gpp:video-orientation` extmap told libwebrtc the
// receiver would rotate at render time, so it sent LANDSCAPE frames + a CVO header we never
// parse → the peer's video rendered sideways on the desktop, always. Echoing
// transport-cc/abs-send-time likewise promised congestion feedback that never comes. With no
// extmap in the answer, libwebrtc rotates frames BEFORE encoding (upright on our side).
void stripExtMaps(rtc::Description::Media& media) {
    for (int id = 1; id <= 255; ++id) media.removeExtMap(id);
}

// Local candidates that can never help a remote peer and actively mislead it: loopback, and
// the 198.18.0.0/15 fake-IP range a TUN proxy (Surge/Clash) presents as a phantom NIC. A TUN
// proxy usually also swallows STUN (no srflx at all — that part only the user can fix by
// bypassing UDP/STUN in the proxy), but at least don't advertise poison to the peer.
bool uselessLocalCandidate(const std::string& cand) {
    return cand.find(" 127.")    != std::string::npos ||
           cand.find(" ::1 ")    != std::string::npos ||
           cand.find(" 198.18.") != std::string::npos ||
           cand.find(" 198.19.") != std::string::npos;
}

void preferBaselineH264(rtc::Description::Media& media) {
    auto isBaseline = [&](int pt) {
        const auto* m = media.rtpMap(pt);
        if (!m || !sameFormat(m->format, "H264")) return false;
        for (const auto& f : m->fmtps)
            if (f.find("profile-level-id=42") != std::string::npos) return true;
        return false;
    };
    bool hasBaseline = false;
    for (int pt : media.payloadTypes())
        if (isBaseline(pt)) { hasBaseline = true; break; }
    if (!hasBaseline) return;
    std::vector<int> drop;
    for (int pt : media.payloadTypes()) {
        const auto* m = media.rtpMap(pt);
        if (m && sameFormat(m->format, "H264") && !isBaseline(pt)) drop.push_back(pt);
    }
    for (int pt : drop) media.removeRtpMap(pt);
}
}

DcMediaEngine::DcMediaEngine() {
    // SCHAT_RTC_DEBUG=1 → libdatachannel/libjuice 全量日志(ICE 候选/连通性检查逐条可见),
    // 排查「接听后一直连接中→超时挂断」这类 ICE 级问题用;默认仅告警。
    if (!g_logInit) {
        rtc::InitLogger(std::getenv("SCHAT_RTC_DEBUG") ? rtc::LogLevel::Verbose
                                                       : rtc::LogLevel::Warning);
        g_logInit = true;
    }
}

DcMediaEngine::~DcMediaEngine() { stop(); }

bool DcMediaEngine::start(bool video, bool asCaller) {
    stop();
    m_caller = asCaller;
    m_wantVideo = video;
    m_ssrc  = asCaller ? 0x5C001111u : 0x5C002222u;   // distinct SSRC per side
    m_vssrc = asCaller ? 0x5C003333u : 0x5C004444u;
    m_rtpTs = 0;
    m_vts = 0;
    m_videoEpoch = {};

    int err = 0;
    {
        std::lock_guard<std::mutex> lk(m_sendMutex);
        m_enc = opus_encoder_create(kRate, kChans, OPUS_APPLICATION_VOIP, &err);
        if (m_enc) {
            opus_encoder_ctl(m_enc, OPUS_SET_BITRATE(24000));
            opus_encoder_ctl(m_enc, OPUS_SET_SIGNAL(OPUS_SIGNAL_VOICE));
        }
    }
    {
        std::lock_guard<std::mutex> lk(m_playMutex);
        m_dec = opus_decoder_create(kRate, kChans, &err);
    }

    rtc::Configuration config;
    // Mirror of iOS ICEServerConfig.defaultServers / Android P2PTypes defaultServers — STUN
    // parity is a registered cross-end rule (Google STUN alone is unreachable from mainland
    // China → no srflx candidates → every cross-network call fails). TURN relays are appended
    // below from m_iceServers (server ack) when configured — the only way a call traverses a
    // symmetric NAT, or connects at all behind a TUN proxy that swallows STUN (no srflx).
    config.iceServers.emplace_back("stun:stun.l.google.com:19302");
    config.iceServers.emplace_back("stun:stun1.l.google.com:19302");
    config.iceServers.emplace_back("stun:stun2.l.google.com:19302");
    config.iceServers.emplace_back("stun:stun3.l.google.com:19302");
    config.iceServers.emplace_back("stun:stun4.l.google.com:19302");
    config.iceServers.emplace_back("stun:stun.miwifi.com:3478");
    config.iceServers.emplace_back("stun:stun.qq.com:3478");
    config.iceServers.emplace_back("stun:stun.syncthing.net:3478");
    config.iceServers.emplace_back("stun:stun.stunprotocol.org:3478");
    // Server-provided ICE servers (TURN relays with short-lived coturn REST creds, from the
    // p2p_call_create/join ack). Construct from the URL (parses scheme/host/port/transport →
    // type/relayType), then OVERWRITE username/password with the ack values — never embed creds
    // in the URL: coturn REST credentials are base64 (may contain '/', '+', '=') and would break
    // URL parsing. A malformed entry is skipped (fail-safe: the STUN base above still stands).
    for (const auto& s : m_iceServers) {
        if (s.url.empty()) continue;
        try {
            rtc::IceServer srv(s.url);
            if (!s.username.empty() || !s.credential.empty()) {
                srv.username = s.username;
                srv.password = s.credential;
            }
            config.iceServers.push_back(std::move(srv));
        } catch (const std::exception&) { /* skip malformed url */ }
    }
    // We drive offer/answer EXPLICITLY (caller: setLocalDescription after addTrack; callee:
    // after setRemoteDescription(offer)). libdatachannel's auto-negotiation (default ON)
    // otherwise fires a spurious renegotiation offer the moment signaling returns to Stable:
    // the callee's onTrack calls track->setDescription (SSRC + codec prune) which marks
    // negotiationNeeded, so right after our ANSWER went out the PC emitted a second, useless
    // offer and the phone dutifully re-answered (seen live in the 2026-07-15 server log).
    config.disableAutoNegotiation = true;
    m_pc = std::make_shared<rtc::PeerConnection>(config);

    m_pc->onLocalDescription([this](rtc::Description d) {
        if (onLocalSdp) onLocalSdp(d.typeString(), std::string(d));
    });
    m_pc->onLocalCandidate([this](rtc::Candidate c) {
        const std::string cand = std::string(c);
        if (uselessLocalCandidate(cand)) return;
        if (onLocalCandidate) onLocalCandidate(cand);
    });
    m_pc->onStateChange([this](rtc::PeerConnection::State s) {
        using St = rtc::PeerConnection::State;
        if (s == St::Connected) { if (onConnected) onConnected(); }
        else if (s == St::Failed || s == St::Closed) { if (onClosed) onClosed(); }
    });
    // Callee path: the reciprocal tracks show up here after setRemoteDescription(offer).
    // Member track assignments go under the feeder's mutex (see stop() for why).
    m_pc->onTrack([this](std::shared_ptr<rtc::Track> t) {
        if (t->description().type() == "video") {
            { std::lock_guard<std::mutex> lk(m_encMutex); m_videoTrack = t; }
            setupVideoTrack(t);
        } else {
            { std::lock_guard<std::mutex> lk(m_sendMutex); m_audioTrack = t; }
            setupAudioTrack(t);
        }
    });

    if (asCaller) {
        // mids MUST be "0"/"1": the wire ICE dialect (CallSignalWire) stamps sdpMid:"0" on every
        // candidate, and libwebrtc drops a candidate whose non-empty mid matches no m-line — it
        // does NOT fall back to sdpMLineIndex. With the old mids ("audio"/"video") phones dropped
        // every desktop candidate in the desktop-as-caller direction (LAN survived on prflx only).
        rtc::Description::Audio amedia("0", rtc::Description::Direction::SendRecv);
        amedia.addOpusCodec(kPayloadType);
        amedia.addSSRC(m_ssrc, "schat-audio");
        auto atrack = m_pc->addTrack(amedia);
        { std::lock_guard<std::mutex> lk(m_sendMutex); m_audioTrack = atrack; }
        setupAudioTrack(atrack);
        if (m_wantVideo) {
            rtc::Description::Video vmedia("1", rtc::Description::Direction::SendRecv);
            vmedia.addH264Codec(kVideoPT);
            vmedia.addSSRC(m_vssrc, "schat-video");
            auto vtrack = m_pc->addTrack(vmedia);
            { std::lock_guard<std::mutex> lk(m_encMutex); m_videoTrack = vtrack; }
            setupVideoTrack(vtrack);
        }
        m_pc->setLocalDescription();   // → offer with the audio (+video) m-lines
    }
    return true;
}

void DcMediaEngine::setupVideoTrack(std::shared_ptr<rtc::Track> track) {
    int videoPT = kVideoPT;
    if (!m_caller) {
        auto d = track->description();
        pruneToFormat(d, "H264");     // answer H264-only so libwebrtc can't elect VP8/VP9
        preferBaselineH264(d);
        stripExtMaps(d);              // no CVO/transport-cc echo (sideways-video fix, see above)
        videoPT = negotiatedPayloadType(d, "H264", kVideoPT);
        d.addSSRC(m_vssrc, "schat-video");
        track->setDescription(d);
    }
    auto rtpConfig = std::make_shared<rtc::RtpPacketizationConfig>(
        m_vssrc, "schat", videoPT, static_cast<uint32_t>(kVideoClock));
    auto packetizer = std::make_shared<rtc::H264RtpPacketizer>(
        rtc::NalUnit::Separator::StartSequence, rtpConfig);   // fragments large NALs (FU-A)
    auto depacketizer = std::make_shared<rtc::H264RtpDepacketizer>(rtc::NalUnit::Separator::StartSequence);
    packetizer->addToChain(depacketizer);
    // The peer's PLI/FIR must yield a fresh IDR: libwebrtc keeps requesting one until it can
    // decode, and openh264 only emits an IDR on the FIRST encode — ignoring PLI left the
    // phone stuck on undecodable P-frames (black remote video) forever.
    packetizer->addToChain(std::make_shared<rtc::PliHandler>([this]() { m_forceIdr = true; }));
    // Inbound RTCP bookkeeping + lets Track::requestKeyframe() emit an outgoing PLI.
    packetizer->addToChain(std::make_shared<rtc::RtcpReceivingSession>());
    // Friday 版：丢包重传 + 按节奏发。助手的画面一个关键帧要拆成上百个 RTP 包，一下子全发出去在真实网络上容易丢一片，
    // 丢了对方解不出来、只能再要关键帧，新的关键帧又是一大坨——手机上画面卡住不动、发糊（用户 2026-10-09）。
    // 重传要在节奏之前（它记下的是要发的包；节奏把包收进自己的队列，后面的处理看不到）
    // 节奏固定按 kVideoPacingBps（官方 libdatachannel 0.24.5 的 PacingHandler 只有构造函数，不能中途改）
    packetizer->addToChain(std::make_shared<rtc::RtcpNackResponder>());
    packetizer->addToChain(std::make_shared<rtc::PacingHandler>(kVideoPacingBps, std::chrono::milliseconds(5)));
    track->setMediaHandler(packetizer);
    track->onFrame([this](rtc::binary frame, rtc::FrameInfo) {
        handleIncomingVideo(frame.data(), frame.size());
    });
    track->onOpen([this]() { m_videoOpen = true; m_forceIdr = true; });

    std::lock_guard<std::mutex> lk(m_encMutex);
    if (!m_dec264) {
        WelsCreateDecoder(&m_dec264);
        if (m_dec264) {
            SDecodingParam dp; std::memset(&dp, 0, sizeof(dp));
            dp.sVideoProperty.eVideoBsType = VIDEO_BITSTREAM_AVC;
            m_dec264->Initialize(&dp);
        }
    }
}

bool DcMediaEngine::initEncoder(int w, int h) {
    if (m_enc264) { m_enc264->Uninitialize(); WelsDestroySVCEncoder(m_enc264); m_enc264 = nullptr; }
    if (WelsCreateSVCEncoder(&m_enc264) != 0 || !m_enc264) { m_enc264 = nullptr; return false; }
    // Friday 版：扩展参数——码率、帧率由 setVideoEncoding 定（原来写死 800000 / 30），线程多于 1 时按同样多的条带并行编码
    // （助手 2K 60 帧：单线程编一帧要二十多毫秒）。其余同原来的 SEncParamBase（Initialize 内部也是按这些默认值展开的）
    SEncParamExt p;
    m_enc264->GetDefaultParams(&p);
    p.iUsageType         = CAMERA_VIDEO_REAL_TIME;
    p.iPicWidth          = w;
    p.iPicHeight         = h;
    p.iTargetBitrate     = m_encBitrate;
    p.iMaxBitrate        = UNSPECIFIED_BIT_RATE;
    p.iRCMode            = RC_BITRATE_MODE;
    p.fMaxFrameRate      = m_encFps;
    p.iMultipleThreadIdc = static_cast<unsigned short>(std::max(1, m_encThreads));
    p.iSpatialLayerNum   = 1;
    SSpatialLayerConfig& layer = p.sSpatialLayers[0];
    layer.iVideoWidth        = w;
    layer.iVideoHeight       = h;
    layer.fFrameRate         = m_encFps;
    layer.iSpatialBitrate    = m_encBitrate;
    layer.iMaxSpatialBitrate = UNSPECIFIED_BIT_RATE;
    if (m_encThreads > 1) {
        layer.sSliceArgument.uiSliceMode = SM_FIXEDSLCNUM_SLICE;
        layer.sSliceArgument.uiSliceNum  = static_cast<unsigned>(m_encThreads);
    }
    if (m_enc264->InitializeExt(&p) != 0) { WelsDestroySVCEncoder(m_enc264); m_enc264 = nullptr; return false; }
    int fmt = videoFormatI420;
    m_enc264->SetOption(ENCODER_OPTION_DATAFORMAT, &fmt);
    m_encW = w; m_encH = h;
    return true;
}

void DcMediaEngine::setVideoEncoding(int bitrateBps, float frameRate, int threads) {
    std::lock_guard<std::mutex> lk(m_encMutex);
    if (bitrateBps == m_encBitrate && frameRate == m_encFps && threads == m_encThreads) return;
    m_encBitrate = bitrateBps;
    m_encFps = frameRate;
    m_encThreads = threads;
    m_encW = m_encH = 0;   // 下一帧按新参数重建编码器
}

void DcMediaEngine::feedCaptureFrame(const uint8_t* i420, int w, int h) {
    if (!i420 || w <= 0 || h <= 0) return;
    // Everything (incl. the m_videoTrack null-check) runs under m_encMutex, and stop() clears
    // the track/encoder under the SAME lock. The camera dispatch queue feeds frames while a
    // main-thread hangup tears down: with the old unlocked member read, stop() nulled
    // m_videoTrack mid-encode and this thread crashed in Track::isOpen() as soon as it
    // finished (stop() was meanwhile blocked right here waiting for m_encMutex).
    std::lock_guard<std::mutex> lk(m_encMutex);
    // Not-yet-open frames must not reach the encoder: the camera starts feeding while the call
    // still RINGS, and openh264 emits its one natural IDR on the very first encode — burning it
    // on a frame that gets dropped (track closed) left the peer with undecodable P-frames only.
    // Gating here makes the first frame the peer actually receives an IDR.
    if (!m_videoTrack || !m_videoTrack->isOpen()) return;
    if (!m_enc264 || m_encW != w || m_encH != h) { if (!initEncoder(w, h)) return; }
    if (m_forceIdr.exchange(false)) m_enc264->ForceIntraFrame(true);   // PLI / track (re)open

    SSourcePicture pic; std::memset(&pic, 0, sizeof(pic));
    pic.iColorFormat = videoFormatI420;
    pic.iPicWidth = w; pic.iPicHeight = h;
    pic.iStride[0] = w; pic.iStride[1] = w / 2; pic.iStride[2] = w / 2;
    pic.pData[0] = const_cast<unsigned char*>(i420);
    pic.pData[1] = const_cast<unsigned char*>(i420) + w * h;
    pic.pData[2] = const_cast<unsigned char*>(i420) + w * h + (w / 2) * (h / 2);

    SFrameBSInfo info; std::memset(&info, 0, sizeof(info));
    if (m_enc264->EncodeFrame(&pic, &info) != 0) return;
    if (info.eFrameType == videoFrameTypeInvalid || info.eFrameType == videoFrameTypeSkip) return;

    std::vector<std::byte> au;   // one access unit = all layers' NALs (Annex-B), concatenated
    for (int l = 0; l < info.iLayerNum; ++l) {
        const SLayerBSInfo& layer = info.sLayerInfo[l];
        int len = 0;
        for (int n = 0; n < layer.iNalCount; ++n) len += layer.pNalLengthInByte[n];
        if (len > 0)
            au.insert(au.end(), reinterpret_cast<const std::byte*>(layer.pBsBuf),
                      reinterpret_cast<const std::byte*>(layer.pBsBuf) + len);
    }
    if (au.empty()) return;
    // Friday 版：按真实时间打 90 kHz 时间戳（助手发的画面帧率不固定；原来每帧按 30 fps 递增）
    const auto now = std::chrono::steady_clock::now();
    if (m_videoEpoch == std::chrono::steady_clock::time_point{}) m_videoEpoch = now;
    m_vts = static_cast<uint32_t>(std::chrono::duration_cast<std::chrono::microseconds>(now - m_videoEpoch).count() * 9 / 100);
    if (m_videoTrack->isOpen()) {
        try { m_videoTrack->sendFrame(au.data(), au.size(), rtc::FrameInfo(m_vts)); } catch (...) {}
    }
}

void DcMediaEngine::handleIncomingVideo(const std::byte* data, size_t len) {
    if (len == 0) return;
    std::lock_guard<std::mutex> lk(m_encMutex);   // m_dec264 lives/dies under m_encMutex
    if (!m_dec264) return;
    unsigned char* dst[3] = {nullptr, nullptr, nullptr};
    SBufferInfo bi; std::memset(&bi, 0, sizeof(bi));
    if (m_dec264->DecodeFrameNoDelay(reinterpret_cast<const unsigned char*>(data),
                                     static_cast<int>(len), dst, &bi) != 0) {
        // Mid-GOP join / packet loss: ask the sender for a fresh IDR (≤1/s) instead of
        // silently rendering nothing until the next spontaneous keyframe (which a libwebrtc
        // sender never produces unless asked).
        const auto now = std::chrono::steady_clock::now();
        if (m_videoTrack && now - m_lastKeyframeReq >= std::chrono::seconds(1)) {
            m_lastKeyframeReq = now;
            try { m_videoTrack->requestKeyframe(); } catch (...) {}
        }
        return;
    }
    if (bi.iBufferStatus != 1 || !dst[0]) return;
    const int w  = bi.UsrData.sSystemBuffer.iWidth;
    const int h  = bi.UsrData.sSystemBuffer.iHeight;
    const int sy = bi.UsrData.sSystemBuffer.iStride[0];
    const int sc = bi.UsrData.sSystemBuffer.iStride[1];
    if (w <= 0 || h <= 0) return;
    std::vector<uint8_t> out(static_cast<size_t>(w) * h * 3 / 2);
    for (int r = 0; r < h; ++r)     std::memcpy(out.data() + r * w, dst[0] + r * sy, w);
    uint8_t* uOut = out.data() + static_cast<size_t>(w) * h;
    uint8_t* vOut = uOut + static_cast<size_t>(w / 2) * (h / 2);
    for (int r = 0; r < h / 2; ++r) std::memcpy(uOut + r * (w / 2), dst[1] + r * sc, w / 2);
    for (int r = 0; r < h / 2; ++r) std::memcpy(vOut + r * (w / 2), dst[2] + r * sc, w / 2);
    if (onDecodedVideo) onDecodedVideo(out.data(), w, h);
}

void DcMediaEngine::setupAudioTrack(std::shared_ptr<rtc::Track> track) {
    // Callee path: the reciprocal track has no send-SSRC yet. Declare ours so the answer
    // advertises it and the caller's SRTP accepts our outbound stream.
    int audioPT = kPayloadType;
    if (!m_caller) {
        auto d = track->description();
        pruneToFormat(d, "opus");
        stripExtMaps(d);   // same rule as video: promise no extensions we don't implement
        audioPT = negotiatedPayloadType(d, "opus", kPayloadType);
        d.addSSRC(m_ssrc, "schat-audio");
        track->setDescription(d);
    }
    auto rtpConfig = std::make_shared<rtc::RtpPacketizationConfig>(
        m_ssrc, "schat", audioPT, static_cast<uint32_t>(kRate));
    auto packetizer = std::make_shared<rtc::OpusRtpPacketizer>(rtpConfig);
    auto depacketizer = std::make_shared<rtc::OpusRtpDepacketizer>();
    packetizer->addToChain(depacketizer);   // outgoing packetize + incoming depacketize
    track->setMediaHandler(packetizer);
    track->onFrame([this](rtc::binary frame, rtc::FrameInfo) {
        handleIncomingFrame(frame.data(), frame.size());
    });
    track->onOpen([this]() { m_audioOpen = true; });
}

void DcMediaEngine::handleIncomingFrame(const std::byte* data, size_t len) {
    if (len == 0) return;
    int16_t pcm[kFrame * 2];
    int samples = 0;
    {
        std::lock_guard<std::mutex> lk(m_playMutex);   // m_dec lives/dies under m_playMutex
        if (!m_dec) return;
        samples = opus_decode(m_dec, reinterpret_cast<const unsigned char*>(data),
                              static_cast<opus_int32>(len), pcm, kFrame, 0);
        if (samples <= 0) return;
        for (int i = 0; i < samples; ++i) m_playQueue.push_back(pcm[i]);
        const size_t cap = static_cast<size_t>(kRate) * 4 / 10;   // ~400 ms jitter cap
        while (m_playQueue.size() > cap) m_playQueue.pop_front();
    }
    if (onDecodedAudio) onDecodedAudio(pcm, samples);
}

void DcMediaEngine::feedCapturePcm(const int16_t* pcm, int numSamples) {
    std::lock_guard<std::mutex> lk(m_sendMutex);   // null-check under the lock (same as video)
    if (!m_enc || !m_audioTrack) return;
    m_capAccum.insert(m_capAccum.end(), pcm, pcm + numSamples);
    while (static_cast<int>(m_capAccum.size()) >= kFrame) {
        unsigned char out[4000];
        int n = opus_encode(m_enc, m_capAccum.data(), kFrame, out, sizeof(out));
        m_capAccum.erase(m_capAccum.begin(), m_capAccum.begin() + kFrame);
        if (n <= 0) continue;
        m_rtpTs += kFrame;
        if (m_audioTrack->isOpen()) {
            try {
                m_audioTrack->sendFrame(reinterpret_cast<const std::byte*>(out),
                                        static_cast<size_t>(n), rtc::FrameInfo(m_rtpTs));
            } catch (...) {}
        }
    }
}

int DcMediaEngine::pullPlaybackPcm(int16_t* out, int maxSamples) {
    std::lock_guard<std::mutex> lk(m_playMutex);
    int n = 0;
    while (n < maxSamples && !m_playQueue.empty()) { out[n++] = m_playQueue.front(); m_playQueue.pop_front(); }
    return n;
}

void DcMediaEngine::onRemoteSdp(const std::string& type, const std::string& sdp) {
    if (!m_pc) return;
    // 同 onRemoteCandidate:setRemoteDescription/setLocalDescription 对坏 SDP/状态错误抛
    // 异常(如重连窗口内重放的旧 answer),未捕获 = 整进程 terminate。吞掉让通话自然超时。
    try {
        m_pc->setRemoteDescription(rtc::Description(sdp, type));   // callee: fires onTrack (sets SSRCs)
        if (type == "offer")
            m_pc->setLocalDescription();   // generate the answer now that our tracks carry SSRCs
    } catch (...) {}
}

void DcMediaEngine::onRemoteCandidate(const std::string& cand) {
    if (!m_pc) return;
    // 对端垃圾候选同样过滤(与出站 uselessLocalCandidate 对称):实测 Android libwebrtc
    // 会把回环口也枚举出来发 127.0.0.1/::1 的 host 候选(2026-07-16 手机→Linux 实抓),
    // 这些 pair 永远打不通还占检查配额;TUN fake-IP 段同理。
    if (uselessLocalCandidate(cand) || cand.find(" ::1 ") != std::string::npos) return;
    // libdatachannel 对畸形候选串/状态错误会抛 C++ 异常 —— 桌面端信令线程未捕获异常
    // = std::terminate 整个进程,一条坏候选不值得陪葬。
    try { m_pc->addRemoteCandidate(rtc::Candidate(cand)); } catch (...) {}
}

void DcMediaEngine::stop() {
    // Teardown races the capture/decode threads (camera dispatch queue, miniaudio callback,
    // libdatachannel workers): every member must be freed UNDER the mutex its feeder holds,
    // or a feeder mid-flight dereferences a just-nulled track/codec (the 「挂断即崩」 crash:
    // Track::isOpen on a reset shared_ptr while stop() waited on m_encMutex).
    m_audioOpen = false;
    m_videoOpen = false;
    if (m_pc) { try { m_pc->close(); } catch (...) {} m_pc.reset(); }
    {
        std::lock_guard<std::mutex> lk(m_sendMutex);   // vs feedCapturePcm
        m_audioTrack.reset();
        if (m_enc) { opus_encoder_destroy(m_enc); m_enc = nullptr; }
        m_capAccum.clear();
    }
    {
        std::lock_guard<std::mutex> lk(m_encMutex);    // vs feedCaptureFrame / handleIncomingVideo
        m_videoTrack.reset();
        if (m_enc264) { m_enc264->Uninitialize(); WelsDestroySVCEncoder(m_enc264); m_enc264 = nullptr; }
        if (m_dec264) { m_dec264->Uninitialize(); WelsDestroyDecoder(m_dec264); m_dec264 = nullptr; }
    }
    {
        std::lock_guard<std::mutex> lk(m_playMutex);   // vs handleIncomingFrame / pullPlaybackPcm
        if (m_dec) { opus_decoder_destroy(m_dec); m_dec = nullptr; }
        m_playQueue.clear();
    }
}

}  // namespace schat
