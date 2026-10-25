#pragma once
#include "schat/P2PCall.h"
#include "schat/IceServer.h"
#include <memory>
#include <string>
#include <cstdint>
#include <vector>
#include <deque>
#include <mutex>
#include <atomic>
#include <chrono>
#include <functional>

// libdatachannel-backed MediaEngine. Increment A established the rtc::PeerConnection
// (ICE/DTLS). Increment B adds a real Opus audio track: PCM in feedCapturePcm() is encoded
// and sent as SRTP, incoming SRTP is decoded to PCM drained via pullPlaybackPcm(). The
// platform drives the mic/speaker (miniaudio); a test can drive it with synthetic PCM.
typedef struct OpusEncoder OpusEncoder;
typedef struct OpusDecoder OpusDecoder;
class ISVCEncoder;   // openh264 (opaque)
class ISVCDecoder;
namespace rtc { class PeerConnection; class Track; }

namespace schat {

class DcMediaEngine : public MediaEngine {
public:
    DcMediaEngine();
    ~DcMediaEngine() override;

    bool start(bool video, bool asCaller) override;
    void stop() override;

    // Extra ICE servers (TURN relay from the server ack) appended to the built-in STUN list at
    // start(). Must be set BEFORE start() (a per-call/per-peer engine is started once). Empty =
    // STUN-only, unchanged.
    void setIceServers(std::vector<IceServer> servers) { m_iceServers = std::move(servers); }
    void onRemoteSdp(const std::string& type, const std::string& sdp) override;
    void onRemoteCandidate(const std::string& candidate) override;
    bool isReal() const override { return true; }

    // Audio bridge — 48 kHz mono int16. Called by the platform audio device or a test.
    void feedCapturePcm(const int16_t* pcm, int numSamples);   // mic → Opus encode → SRTP
    int  pullPlaybackPcm(int16_t* out, int maxSamples);        // decoded SRTP → speaker
    std::function<void(const int16_t*, int)> onDecodedAudio;   // optional tap (tests)
    bool audioReady() const { return m_audioOpen.load(); }

    // Video bridge (Increment C) — planar I420 frames. Driven by the camera or a test.
    void feedCaptureFrame(const uint8_t* i420, int width, int height);   // → H.264 encode → SRTP
    // Friday 版：H.264 的目标码率、实际送帧的帧率（openh264 按这个帧率分每帧的码率；原来写死 800 kbps / 30 帧，
    // 助手每秒只送 15 帧时实际只用到一半）和编码线程数（多于 1 时按同样多的条带并行编）。改了下一帧重建编码器（出 IDR）
    void setVideoEncoding(int bitrateBps, float frameRate, int threads = 1);
    std::function<void(const uint8_t* i420, int w, int h)> onDecodedVideo;
    bool videoReady() const { return m_videoOpen.load(); }

    static constexpr int kRate = 48000;
    static constexpr int kFrame = 960;     // 20 ms @ 48 kHz
    static constexpr int kVideoClock = 90000;
    // Friday 版：视频按这个速度匀着发（比助手最高一档 1.5 Mbps 宽裕，关键帧也能很快发完，又不会一下子全倒出去）
    static constexpr double kVideoPacingBps = 4000000;

private:
    void setupAudioTrack(std::shared_ptr<rtc::Track> track);
    void setupVideoTrack(std::shared_ptr<rtc::Track> track);
    void handleIncomingFrame(const std::byte* data, size_t len);
    void handleIncomingVideo(const std::byte* data, size_t len);
    bool initEncoder(int w, int h);

    std::shared_ptr<rtc::PeerConnection> m_pc;
    std::shared_ptr<rtc::Track> m_audioTrack;
    std::shared_ptr<rtc::Track> m_videoTrack;
    OpusEncoder* m_enc = nullptr;
    OpusDecoder* m_dec = nullptr;
    ISVCEncoder* m_enc264 = nullptr;
    ISVCDecoder* m_dec264 = nullptr;
    uint32_t m_ssrc = 0;
    uint32_t m_rtpTs = 0;
    uint32_t m_vssrc = 0;
    uint32_t m_vts = 0;
    int m_encW = 0, m_encH = 0;
    int m_encBitrate = 800000;           // Friday 版：setVideoEncoding；guarded by m_encMutex
    float m_encFps = 30;
    int m_encThreads = 1;
    std::vector<int16_t> m_capAccum;     // accumulate mic samples into 20 ms frames
    std::deque<int16_t>  m_playQueue;    // decoded PCM awaiting playback
    std::mutex m_playMutex;
    std::mutex m_sendMutex;
    std::mutex m_encMutex;               // guards the H.264 encoder
    std::atomic<bool> m_audioOpen{false};
    std::atomic<bool> m_videoOpen{false};
    std::atomic<bool> m_forceIdr{false};   // PLI from the peer / track (re)open → next encode is an IDR
    std::chrono::steady_clock::time_point m_lastKeyframeReq{};   // guarded by m_encMutex
    std::chrono::steady_clock::time_point m_videoEpoch{};        // Friday：第一帧的时刻（视频时间戳按真实时间算）
    bool m_caller = false;
    bool m_wantVideo = false;
    std::vector<IceServer> m_iceServers;   // TURN relays injected from the server ack (set before start)
};

}  // namespace schat
