#pragma once
// 通话内核的 C 接口（edith-friday 的 call_engine 模块经 FFI 调；同 Friday 仓库 Friday/Vendor/FridayCall 给 Swift 用的那一份）。底下是 Secret-Chat 桌面端的
// 通话内核（schat/：P2PCallManager + DcMediaEngine，libdatachannel / Mbed TLS / Opus / openh264，静态链进去，见同目录上一层的 CMakeLists.txt 和 build.rs）。
// 另加 P2P 文字用的 DataChannel 连接。
//
// 线程：内核的回调来自 libdatachannel 的线程。除了解出来的音视频（on_decoded_audio / on_decoded_video，直接在引擎线程上调，平台自己加锁），
// 其余回调都先排进队列、调 wake 叫醒平台，平台在主线程上调 friday_*_drain 时才真正调出来——所以平台只在主线程上收到状态、信令。
// 其余函数都在主线程上调（例外：friday_call_feed_capture_pcm / feed_capture_frame / set_video_encoding 任意线程都能调，
// 引擎自己加锁——同 Qt 版在画面、编码线程上调）。字符串都是 UTF-8、以 NUL 结尾，只在这次调用里有效。
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// ---- 一通语音 / 视频电话（P2PCallManager + DcMediaEngine） ----------------------------------------------

typedef struct friday_call friday_call;

/// 同 schat::CallState
typedef enum {
    FRIDAY_CALL_IDLE = 0,
    FRIDAY_CALL_OUTGOING = 1,
    FRIDAY_CALL_INCOMING = 2,
    FRIDAY_CALL_CONNECTING = 3,
    FRIDAY_CALL_CONNECTED = 4,
    FRIDAY_CALL_ENDED = 5,
} friday_call_state;

typedef struct {
    void* ctx;
    /// 有排着的回调了：在主线程上调 friday_call_drain（任意线程上调这个）
    void (*wake)(void* ctx);
    /// 状态机（friday_call_state）
    void (*on_state)(void* ctx, int state);
    /// 对方当场拒了（user_rejected / busy / not_friends / …）
    void (*on_rejected)(void* ctx, const char* reason);
    void (*on_incoming)(void* ctx, int from_user_id, const char* request_id, int video);
    /// 要发出去的信令（内核的说法：type 是 offer / answer / ice / hangup / request_cancel，SDP 和候选分开；平台换成线上的写法再发）
    void (*send_request)(void* ctx, int to_user_id, const char* request_id, const char* call_type);
    void (*send_response)(void* ctx, int target_user_id, const char* request_id, int accepted, const char* reason);
    void (*send_signaling)(void* ctx, int to_user_id, const char* request_id, const char* type, const char* sdp, const char* candidate);
    /// 解出来的对方声音（48 kHz 单声道 16 位）、画面（I420）：在引擎线程上直接调
    void (*on_decoded_audio)(void* ctx, const int16_t* pcm, int samples);
    void (*on_decoded_video)(void* ctx, const uint8_t* i420, int width, int height);
} friday_call_callbacks;

friday_call* friday_call_create(const friday_call_callbacks* callbacks);
/// 不发信令地收掉、停引擎、放掉；之后不再回调
void friday_call_destroy(friday_call* call);
/// 主线程上把排着的回调调出来
void friday_call_drain(friday_call* call);

void friday_call_start(friday_call* call, int peer_id, int video);
void friday_call_accept(friday_call* call);
void friday_call_reject(friday_call* call);
void friday_call_hangup(friday_call* call);
/// 不发信令地收尾（没人接、两边同时打输了）
void friday_call_abort_local(friday_call* call);

/// 服务器给的 TURN 中继（每个地址一条；用户名、密码可以为空）：开始前设，接在内置的 STUN 后面
void friday_call_set_ice_servers(friday_call* call, const char* const* urls, const char* const* usernames,
                                 const char* const* credentials, int count);

/// 收到的信令（内核的说法；平台已经从线上的写法换过来）
void friday_call_on_request(friday_call* call, int from_user_id, const char* request_id, const char* call_type);
void friday_call_on_response(friday_call* call, int from_user_id, const char* request_id, int accepted, const char* reason);
void friday_call_on_signaling(friday_call* call, int from_user_id, const char* request_id, const char* type, const char* sdp,
                              const char* candidate);

/// 送进去的声音（48 kHz 单声道 16 位，攒够 20 毫秒编一帧 Opus）、画面（I420，宽高偶数）
void friday_call_feed_capture_pcm(friday_call* call, const int16_t* pcm, int samples);
void friday_call_feed_capture_frame(friday_call* call, const uint8_t* i420, int width, int height);
/// H.264 的目标码率、送帧的帧率、编码线程数（改了下一帧重建编码器）
void friday_call_set_video_encoding(friday_call* call, int bitrate_bps, float frame_rate, int threads);

int friday_call_get_state(friday_call* call);
int friday_call_is_video(friday_call* call);
/// 这通电话的 requestId（写进 out，最多 cap - 1 字节）
void friday_call_request_id(friday_call* call, char* out, int cap);

// ---- P2P 文字：一条只有 DataChannel 的连接（手机主叫：它建通道、出 offer，这边 answer） ----------------------------

typedef struct friday_text_peer friday_text_peer;

typedef struct {
    void* ctx;
    void (*wake)(void* ctx);
    /// 本机的 SDP（answer）、ICE 候选
    void (*on_local_description)(void* ctx, const char* type, const char* sdp);
    void (*on_local_candidate)(void* ctx, const char* candidate, const char* mid);
    /// 连接失败 / 关了
    void (*on_failed)(void* ctx);
    /// 通道（label 以 p2p-text 开头的）开了、关了；手机重建通道时换成新的那条
    void (*on_channel_open)(void* ctx);
    void (*on_channel_closed)(void* ctx);
    void (*on_message)(void* ctx, const uint8_t* data, int length);
} friday_text_callbacks;

/// 建连接（ICE 服务器同 friday_call_set_ice_servers 的写法，接在内置的 STUN 后面）；建不了返回 NULL
friday_text_peer* friday_text_create(const friday_text_callbacks* callbacks, const char* const* urls, const char* const* usernames,
                                     const char* const* credentials, int count);
void friday_text_destroy(friday_text_peer* peer);
void friday_text_drain(friday_text_peer* peer);
/// 设对方的 offer、出 answer；SDP 用不了返回非 0
int friday_text_set_offer(friday_text_peer* peer, const char* sdp);
void friday_text_add_candidate(friday_text_peer* peer, const char* candidate, const char* mid);
/// 在通道上发一条文字；发不出去返回非 0
int friday_text_send(friday_text_peer* peer, const char* text);

// ---- 测试用：主叫那端（建通道、出 offer），在自检里扮手机开 P2P 文字 ----

/// 建一条只连本机的连接（不要 STUN）、建通道（label）、出 offer（on_local_description）；通道开了调 on_channel_open
friday_text_peer* friday_text_create_offerer(const friday_text_callbacks* callbacks, const char* label);
/// 设对方的 answer；SDP 用不了返回非 0
int friday_text_set_answer(friday_text_peer* peer, const char* sdp);

#ifdef __cplusplus
}
#endif
