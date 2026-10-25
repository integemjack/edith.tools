# 通话内核（edith-friday 的 `calls` 特性）

助手的语音 / 视频通话、P2P 文字用（`docs/FRIDAY.md`「助手 › 通话」）。机器人那端要和 SChat 的 iOS / 安卓 / 桌面端在 WebRTC 上逐字节互通，
这里就是 Secret-Chat 桌面端已经和手机两端真机联调过的那一套 C++，Friday（Qt 版、Swift 版）用的也是它，原样搬过来。

- `schat/`：Secret-Chat（`integemjack/Secret-Chat`，同一作者）桌面端的通话内核，`desktop/desktop-qt/core/`，提交 `9bbc80670`（2026-09-25）：
  `include/schat/{DcMediaEngine,P2PCall,IceServer}.h`、`src/{DcMediaEngine,P2PCall}.cpp`。经 Friday 仓库 `qt/vendor/schat-call` 改过的版本（都标了「Friday 版」）：
  视频的 90 kHz 时间戳按真实时间算；`setVideoEncoding(码率, 帧率, 线程数)`；编码器用 `SEncParamExt` 初始化、多线程按条带并行编；
  视频轨道加了 `RtcpNackResponder`（丢包重传）和 `PacingHandler`（匀着发）。不用 Qt。
- `include/FridayCall.h`、`src/FridayCall.cpp`：C 接口（同 Friday 仓库 `Friday/Vendor/FridayCall`），edith-friday 的 `call_engine` 模块经 FFI 调。
  这里多了几个：`friday_text_create_labeled`（认别的 label 的通道：远程工作通道 `friday-work`）、`friday_text_send_binary`（二进制消息：工作通道的分片）、
  `friday_text_buffered`（通道上积着多少没发出去）；同一个连接上的别的通道（客户端设备的 `friday-serial:` / `friday-tcp:`）：`friday_text_accept_extra`、
  `friday_text_open_extra`、`friday_text_extra_send`、`friday_text_extra_close`，回调表多了 `on_extra_open` / `message` / `closed` / `drained`。
- `cmake/FridayCallDeps.cmake`：依赖，第一次配置时用 FetchContent 从 GitHub 拉源码——
  libdatachannel v0.24.5（MPL-2.0）、Mbed TLS 3.6.7（Apache-2.0）、Opus v1.5.2（BSD-3）、openh264 v2.6.0（BSD-2），各自按各自的许可证；
  这个目录里的其余代码随 edith.tools 一起公开。
- `CMakeLists.txt`：编成一组静态库；`../build.rs` 在 `cargo build --features calls` 时调 CMake（Release）编好、按顺序静态链进 edith-friday，
  再链 C++ 标准库（macOS libc++、Linux glibc libstdc++、Linux musl 用 zig 时 zig 的 libc++、Windows MSVC 自带）。

编的时候要 CMake（3.24 以上；有 ninja 用 ninja）和 C++17 编译器。环境变量：

- `FRIDAY_CALL_DEPS_DIR`：放着 `libdatachannel-src`、`mbedtls-src`、`opus-src`、`openh264-src` 的目录（如 Friday 的 qt 构建目录下的 `_deps`）：有就不下载。

Secret-Chat 那边改了这几个文件（尤其 SDP 处理、PLI / IDR、候选过滤）要同步过来。
