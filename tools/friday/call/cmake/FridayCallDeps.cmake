# 通话内核（../CMakeLists.txt）要的第三方库，全部静态编进 edith-friday（同 Friday 仓库 qt/cmake/FridayCallDeps.cmake）：
#   - libdatachannel v0.24.5（MPL-2.0；WebRTC：ICE / DTLS / SRTP，自带 libjuice、libsrtp、usrsctp、plog 子模块）
#   - Mbed TLS 3.6.7（Apache-2.0；libdatachannel 的 DTLS）
#   - Opus v1.5.2（BSD-3；语音编解码）
#   - openh264 v2.6.0（BSD-2；H.264 编解码。没有 CMake：按它的 meson.build 列源文件编成静态库，不用汇编）
# 版本与 Secret-Chat 桌面端一致：通话的另一端就是 SChat 的 iOS / 安卓 / 桌面端。
#
# 源码用 FetchContent 拉（压缩包校验 SHA-256，git 按标签）。本机已经有源码时用 CMake 自带的覆盖变量指过去，不再下载
# （build.rs 认环境变量 FRIDAY_CALL_DEPS_DIR：放着 libdatachannel-src、mbedtls-src、opus-src、openh264-src 的目录）：
#   -DFETCHCONTENT_SOURCE_DIR_LIBDATACHANNEL=…  -DFETCHCONTENT_SOURCE_DIR_OPUS=…  -DFETCHCONTENT_SOURCE_DIR_OPENH264=…  -DFETCHCONTENT_SOURCE_DIR_MBEDTLS=…
# 结果：目标 friday_call_deps（INTERFACE），链上它就有 rtc/、opus、wels/ 的头文件和库。
include_guard(GLOBAL)
include(FetchContent)

enable_language(C)

# 第三方代码一律不报警告、不当错误
function(friday_quiet_target target)
    if(NOT TARGET ${target})
        return()
    endif()
    get_target_property(_type ${target} TYPE)
    if(_type STREQUAL "INTERFACE_LIBRARY")
        return()
    endif()
    if(MSVC)
        target_compile_options(${target} PRIVATE /W0)
    else()
        target_compile_options(${target} PRIVATE -w)
    endif()
    set_target_properties(${target} PROPERTIES POSITION_INDEPENDENT_CODE ON)
endfunction()

# 子项目都按静态库编，不装、不编示例和测试
set(BUILD_SHARED_LIBS OFF)
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)

# ---- Mbed TLS ----------------------------------------------------------------------------
FetchContent_Declare(mbedtls
    URL https://github.com/Mbed-TLS/mbedtls/releases/download/mbedtls-3.6.7/mbedtls-3.6.7.tar.bz2
    URL_HASH SHA256=a7e8bcbec0e6f761b4af24f25677626b35f762f68eef79c08677a363212d11f6
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    EXCLUDE_FROM_ALL)
set(ENABLE_PROGRAMS OFF CACHE BOOL "" FORCE)
set(ENABLE_TESTING OFF CACHE BOOL "" FORCE)
set(MBEDTLS_FATAL_WARNINGS OFF CACHE BOOL "" FORCE)
set(USE_SHARED_MBEDTLS_LIBRARY OFF CACHE BOOL "" FORCE)
set(USE_STATIC_MBEDTLS_LIBRARY ON CACHE BOOL "" FORCE)
set(GEN_FILES OFF CACHE BOOL "" FORCE)
set(DISABLE_PACKAGE_CONFIG_AND_INSTALL ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(mbedtls)
# 追加配置（打开 DTLS-SRTP）：Mbed TLS 自己和用它头文件的 libdatachannel 必须用同一份
set(FRIDAY_MBEDTLS_CONFIG_DIR "${CMAKE_CURRENT_LIST_DIR}/mbedtls")
foreach(_t mbedtls mbedx509 mbedcrypto)
    friday_quiet_target(${_t})
    target_include_directories(${_t} PUBLIC "${FRIDAY_MBEDTLS_CONFIG_DIR}")
    target_compile_definitions(${_t} PUBLIC "MBEDTLS_USER_CONFIG_FILE=\"friday_mbedtls_config.h\"")
endforeach()
# libdatachannel 认这个目标名（有了就不再 find_package）。要是 IMPORTED：它的 install(EXPORT) 不肯导出依赖普通目标的库
if(NOT TARGET MbedTLS::MbedTLS)
    add_library(MbedTLS::MbedTLS INTERFACE IMPORTED GLOBAL)
    set_target_properties(MbedTLS::MbedTLS PROPERTIES INTERFACE_LINK_LIBRARIES "mbedtls;mbedx509;mbedcrypto")
endif()

# ---- libdatachannel ----------------------------------------------------------------------
FetchContent_Declare(libdatachannel
    GIT_REPOSITORY https://github.com/paullouisageneau/libdatachannel.git
    GIT_TAG v0.24.5
    GIT_SHALLOW TRUE
    GIT_SUBMODULES deps/libjuice deps/libsrtp deps/usrsctp deps/plog deps/json
    GIT_SUBMODULES_RECURSE FALSE
    # 只要静态库：链到谁才编谁
    EXCLUDE_FROM_ALL)
set(USE_MBEDTLS ON CACHE BOOL "" FORCE)
set(USE_GNUTLS OFF CACHE BOOL "" FORCE)
# libSRTP 用自带的加密（AES-CM + HMAC-SHA1，SRTP 默认的套件），不再拉 Mbed TLS 进去
set(ENABLE_MBEDTLS OFF CACHE BOOL "" FORCE)
set(ENABLE_OPENSSL OFF CACHE BOOL "" FORCE)
set(NO_WEBSOCKET ON CACHE BOOL "" FORCE)
set(NO_EXAMPLES ON CACHE BOOL "" FORCE)
set(NO_TESTS ON CACHE BOOL "" FORCE)
set(USE_NICE OFF CACHE BOOL "" FORCE)
# usrsctp 默认把警告当错误（新编译器上会冒新警告）
set(sctp_werror OFF CACHE BOOL "" FORCE)
set(sctp_build_programs OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(libdatachannel)
foreach(_t datachannel-static juice-static srtp2 usrsctp)
    friday_quiet_target(${_t})
endforeach()

# ---- Opus --------------------------------------------------------------------------------
FetchContent_Declare(opus
    GIT_REPOSITORY https://github.com/xiph/opus.git
    GIT_TAG v1.5.2
    GIT_SHALLOW TRUE
    EXCLUDE_FROM_ALL)
set(OPUS_BUILD_PROGRAMS OFF CACHE BOOL "" FORCE)
set(OPUS_BUILD_TESTING OFF CACHE BOOL "" FORCE)
set(OPUS_BUILD_SHARED_LIBRARY OFF CACHE BOOL "" FORCE)
set(OPUS_INSTALL_PKG_CONFIG_MODULE OFF CACHE BOOL "" FORCE)
set(OPUS_INSTALL_CMAKE_CONFIG_MODULE OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(opus)
friday_quiet_target(opus)

# ---- openh264 ----------------------------------------------------------------------------
FetchContent_Declare(openh264
    GIT_REPOSITORY https://github.com/cisco/openh264.git
    GIT_TAG v2.6.0
    GIT_SHALLOW TRUE
    # 它没有 CMakeLists：指一个不存在的子目录，MakeAvailable 只拉源码，下面自己列源文件
    SOURCE_SUBDIR friday-no-cmake)
FetchContent_MakeAvailable(openh264)
set(_h264 ${openh264_SOURCE_DIR}/codec)
# 源文件列表照 codec/{common,encoder,decoder,processing}/meson.build（不含汇编：纯 C++ 实现）。
# 和 meson 一样分成四个静态库：编码器、解码器有同名的头文件（deblocking.h 等），include 路径不能混
find_package(Threads REQUIRED)
function(friday_openh264_lib name)
    cmake_parse_arguments(ARG "" "" "SOURCES;INCLUDES" ${ARGN})
    add_library(${name} STATIC ${ARG_SOURCES})
    target_include_directories(${name} PUBLIC ${_h264}/api PRIVATE ${_h264}/api/wels ${_h264}/common/inc ${ARG_INCLUDES})
    target_link_libraries(${name} PRIVATE Threads::Threads)
    if(MSVC)
        target_compile_definitions(${name} PRIVATE _CRT_SECURE_NO_WARNINGS NOMINMAX)
    endif()
    friday_quiet_target(${name})
endfunction()
set(_h264_common
    common_tables.cpp copy_mb.cpp cpu.cpp crt_util_safe_x.cpp deblocking_common.cpp expand_pic.cpp intra_pred_common.cpp
    mc.cpp memory_align.cpp sad_common.cpp utils.cpp welsCodecTrace.cpp WelsTaskThread.cpp WelsThread.cpp
    WelsThreadLib.cpp WelsThreadPool.cpp)
list(TRANSFORM _h264_common PREPEND ${_h264}/common/src/)
friday_openh264_lib(friday_openh264_common SOURCES ${_h264_common})
file(GLOB_RECURSE _h264_processing ${_h264}/processing/src/*.cpp)
friday_openh264_lib(friday_openh264_processing SOURCES ${_h264_processing}
    INCLUDES ${_h264}/processing/interface ${_h264}/processing/src/common ${_h264}/processing/src/adaptivequantization
             ${_h264}/processing/src/downsample ${_h264}/processing/src/scrolldetection ${_h264}/processing/src/vaacalc)
set(_h264_encoder
    core/src/au_set.cpp core/src/deblocking.cpp core/src/decode_mb_aux.cpp core/src/encode_mb_aux.cpp core/src/encoder.cpp
    core/src/encoder_data_tables.cpp core/src/encoder_ext.cpp core/src/get_intra_predictor.cpp core/src/md.cpp
    core/src/mv_pred.cpp core/src/nal_encap.cpp core/src/paraset_strategy.cpp core/src/picture_handle.cpp core/src/ratectl.cpp
    core/src/ref_list_mgr_svc.cpp core/src/sample.cpp core/src/set_mb_syn_cabac.cpp core/src/set_mb_syn_cavlc.cpp
    core/src/slice_multi_threading.cpp core/src/svc_base_layer_md.cpp core/src/svc_enc_slice_segment.cpp
    core/src/svc_encode_mb.cpp core/src/svc_encode_slice.cpp core/src/svc_mode_decision.cpp core/src/svc_motion_estimate.cpp
    core/src/svc_set_mb_syn_cabac.cpp core/src/svc_set_mb_syn_cavlc.cpp core/src/wels_preprocess.cpp
    core/src/wels_task_base.cpp core/src/wels_task_encoder.cpp core/src/wels_task_management.cpp plus/src/welsEncoderExt.cpp)
list(TRANSFORM _h264_encoder PREPEND ${_h264}/encoder/)
friday_openh264_lib(friday_openh264_encoder SOURCES ${_h264_encoder}
    INCLUDES ${_h264}/encoder/core/inc ${_h264}/encoder/plus/inc ${_h264}/processing/interface)
file(GLOB _h264_decoder ${_h264}/decoder/core/src/*.cpp ${_h264}/decoder/plus/src/*.cpp)
friday_openh264_lib(friday_openh264_decoder SOURCES ${_h264_decoder}
    INCLUDES ${_h264}/decoder/core/inc ${_h264}/decoder/plus/inc)
add_library(friday_openh264 INTERFACE)
target_link_libraries(friday_openh264 INTERFACE friday_openh264_encoder friday_openh264_decoder friday_openh264_processing
    friday_openh264_common Threads::Threads)

# ---- 汇总 --------------------------------------------------------------------------------
add_library(friday_call_deps INTERFACE)
target_link_libraries(friday_call_deps INTERFACE datachannel-static opus friday_openh264)
