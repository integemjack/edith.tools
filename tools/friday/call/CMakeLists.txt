# edith-friday 的通话内核（README.md）：Secret-Chat 桌面端的 P2PCallManager + DcMediaEngine（schat/）+ C 接口（src/FridayCall.cpp、
# include/FridayCall.h）+ 依赖（cmake/FridayCallDeps.cmake：libdatachannel、Mbed TLS、Opus、openh264）。
# build.rs 在打开 calls 特性时用它编成一组静态库，把每个库的路径写进 <构建目录>/libs-<配置>.txt（一行一个，按链接顺序），再一个个静态链进 edith-friday。
cmake_minimum_required(VERSION 3.24)
project(EdithFridayCall LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

include("${CMAKE_CURRENT_LIST_DIR}/cmake/FridayCallDeps.cmake")

add_library(fridaycall_core STATIC
    schat/src/DcMediaEngine.cpp
    schat/src/P2PCall.cpp
    src/FridayCall.cpp)
target_include_directories(fridaycall_core PUBLIC include schat/include)
target_link_libraries(fridaycall_core PUBLIC friday_call_deps)
if(MSVC)
    target_compile_options(fridaycall_core PRIVATE /W0 /utf-8)
    target_compile_definitions(fridaycall_core PRIVATE NOMINMAX _CRT_SECURE_NO_WARNINGS)
else()
    target_compile_options(fridaycall_core PRIVATE -w)
endif()

# 链接顺序：用的在前、被用的在后（GNU ld 按顺序找）
set(_parts fridaycall_core datachannel-static juice-static srtp2 usrsctp mbedtls mbedx509 mbedcrypto opus
    friday_openh264_encoder friday_openh264_decoder friday_openh264_processing friday_openh264_common)
set(_lines "")
foreach(_part ${_parts})
    string(APPEND _lines "$<TARGET_FILE:${_part}>\n")
endforeach()
file(GENERATE OUTPUT "${CMAKE_BINARY_DIR}/libs-$<CONFIG>.txt" CONTENT "${_lines}")
add_custom_target(fridaycall ALL DEPENDS ${_parts})
