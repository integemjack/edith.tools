//! 打开 calls 特性时编通话内核（call/，CMake，见 call/README.md）并静态链进来；没开什么也不做（没有 CMake 也能编）。
//! 构建目录在 <target 目录>[/<目标平台>]/call-engine：debug、release、测试共用一份（第一次要拉依赖、编几分钟）。

use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

fn run(command: &mut Command, what: &str) {
    let status = command.status().unwrap_or_else(|error| panic!("{what}：跑不起来 {command:?}：{error}（要装 CMake）"));
    if !status.success() {
        panic!("{what}失败：{command:?}");
    }
}

fn found(program: &str) -> bool {
    Command::new(program).arg("--version").output().is_ok_and(|output| output.status.success())
}

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    if env::var_os("CARGO_FEATURE_CALLS").is_none() {
        return;
    }
    let manifest = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let source = manifest.join("call");
    for part in ["CMakeLists.txt", "cmake", "include", "src", "schat"] {
        println!("cargo:rerun-if-changed={}", source.join(part).display());
    }
    println!("cargo:rerun-if-env-changed=FRIDAY_CALL_DEPS_DIR");
    let target = env::var("TARGET").unwrap();
    let target_env = target.replace('-', "_");
    for name in ["CMAKE_TOOLCHAIN_FILE", "CC", "CXX", "AR"] {
        println!("cargo:rerun-if-env-changed={name}_{target_env}");
    }
    let os = env::var("CARGO_CFG_TARGET_OS").unwrap();
    let arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap();
    let msvc = env::var("CARGO_CFG_TARGET_ENV").unwrap() == "msvc";
    let out = PathBuf::from(env::var("OUT_DIR").unwrap());
    // OUT_DIR = <target 目录>[/<目标平台>]/<配置>/build/<包>/out
    let build = out.ancestors().nth(4).unwrap_or(&out).join("call-engine");

    let multi = msvc;
    if !build.join("CMakeCache.txt").exists() {
        let mut configure = Command::new("cmake");
        configure.arg("-S").arg(&source).arg("-B").arg(&build).arg("-DCMAKE_BUILD_TYPE=Release").arg("-DCMAKE_POLICY_VERSION_MINIMUM=3.5");
        if msvc {
            // Visual Studio 生成器（自己找 MSVC）：按目标选架构；Rust 默认 /MD，CMake 的 Release 也是 /MD
            let platform = match arch.as_str() {
                "x86" => "Win32",
                "aarch64" => "ARM64",
                _ => "x64",
            };
            configure.arg("-A").arg(platform);
        } else if found("ninja") {
            configure.arg("-G").arg("Ninja");
        }
        if os == "macos" {
            configure.arg(format!("-DCMAKE_OSX_ARCHITECTURES={}", if arch == "aarch64" { "arm64" } else { "x86_64" }));
            let minimum = env::var("MACOSX_DEPLOYMENT_TARGET").unwrap_or_else(|_| "11.0".into());
            configure.arg(format!("-DCMAKE_OSX_DEPLOYMENT_TARGET={minimum}"));
        }
        // cargo-zigbuild（Linux musl）给 CMake 备好了工具链文件；别的交叉编译按 CC_<目标> / CXX_<目标>
        if let Ok(toolchain) = env::var(format!("CMAKE_TOOLCHAIN_FILE_{target_env}")) {
            configure.arg(format!("-DCMAKE_TOOLCHAIN_FILE={toolchain}"));
        } else if !msvc {
            for (variable, option) in [("CC", "CMAKE_C_COMPILER"), ("CXX", "CMAKE_CXX_COMPILER"), ("AR", "CMAKE_AR")] {
                if let Ok(program) = env::var(format!("{variable}_{target_env}")) {
                    configure.arg(format!("-D{option}={program}"));
                }
            }
        }
        if let Ok(deps) = env::var("FRIDAY_CALL_DEPS_DIR") {
            let deps = Path::new(&deps);
            for (name, folder) in
                [("LIBDATACHANNEL", "libdatachannel-src"), ("MBEDTLS", "mbedtls-src"), ("OPUS", "opus-src"), ("OPENH264", "openh264-src")]
            {
                configure.arg(format!("-DFETCHCONTENT_SOURCE_DIR_{name}={}", deps.join(folder).display()));
            }
        }
        run(&mut configure, "配置通话内核");
    }
    let jobs = std::thread::available_parallelism().map(|count| count.get()).unwrap_or(4).to_string();
    let mut compile = Command::new("cmake");
    compile.arg("--build").arg(&build).arg("--target").arg("fridaycall").arg("--parallel").arg(&jobs);
    if multi {
        compile.arg("--config").arg("Release");
    }
    run(&mut compile, "编通话内核");

    // 每个静态库按顺序链进来
    let list = build.join("libs-Release.txt");
    let libraries = std::fs::read_to_string(&list).unwrap_or_else(|error| panic!("没有 {}：{error}", list.display()));
    let mut searched = Vec::new();
    for line in libraries.lines().map(str::trim).filter(|line| !line.is_empty()) {
        let path = Path::new(line);
        let directory = path.parent().unwrap().to_path_buf();
        if !searched.contains(&directory) {
            println!("cargo:rustc-link-search=native={}", directory.display());
            searched.push(directory);
        }
        let stem = path.file_stem().unwrap().to_string_lossy();
        let name = if msvc { stem.to_string() } else { stem.strip_prefix("lib").unwrap_or(&stem).to_string() };
        println!("cargo:rustc-link-lib=static={name}");
    }
    // C++ 标准库、系统库
    match os.as_str() {
        "macos" | "ios" => println!("cargo:rustc-link-lib=c++"),
        "windows" => {
            for library in ["ws2_32", "iphlpapi", "bcrypt", "advapi32"] {
                println!("cargo:rustc-link-lib={library}");
            }
        }
        _ => {
            // zig（cargo-zigbuild）带的是 libc++；系统的 gcc 是 libstdc++
            let zig = env::var(format!("CMAKE_TOOLCHAIN_FILE_{target_env}")).is_ok()
                || env::var(format!("CXX_{target_env}")).is_ok_and(|cxx| cxx.contains("zig"));
            println!("cargo:rustc-link-lib={}", if zig { "c++" } else { "stdc++" });
            println!("cargo:rustc-link-lib=pthread");
        }
    }
}
