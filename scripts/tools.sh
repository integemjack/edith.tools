#!/usr/bin/env bash
# 有哪些工具（Cargo.toml 里的 [[bin]]，一行一个）
sed -n '/^\[\[bin\]\]/,/^$/s/^name = "\(.*\)"/\1/p' "$(dirname "$0")/../Cargo.toml"
