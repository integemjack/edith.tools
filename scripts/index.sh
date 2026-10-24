#!/usr/bin/env bash
# 重新生成 index.json：取每个工具最新 Release 上的清单片段（<工具>.json）合起来，挂到固定的 index Release 上（标成 Latest，
# 所以 releases/latest/download/index.json 一直指向它）。几个工具同时发版时互相不等：合完隔一会儿再合一遍，有变化再传。
# 用法：GH_TOKEN=… bash scripts/index.sh <仓库>
set -euo pipefail
shopt -s nullglob
repository="$1"
root="$(cd "$(dirname "$0")/.." && pwd)"
work="$(mktemp -d)"

build() {
  rm -rf "$work/parts" && mkdir -p "$work/parts"
  local releases
  releases=$(gh release list --repo "$repository" --limit 200 --json tagName,createdAt -q 'sort_by(.createdAt) | reverse | .[].tagName')
  for manifest in "$root"/tools/*/Cargo.toml; do
    local tool tag
    tool=$(sed -n 's/^name = "\(.*\)"/\1/p' "$manifest" | head -1)
    tag=$(printf '%s\n' "$releases" | { grep -E "^${tool}-v[0-9]+\.[0-9]+\.[0-9]+$" || true; } | head -1)
    if [ -n "$tag" ]; then
      gh release download "$tag" --repo "$repository" -p "$tool.json" -D "$work/parts" --clobber
    fi
  done
  python3 "$root/scripts/index.py" merge "$repository" "$work"/parts/*.json > "$work/index.json"
}

upload() {
  if ! gh release view index --repo "$repository" > /dev/null 2>&1; then
    gh release create index --repo "$repository" --target "${GITHUB_SHA:-main}" --title "EDITH 工具索引" \
      --notes "index.json：每个工具最新的版本、每个平台的包（大脑的 edith tools 读它）。工具各自发版，见各自的 Release。" --latest
  fi
  gh release upload index "$work/index.json" --repo "$repository" --clobber
  gh release edit index --repo "$repository" --latest
}

build
upload
cp "$work/index.json" "$work/uploaded.json"
# 同时发版的另一个工具可能在我们读完之后才挂上片段：过一会儿再合一遍
sleep 20
build
if ! cmp -s <(python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); d.pop("version"); print(json.dumps(d, sort_keys=True))' "$work/index.json") \
            <(python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); d.pop("version"); print(json.dumps(d, sort_keys=True))' "$work/uploaded.json"); then
  upload
fi
cat "$work/index.json"
