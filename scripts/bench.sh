#!/usr/bin/env bash
# GoRangeTask 性能基线的固定跑法。
#
# 目的：让 docs/GoRangeTask-性能基线.md 里的每一个数字都能被一条命令复现——
# 参数、库名、payload 组合、并发档位都写死在这里，而不是散落在文档的散文里。
#
# 用法: ./scripts/bench.sh [额外的 bench 参数...]
#   例: ./scripts/bench.sh                     # 完整矩阵，结果写到 bench/results/
#       ./scripts/bench.sh -scenario drain -tasks 200   # 只跑排空，缩小规模
set -euo pipefail

CONTAINER=gotask-mysql
DB=gotask_bench
DSN="root:rootpw@tcp(127.0.0.1:3306)/${DB}?parseTime=true&loc=UTC&timeout=5s&readTimeout=30s&writeTimeout=30s"

cd "$(dirname "$0")/.."

# 集成测试与基线都依赖容器里的 MySQL；没起就别浪费时间跑出一个空结果。
if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
  echo "容器 $CONTAINER 未运行，先执行: ./scripts/dev-mysql.sh up" >&2
  exit 1
fi

TS=$(date +%Y%m%dT%H%M%S)
OUT="bench/results/${TS}-baseline.json"

if [ "$#" -gt 0 ]; then
  # 调用方覆盖参数时也仍然写 JSON，只是文件名带 -custom 以便区分证据来源。
  OUT="bench/results/${TS}-custom.json"
  exec env GOTASK_BENCH_DSN="$DSN" go run ./bench -out "$OUT" "$@"
fi

exec env GOTASK_BENCH_DSN="$DSN" go run ./bench \
  -scenario all \
  -tasks 1000 \
  -workers 1,4,16 \
  -sizes 256,4096,32768 \
  -producers 8 \
  -enqueues 800 \
  -idle 20s \
  -timeout 5m \
  -container "$CONTAINER" \
  -out "$OUT"
