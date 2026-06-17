#!/usr/bin/env bash
# 构建运行记录持久化插件（Go plugin / .so）。
#
# 用法：
#   ./build-plugins.sh                 # 构建全部插件
#   ./build-plugins.sh logfile bbolt   # 仅构建指定插件
#
# 注意（Go plugin 硬性约束）：
#   1. 仅支持 Linux / macOS；
#   2. 插件 .so 必须与主程序 dag-app 用【相同的 Go 版本与依赖版本】编译，
#      否则运行时 plugin.Open 会报 "different version of package" 之类错误；
#      因此修改依赖后请同时重新编译主程序与插件。
#   3. duckdb 采用“薄壳 .so + 边车子进程”方案：DuckDB 的 C++ 库无法在 Go plugin
#      （dlopen）中运行，故 duckdb.so 为纯 Go 薄壳，真正的读写在独立的
#      duckdb-helper 可执行文件中完成（CGO，首次构建会下载较大的 DuckDB 静态库）。
set -euo pipefail
cd "$(dirname "$0")"

OUT_DIR="plugins/build"
mkdir -p "$OUT_DIR"

ALL=(logfile bbolt sqlite duckdb s3)
TARGETS=("$@")
if [ ${#TARGETS[@]} -eq 0 ]; then
  TARGETS=("${ALL[@]}")
fi

# 确保第三方依赖就绪（logfile 无需依赖）
echo ">> go mod tidy（拉取插件依赖，首次较慢）"
go mod tidy

for name in "${TARGETS[@]}"; do
  echo ">> building plugin: $name"
  if [ "$name" = "duckdb" ]; then
    # duckdb：先构建 CGO 边车二进制，再构建纯 Go 薄壳插件。
    echo "   - duckdb-helper（CGO 边车，首次会下载 DuckDB 静态库）"
    CGO_ENABLED=1 go build -o "$OUT_DIR/duckdb-helper" ./cmd/duckdb-helper
    echo "     -> $OUT_DIR/duckdb-helper"
    echo "   - duckdb.so（纯 Go 薄壳）"
    go build -buildmode=plugin -o "$OUT_DIR/$name.so" "./plugins/$name"
  else
    go build -buildmode=plugin -o "$OUT_DIR/$name.so" "./plugins/$name"
  fi
  echo "   -> $OUT_DIR/$name.so"
done

echo "完成。启用示例："
echo "  ./dag-app -sink-plugin $OUT_DIR/bbolt.so -sink-config path=run-records.bolt"
