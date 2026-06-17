---
name: 运行记录持久化插件（Sink Plugins）
description: |
  dag-app 的运行记录持久化子系统。通过 Go plugin（.so 动态库）机制，将每次 DAG
  执行的结果（节点状态、stdout/stderr、耗时等）持久化到可插拔的后端。内置 5 种后端：
  本地日志文件（logfile）、bbolt、SQLite、DuckDB、S3 兼容对象存储。
  主程序与插件共享 internal/sink 接口包，运行时按需加载，互不耦合。
modules:
  - internal/sink/sink.go     # Sink 接口与 RunRecord 数据模型（主程序与插件的契约）
  - internal/sink/loader.go   # plugin.Open 动态加载器与配置解析
  - plugins/logfile           # 本地日志文件后端（JSON Lines，零依赖）
  - plugins/bbolt             # go.etcd.io/bbolt 嵌入式 KV 后端
  - plugins/sqlite            # modernc.org/sqlite 纯 Go SQLite 后端
  - plugins/duckdb            # DuckDB 分析型数据库后端（纯 Go 薄壳，通过 duckdb-helper 边车进程读写）
  - cmd/duckdb-helper         # DuckDB 边车子进程（CGO 直连 duckdb，与薄壳插件 JSON-over-stdio 通信）
  - plugins/s3                # minio-go S3 兼容对象存储后端
  - build-plugins.sh          # 插件构建脚本
---

# 运行记录持久化插件

## 解决的问题

在引入插件前，每次 DAG 执行的结果（运行历史、各节点的 stdout/stderr）**只保存在内存**中，
进程重启即丢失。本子系统把这些记录写入外部后端，使历史可持久化、可跨重启恢复、可外部分析。

## 设计

这 5 个后端本质上都是"把一次执行记录写到某处"，因此统一抽象为 `internal/sink.Sink` 接口：

```go
type Sink interface {
    Name() string
    Open(config map[string]string) error
    Save(rec *RunRecord) error
    List(pipelineID string, limit int) ([]*RunRecord, error)
    Close() error
}
```

- **主程序**：运行结束后调用 `Save` 持久化；启动时对每个流水线调用 `List` 把最近 50 条历史
  载入内存，从而 Web 历史面板在重启后依然可见。
- **插件**：每个后端是一个独立的 `package main`，导出 `var Sink sink.Sink = &impl{}`，
  编译为 `.so`。主程序用 `plugin.Open` + `Lookup("Sink")` 动态加载。

## ⚠️ Go plugin 硬性约束（务必阅读）

1. **平台**：Go plugin 仅支持 **Linux / macOS**，不支持 Windows。
2. **版本一致性**：插件 `.so` 必须与主程序 `dag-app` 用**完全相同的 Go 版本和依赖版本**编译；
   否则运行时 `plugin.Open` 会报 `plugin was built with a different version of package ...`。
   **修改依赖后必须同时重新编译主程序与所有插件。**
3. **CGO**：`duckdb` 后端需要 CGO（仅 `duckdb-helper` 边车二进制需要，详见下条）。

## 🦆 DuckDB 的特殊设计：薄壳插件 + 边车进程

DuckDB 的 C++ 静态库**无法在 Go plugin（`buildmode=plugin` / `dlopen`）中运行**——
`.so` 能编译通过，但运行时首次进入 duckdb 的 C 接口（如 `duckdb_open_ext`）即触发
`SIGABRT`（`signal arrived during cgo execution`）。而把 duckdb 编进**普通可执行文件**
则完全正常。这是 Go plugin 加载大型 C++ 库的已知硬限制。

为同时满足「`.so` 动态加载」与「duckdb 真正可用」，duckdb 采用进程隔离：

```
dag-app(host)  --dlopen-->  plugins/build/duckdb.so（纯 Go 薄壳，不链接 duckdb）
                                   |  spawn + JSON-over-stdio
                                   v
                          plugins/build/duckdb-helper（CGO 直连 duckdb）
```

- `duckdb.so`：纯 Go，可被主程序正常加载；仅负责启动 helper、转发 `Save/List`。
- `duckdb-helper`：普通可执行文件，duckdb 直接编入，在其中读写数据库。
- 二者通过 stdin/stdout 上一来一回的 JSON 协议通信。

额外配置项 `helper=<path>` 可显式指定 helper 路径；默认依次尝试
`./plugins/build/duckdb-helper`、环境变量 `DUCKDB_HELPER`、`PATH`。

## 构建

```bash
# 构建全部插件（自动 go mod tidy 拉取依赖）
./build-plugins.sh

# 仅构建指定插件
./build-plugins.sh logfile bbolt
```

产物位于 `plugins/build/*.so`。

## 启用

主程序通过两个参数加载插件（不指定则仅内存、重启丢失）：

| 参数 | 说明 |
|------|------|
| `-sink-plugin` | 插件 `.so` 路径 |
| `-sink-config` | 插件配置，`k=v` 逗号分隔 |

各后端配置：

```bash
# 本地日志文件（JSON Lines，每个流水线一个 <dir>/<id>.log）
./dag-app -sink-plugin plugins/build/logfile.so -sink-config dir=run-logs

# bbolt 嵌入式 KV
./dag-app -sink-plugin plugins/build/bbolt.so   -sink-config path=run-records.bolt

# SQLite（纯 Go，无需系统库）
./dag-app -sink-plugin plugins/build/sqlite.so  -sink-config path=run-records.sqlite

# DuckDB（薄壳 .so + duckdb-helper 边车进程，详见上文专节）
./dag-app -sink-plugin plugins/build/duckdb.so  -sink-config path=run-records.duckdb
# 如 duckdb-helper 不在默认位置，可显式指定：
#   -sink-config path=run-records.duckdb,helper=/abs/path/duckdb-helper

# S3 / MinIO / COS 等（凭证走环境变量，见下）
export S3_ACCESS_KEY=...      # 或 AWS_ACCESS_KEY_ID
export S3_SECRET_KEY=...      # 或 AWS_SECRET_ACCESS_KEY
./dag-app -sink-plugin plugins/build/s3.so \
  -sink-config endpoint=s3.amazonaws.com,bucket=my-bucket,region=us-east-1,prefix=dag-runs,use_ssl=true
```

> **安全**：S3 凭证遵循 secrets env-only 原则，**只能**通过环境变量传入，
> 不经命令行/配置项，避免在进程列表或配置文件中泄露。

## Web 界面变化

启用持久化后，Web 顶栏新增**存储后端徽标**：

- 绿色「存储: bbolt / sqlite / ...」——已启用持久化，历史重启不丢失；
- 灰色「存储: 内存」——未配置插件，重启丢失。

历史面板（`/api/runs`、`/api/run/<id>`）会在重启后自动展示从后端恢复的记录，前端无需额外操作。
