# DAG 任务调度应用

一个使用 Go 语言开发的轻量级 DAG（有向无环图）任务调度器，支持运行 **shell / golang / python** 三类任务，提供简单的 **Web 管理界面**，支持通过 **YAML / JSON** 定义任务节点，并可通过参数或界面开关动态启用/禁用节点。

## 功能特性

- **多类型任务执行**：支持 `shell`、`golang`（`go run`）、`python` 三类任务节点
- **依赖编排**：通过 `deps` 声明节点依赖，自动进行环检测与拓扑排序，并发执行无依赖关系的节点
- **节点启停控制**：
  - 配置文件中通过 `enabled: false` 默认禁用
  - 启动参数 `-disable` 指定禁用节点
  - Web 界面开关实时切换
- **级联跳过**：被禁用或上游失败的节点，其下游节点自动跳过
- **N8N 风格可视化画布**：
  - 节点以卡片形式呈现在画布上，可自由拖拽移动，布局自动持久化
  - 从节点右侧端点拖拽到另一节点左侧端点即可建立依赖连线，点击连线可删除依赖
  - 画布支持平移（拖拽空白处）、滚轮缩放、一键「适应视图」与「自动布局」
  - 执行时节点边框 / 状态徽标实时着色（等待 / 执行中 / 成功 / 失败 / 跳过）
- **侧边检视面板**：选中节点即可在右侧编辑全部字段、查看运行日志与执行历史
- **双格式配置**：支持 YAML 与 JSON 定义任务
- **两种运行模式**：Web 服务模式 / 命令行一次性执行模式

## 项目结构

```
dag-app/
├── main.go                      # 程序入口（Web 模式 / CLI 模式）
├── go.mod
├── examples/
│   ├── dag.yaml                 # YAML 示例配置
│   └── dag.json                 # JSON 示例配置
└── internal/
    ├── model/node.go            # 节点与 DAG 配置数据模型
    ├── config/config.go         # YAML/JSON 配置加载
    ├── dag/dag.go               # DAG 构建、环检测、拓扑排序
    ├── executor/executor.go     # shell/golang/python 执行器
    ├── engine/engine.go         # 并发执行引擎与状态跟踪
    ├── manager/manager.go       # DAG 状态、节点启停、运行历史管理
    └── web/
        ├── server.go            # HTTP REST API 与静态资源
        └── static/index.html    # Web 管理界面
```

## 快速开始

### 编译

```bash
cd dag-app
go mod tidy
go build -o dag-app .
```

### 启动 Web 服务

```bash
./dag-app -config examples/dag.yaml -addr 127.0.0.1:8080
```

然后浏览器访问 http://127.0.0.1:8080

### 启用访问鉴权（可选）

管理界面支持 HTTP Basic 鉴权，账号密码通过环境变量配置，两者均未设置时不启用鉴权（便于本地调试）：

```bash
export DAG_AUTH_USER=admin
export DAG_AUTH_PASS=your-strong-password
./dag-app -config examples/dag.yaml -addr 0.0.0.0:8080
```

启用后访问页面或任意 `/api/*` 接口都需要提供凭证，否则返回 `401`。建议在对外暴露端口时务必配置。

### 命令行一次性执行

```bash
./dag-app -run -config examples/dag.yaml
```

## 启动参数

| 参数 | 说明 | 默认值 |
| --- | --- | --- |
| `-config` | DAG 配置文件路径（.yaml/.yml/.json） | `examples/dag.yaml` |
| `-addr` | Web 界面监听地址 | `127.0.0.1:8080` |
| `-parallel` | 最大并发执行节点数，<=0 不限制 | `4` |
| `-disable` | 启动时禁用的节点 ID，逗号分隔 | 空 |
| `-run` | 命令行模式：执行一次后退出 | `false` |

## 配置说明

### 节点字段

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `id` | string | 节点唯一标识（必填） |
| `name` | string | 显示名称 |
| `type` | string | 任务类型：`shell` / `golang` / `python` |
| `deps` | []string | 依赖的上游节点 ID 列表 |
| `enabled` | bool | 是否启用，默认 `true` |
| `command` | string | 执行内容：shell 命令 / go 文件路径 / py 文件路径 |
| `script` | string | 内联脚本内容（写入临时文件执行，与 command 二选一） |
| `args` | []string | 额外执行参数 |
| `workdir` | string | 工作目录 |
| `env` | []string | 额外环境变量（`KEY=VALUE`） |
| `timeout_sec` | int | 超时时间（秒），0 表示不限制 |
| `x` / `y` | float | 节点在可视化画布中的坐标（拖拽后自动持久化，无需手填） |

### YAML 示例

```yaml
name: 数据处理流水线
description: 示例
nodes:
  - id: start
    name: 初始化
    type: shell
    command: "echo 开始"
  - id: process
    name: Python 处理
    type: python
    deps: [start]
    script: |
      print("处理中")
```

完整示例见 `examples/dag.yaml` 和 `examples/dag.json`。

## REST API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/dag` | 获取 DAG 结构与节点状态 |
| POST | `/api/run` | 触发一次执行 |
| GET | `/api/runs` | 获取执行历史列表 |
| GET | `/api/run/{id}` | 获取指定执行详情（`latest` 表示最近一次） |
| POST | `/api/node/enable` | 启用/禁用节点，body：`{"id":"node1","enabled":true}` |
| POST | `/api/node/save` | 新增/更新节点（按 id 区分），body 为完整节点对象 |
| POST | `/api/node/delete` | 删除节点，body：`{"id":"node1"}` |
| POST | `/api/node/position` | 更新节点画布坐标，body：`{"id":"node1","x":120,"y":80}` |
| POST | `/api/dag/meta` | 更新工作流名称与描述，body：`{"name":"...","description":"..."}` |

## 安全提示

该应用本质是一个任务执行器，会运行配置中定义的任意命令/脚本，存在命令执行能力。请务必：

- 仅在**受信任的内网环境**中部署，默认监听 `127.0.0.1`
- 不要将管理端口直接暴露到公网
- 配置文件应妥善保管，避免被未授权修改
