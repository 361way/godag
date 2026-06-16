package web

import (
	"context"
	"crypto/subtle"
	"embed"
	"encoding/json"
	"io/fs"
	"net/http"

	"dag-app/internal/model"
	"dag-app/internal/store"
)

//go:embed static/*
var staticFS embed.FS

// Server HTTP 服务
type Server struct {
	store *store.Store
}

// NewServer 创建 Web 服务
func NewServer(st *store.Store) *Server {
	return &Server{store: st}
}

// Handler 返回配置好的路由处理器
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// 静态资源
	sub, _ := fs.Sub(staticFS, "static")
	mux.Handle("/", http.FileServer(http.FS(sub)))

	// 流水线维度
	mux.HandleFunc("/api/pipelines", s.handlePipelines)
	mux.HandleFunc("/api/pipeline/create", s.handlePipelineCreate)
	mux.HandleFunc("/api/pipeline/delete", s.handlePipelineDelete)
	mux.HandleFunc("/api/pipeline/schedule", s.handleSchedule)

	// 单流水线内的操作（通过 ?pipeline=<id> 指定，缺省取第一个）
	mux.HandleFunc("/api/dag", s.handleDAG)
	mux.HandleFunc("/api/dag/meta", s.handleDAGMeta)
	mux.HandleFunc("/api/run", s.handleRun)
	mux.HandleFunc("/api/runs", s.handleRuns)
	mux.HandleFunc("/api/run/", s.handleRunDetail)
	mux.HandleFunc("/api/node/enable", s.handleNodeEnable)
	mux.HandleFunc("/api/node/save", s.handleNodeSave)
	mux.HandleFunc("/api/node/delete", s.handleNodeDelete)
	mux.HandleFunc("/api/node/position", s.handleNodePosition)

	return mux
}

// BasicAuth 为传入的 handler 包装一层 HTTP Basic 鉴权。
// 当 user 与 pass 均为空时表示未配置鉴权，直接放行（便于本地调试）。
func BasicAuth(next http.Handler, user, pass string) http.Handler {
	if user == "" && pass == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		// 使用恒定时间比较，避免时序侧信道攻击
		userOK := subtle.ConstantTimeCompare([]byte(u), []byte(user)) == 1
		passOK := subtle.ConstantTimeCompare([]byte(p), []byte(pass)) == 1
		if !ok || !userOK || !passOK {
			w.Header().Set("WWW-Authenticate", `Basic realm="DAG 管理界面", charset="UTF-8"`)
			http.Error(w, "未授权", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

/* ========================= 流水线管理 ========================= */

// pipelineSummary 流水线列表的精简视图
type pipelineSummary struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Running     bool            `json:"running"`
	NodeCount   int             `json:"node_count"`
	Schedule    *model.Schedule `json:"schedule"`
}

// GET /api/pipelines 返回全部流水线概要
func (s *Server) handlePipelines(w http.ResponseWriter, r *http.Request) {
	infos := s.store.List()
	out := make([]pipelineSummary, 0, len(infos))
	for _, info := range infos {
		out = append(out, pipelineSummary{
			ID:          info.ID,
			Name:        info.Name,
			Description: info.Description,
			Running:     info.Running,
			NodeCount:   len(info.Nodes),
			Schedule:    info.Schedule,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

// POST /api/pipeline/create body: {"id":"p1","name":"...","description":"..."}
func (s *Server) handlePipelineCreate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "仅支持 POST")
		return
	}
	var req struct {
		ID          string `json:"id"`
		Name        string `json:"name"`
		Description string `json:"description"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "请求体解析失败")
		return
	}
	mgr, err := s.store.Create(req.ID, req.Name, req.Description)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"id": mgr.ID()})
}

// POST /api/pipeline/delete body: {"id":"p1"}
func (s *Server) handlePipelineDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "仅支持 POST")
		return
	}
	var req struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "请求体解析失败")
		return
	}
	if err := s.store.Delete(req.ID); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// POST /api/pipeline/schedule?pipeline=<id> body 为调度配置
func (s *Server) handleSchedule(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "仅支持 POST")
		return
	}
	m, err := s.store.Get(r.URL.Query().Get("pipeline"))
	if err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	var sch model.Schedule
	if err := json.NewDecoder(r.Body).Decode(&sch); err != nil {
		writeErr(w, http.StatusBadRequest, "请求体解析失败")
		return
	}
	if err := m.UpdateSchedule(&sch); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

/* ========================= 单流水线内操作 ========================= */

// GET /api/dag?pipeline=<id> 返回 DAG 结构与节点状态
func (s *Server) handleDAG(w http.ResponseWriter, r *http.Request) {
	m, err := s.store.Get(r.URL.Query().Get("pipeline"))
	if err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, m.GetInfo())
}

// POST /api/run?pipeline=<id> 触发一次执行
func (s *Server) handleRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "仅支持 POST")
		return
	}
	m, err := s.store.Get(r.URL.Query().Get("pipeline"))
	if err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	run, err := m.TriggerRun(context.Background())
	if err != nil {
		writeErr(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"run_id": run.ID})
}

// GET /api/runs?pipeline=<id> 返回执行历史
func (s *Server) handleRuns(w http.ResponseWriter, r *http.Request) {
	m, err := s.store.Get(r.URL.Query().Get("pipeline"))
	if err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, m.ListRuns())
}

// GET /api/run/{id}?pipeline=<id> 返回指定执行详情；id 为 latest 时返回最近一次
func (s *Server) handleRunDetail(w http.ResponseWriter, r *http.Request) {
	m, err := s.store.Get(r.URL.Query().Get("pipeline"))
	if err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	id := r.URL.Path[len("/api/run/"):]
	if id == "" {
		writeErr(w, http.StatusBadRequest, "缺少 run id")
		return
	}
	if id == "latest" {
		run := m.LatestRun()
		if run == nil {
			writeErr(w, http.StatusNotFound, "暂无执行记录")
			return
		}
		writeJSON(w, http.StatusOK, run)
		return
	}
	run, err := m.GetRun(id)
	if err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, run)
}

// POST /api/node/enable?pipeline=<id> body: {"id": "node1", "enabled": true}
func (s *Server) handleNodeEnable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "仅支持 POST")
		return
	}
	m, err := s.store.Get(r.URL.Query().Get("pipeline"))
	if err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	var req struct {
		ID      string `json:"id"`
		Enabled bool   `json:"enabled"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "请求体解析失败")
		return
	}
	if err := m.SetNodeEnabled(req.ID, req.Enabled); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// POST /api/node/save?pipeline=<id> body 为完整节点对象
func (s *Server) handleNodeSave(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "仅支持 POST")
		return
	}
	m, err := s.store.Get(r.URL.Query().Get("pipeline"))
	if err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	var n model.Node
	if err := json.NewDecoder(r.Body).Decode(&n); err != nil {
		writeErr(w, http.StatusBadRequest, "请求体解析失败")
		return
	}
	if err := m.SaveNode(&n); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// POST /api/node/delete?pipeline=<id> body: {"id": "node1"}
func (s *Server) handleNodeDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "仅支持 POST")
		return
	}
	m, err := s.store.Get(r.URL.Query().Get("pipeline"))
	if err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	var req struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "请求体解析失败")
		return
	}
	if err := m.DeleteNode(req.ID); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// POST /api/node/position?pipeline=<id> body: {"id": "node1", "x": 100, "y": 200}
func (s *Server) handleNodePosition(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "仅支持 POST")
		return
	}
	m, err := s.store.Get(r.URL.Query().Get("pipeline"))
	if err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	var req struct {
		ID string  `json:"id"`
		X  float64 `json:"x"`
		Y  float64 `json:"y"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "请求体解析失败")
		return
	}
	if err := m.SetNodePosition(req.ID, req.X, req.Y); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// POST /api/dag/meta?pipeline=<id> 更新名称与描述
func (s *Server) handleDAGMeta(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "仅支持 POST")
		return
	}
	m, err := s.store.Get(r.URL.Query().Get("pipeline"))
	if err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	var req struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "请求体解析失败")
		return
	}
	if err := m.UpdateMeta(req.Name, req.Description); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}
