package web

import (
	"context"
	"embed"
	"encoding/json"
	"io/fs"
	"net/http"

	"dag-app/internal/manager"
	"dag-app/internal/model"
)

//go:embed static/*
var staticFS embed.FS

// Server HTTP 服务
type Server struct {
	mgr *manager.Manager
}

// NewServer 创建 Web 服务
func NewServer(mgr *manager.Manager) *Server {
	return &Server{mgr: mgr}
}

// Handler 返回配置好的路由处理器
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// 静态资源
	sub, _ := fs.Sub(staticFS, "static")
	mux.Handle("/", http.FileServer(http.FS(sub)))

	// API 路由
	mux.HandleFunc("/api/dag", s.handleDAG)
	mux.HandleFunc("/api/dag/meta", s.handleDAGMeta)
	mux.HandleFunc("/api/run", s.handleRun)
	mux.HandleFunc("/api/runs", s.handleRuns)
	mux.HandleFunc("/api/run/", s.handleRunDetail)
	mux.HandleFunc("/api/node/enable", s.handleNodeEnable)
	mux.HandleFunc("/api/node/save", s.handleNodeSave)
	mux.HandleFunc("/api/node/delete", s.handleNodeDelete)

	return mux
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// GET /api/dag 返回 DAG 结构与节点状态
func (s *Server) handleDAG(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.mgr.GetInfo())
}

// POST /api/run 触发一次执行
func (s *Server) handleRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "仅支持 POST")
		return
	}
	run, err := s.mgr.TriggerRun(context.Background())
	if err != nil {
		writeErr(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"run_id": run.ID})
}

// GET /api/runs 返回执行历史
func (s *Server) handleRuns(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.mgr.ListRuns())
}

// GET /api/run/{id} 返回指定执行详情；id 为 latest 时返回最近一次
func (s *Server) handleRunDetail(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Path[len("/api/run/"):]
	if id == "" {
		writeErr(w, http.StatusBadRequest, "缺少 run id")
		return
	}
	if id == "latest" {
		run := s.mgr.LatestRun()
		if run == nil {
			writeErr(w, http.StatusNotFound, "暂无执行记录")
			return
		}
		writeJSON(w, http.StatusOK, run)
		return
	}
	run, err := s.mgr.GetRun(id)
	if err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, run)
}

// POST /api/node/enable 通过参数控制节点开关
// body: {"id": "node1", "enabled": true}
func (s *Server) handleNodeEnable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "仅支持 POST")
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
	if err := s.mgr.SetNodeEnabled(req.ID, req.Enabled); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// POST /api/node/save 新增或更新节点（按 id 区分）
// body 为完整的节点对象
func (s *Server) handleNodeSave(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "仅支持 POST")
		return
	}
	var n model.Node
	if err := json.NewDecoder(r.Body).Decode(&n); err != nil {
		writeErr(w, http.StatusBadRequest, "请求体解析失败")
		return
	}
	if err := s.mgr.SaveNode(&n); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// POST /api/node/delete 删除节点
// body: {"id": "node1"}
func (s *Server) handleNodeDelete(w http.ResponseWriter, r *http.Request) {
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
	if err := s.mgr.DeleteNode(req.ID); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// POST /api/dag/meta 更新 DAG 名称与描述
// body: {"name": "...", "description": "..."}
func (s *Server) handleDAGMeta(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "仅支持 POST")
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
	if err := s.mgr.UpdateMeta(req.Name, req.Description); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}
