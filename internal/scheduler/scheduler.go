// Package scheduler 提供基于 store 的后台计划任务调度，
// 支持一次性任务（once）与周期性任务（cron）。
package scheduler

import (
	"context"
	"log"
	"sync"
	"time"

	"dag-app/internal/cron"
	"dag-app/internal/manager"
	"dag-app/internal/model"
	"dag-app/internal/store"
)

// atLayouts 一次性任务支持的时间格式
var atLayouts = []string{"2006-01-02 15:04", "2006-01-02T15:04", "2006-01-02 15:04:05", time.RFC3339}

// Scheduler 后台调度器
type Scheduler struct {
	store    *store.Store
	interval time.Duration

	mu       sync.Mutex
	lastCron map[string]string // pipelineID -> 上次 cron 触发的分钟标识，去重用

	stop chan struct{}
	done chan struct{}
}

// New 创建调度器
func New(s *store.Store) *Scheduler {
	return &Scheduler{
		store:    s,
		interval: 15 * time.Second,
		lastCron: make(map[string]string),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// Start 启动后台调度循环
func (s *Scheduler) Start() {
	go func() {
		defer close(s.done)
		ticker := time.NewTicker(s.interval)
		defer ticker.Stop()
		s.tick(time.Now()) // 启动即检查一次
		for {
			select {
			case <-s.stop:
				return
			case now := <-ticker.C:
				s.tick(now)
			}
		}
	}()
	log.Printf("计划任务调度器已启动（检查间隔 %s）", s.interval)
}

// Stop 停止调度器并等待退出
func (s *Scheduler) Stop() {
	close(s.stop)
	<-s.done
}

// tick 单次调度检查
func (s *Scheduler) tick(now time.Time) {
	for _, mgr := range s.store.All() {
		sch := mgr.GetSchedule()
		if sch == nil || !sch.Enabled {
			continue
		}
		switch sch.Type {
		case model.ScheduleCron:
			s.handleCron(mgr, sch, now)
		case model.ScheduleOnce:
			s.handleOnce(mgr, sch, now)
		}
	}
}

// handleCron 处理周期性任务
func (s *Scheduler) handleCron(mgr *manager.Manager, sch *model.Schedule, now time.Time) {
	schedule, err := cron.Parse(sch.Cron)
	if err != nil {
		return // 表达式非法，忽略
	}
	if !schedule.Match(now) {
		return
	}
	// 同一分钟内只触发一次
	key := now.Format("2006-01-02 15:04")
	id := mgr.ID()
	s.mu.Lock()
	if s.lastCron[id] == key {
		s.mu.Unlock()
		return
	}
	s.lastCron[id] = key
	s.mu.Unlock()

	s.trigger(mgr, "cron "+sch.Cron)
}

// handleOnce 处理一次性任务
func (s *Scheduler) handleOnce(mgr *manager.Manager, sch *model.Schedule, now time.Time) {
	if sch.Fired {
		return
	}
	at, ok := parseAt(sch.At)
	if !ok {
		return
	}
	if now.Before(at) {
		return
	}
	// 先标记已触发再执行，避免重复
	if err := mgr.MarkScheduleFired(); err != nil {
		log.Printf("调度器: 标记一次性任务失败 [%s]: %v", mgr.ID(), err)
	}
	s.trigger(mgr, "once "+sch.At)
}

// trigger 触发一次执行（已在运行则跳过）
func (s *Scheduler) trigger(mgr *manager.Manager, reason string) {
	run, err := mgr.TriggerRun(context.Background())
	if err != nil {
		log.Printf("调度器: 跳过触发 [%s]（%s）: %v", mgr.ID(), reason, err)
		return
	}
	log.Printf("调度器: 已触发流水线 [%s] 执行 run=%s（%s）", mgr.ID(), run.ID, reason)
}

// parseAt 按多种格式解析一次性任务时间（按本地时区）
func parseAt(s string) (time.Time, bool) {
	for _, layout := range atLayouts {
		if t, err := time.ParseInLocation(layout, s, time.Local); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}
