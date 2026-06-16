package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"dag-app/internal/config"
	"dag-app/internal/dag"
	"dag-app/internal/engine"
	"dag-app/internal/scheduler"
	"dag-app/internal/store"
	"dag-app/internal/web"
)

func main() {
	var (
		configPath  = flag.String("config", "examples/dag.yaml", "单文件 DAG 配置（CLI 模式或首次导入用）")
		dataDir     = flag.String("data", "pipelines", "多流水线配置目录（每个流水线一个文件）")
		addr        = flag.String("addr", "127.0.0.1:8080", "Web 管理界面监听地址")
		maxParallel = flag.Int("parallel", 4, "最大并发执行节点数，<=0 表示不限制")
		disable     = flag.String("disable", "", "启动时禁用的节点 ID 列表，逗号分隔（仅 CLI 模式）")
		runOnce     = flag.Bool("run", false, "命令行模式：直接执行一次 -config 指定的 DAG 后退出")
	)
	flag.Parse()

	// 命令行一次性执行模式（仅针对 -config 单文件）
	if *runOnce {
		cfg, err := config.Load(*configPath)
		if err != nil {
			log.Fatalf("加载配置失败: %v", err)
		}
		d, err := dag.Build(cfg)
		if err != nil {
			log.Fatalf("构建 DAG 失败: %v", err)
		}
		if *disable != "" {
			for _, id := range strings.Split(*disable, ",") {
				id = strings.TrimSpace(id)
				if node, ok := d.Nodes[id]; ok {
					node.SetEnabled(false)
				}
			}
		}
		executeCLI(d, *maxParallel)
		return
	}

	// 多流水线存储
	st, err := store.New(*dataDir, *maxParallel)
	if err != nil {
		log.Fatalf("初始化流水线目录失败: %v", err)
	}
	// 首次启动且目录为空时，尝试从 -config 导入一个初始流水线
	if st.Count() == 0 {
		if cfg, lerr := config.Load(*configPath); lerr == nil {
			if ierr := st.ImportConfig(cfg); ierr != nil {
				log.Printf("警告: 导入初始流水线失败: %v", ierr)
			} else {
				log.Printf("已从 %s 导入初始流水线到 %s/", *configPath, *dataDir)
			}
		}
	}
	log.Printf("已加载 %d 条流水线（目录: %s）", st.Count(), *dataDir)

	// 启动计划任务调度器
	sch := scheduler.New(st)
	sch.Start()
	defer sch.Stop()

	// 启动 Web 服务
	srv := web.NewServer(st)

	// 最简单的鉴权：HTTP Basic Auth，账号密码经环境变量配置
	//   DAG_AUTH_USER / DAG_AUTH_PASS
	// 两者均未设置时不启用鉴权（便于本地调试）。
	authUser := os.Getenv("DAG_AUTH_USER")
	authPass := os.Getenv("DAG_AUTH_PASS")
	handler := web.BasicAuth(srv.Handler(), authUser, authPass)
	if authUser != "" || authPass != "" {
		log.Printf("已启用 Basic 鉴权（用户: %s）", authUser)
	} else {
		log.Printf("警告: 未配置 DAG_AUTH_USER/DAG_AUTH_PASS，管理界面未启用鉴权")
	}

	httpServer := &http.Server{
		Addr:    *addr,
		Handler: handler,
	}

	go func() {
		log.Printf("DAG 管理界面已启动: http://%s", *addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP 服务异常: %v", err)
		}
	}()

	// 优雅退出
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Println("正在关闭服务...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(ctx)
}

// executeCLI 命令行模式：直接执行一次 DAG 并打印结果
func executeCLI(d *dag.DAG, maxParallel int) {
	run := &engine.Run{
		ID:        time.Now().Format("20060102-150405"),
		StartedAt: time.Now(),
		Nodes:     make(map[string]*engine.NodeResult),
	}
	eng := engine.New(maxParallel)
	eng.Execute(context.Background(), d, run)

	fmt.Println("==== DAG 执行结果 ====")
	for id, nr := range run.Nodes {
		fmt.Printf("[%s] %s: %s (exit=%d)\n", nr.Status, id, nr.Name, nr.ExitCode)
		if nr.Stdout != "" {
			fmt.Printf("  stdout: %s\n", strings.TrimSpace(nr.Stdout))
		}
		if nr.Error != "" {
			fmt.Printf("  error: %s\n", nr.Error)
		}
	}
}
