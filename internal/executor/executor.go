package executor

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"dag-app/internal/model"
)

// Result 执行结果
type Result struct {
	Stdout   string
	Stderr   string
	ExitCode int
	Err      error
	Duration time.Duration
}

// Execute 根据节点类型执行任务
// extraEnv 为运行时额外注入的环境变量（如上游节点的输出），格式 KEY=VALUE
func Execute(ctx context.Context, n *model.Node, extraEnv []string) *Result {
	start := time.Now()

	// 处理超时
	if n.TimeoutSec > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(n.TimeoutSec)*time.Second)
		defer cancel()
	}

	cmd, cleanup, err := buildCmd(ctx, n)
	if cleanup != nil {
		defer cleanup()
	}
	if err != nil {
		return &Result{Err: err, ExitCode: -1, Duration: time.Since(start)}
	}

	// 工作目录
	if n.WorkDir != "" {
		cmd.Dir = n.WorkDir
	}
	// 环境变量：继承当前环境，追加自定义变量与运行时注入的上游输出
	cmd.Env = append(os.Environ(), n.Env...)
	if len(extraEnv) > 0 {
		cmd.Env = append(cmd.Env, extraEnv...)
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	runErr := cmd.Run()
	res := &Result{
		Stdout:   stdout.String(),
		Stderr:   stderr.String(),
		Duration: time.Since(start),
	}

	if ctx.Err() == context.DeadlineExceeded {
		res.Err = fmt.Errorf("执行超时（%d 秒）", n.TimeoutSec)
		res.ExitCode = -1
		return res
	}

	if runErr != nil {
		if exitErr, ok := runErr.(*exec.ExitError); ok {
			res.ExitCode = exitErr.ExitCode()
		} else {
			res.ExitCode = -1
		}
		res.Err = runErr
		return res
	}

	res.ExitCode = 0
	return res
}

// buildCmd 根据任务类型构造命令；返回清理函数用于删除临时脚本文件
func buildCmd(ctx context.Context, n *model.Node) (*exec.Cmd, func(), error) {
	switch n.Type {
	case model.TaskShell:
		return buildShellCmd(ctx, n)
	case model.TaskPython:
		return buildScriptCmd(ctx, n, "python", "py")
	case model.TaskGolang:
		return buildGolangCmd(ctx, n)
	default:
		return nil, nil, fmt.Errorf("不支持的任务类型: %s", n.Type)
	}
}

// buildShellCmd 构造 shell 命令
func buildShellCmd(ctx context.Context, n *model.Node) (*exec.Cmd, func(), error) {
	content := n.Command
	if content == "" {
		content = n.Script
	}
	if content == "" {
		return nil, nil, fmt.Errorf("shell 任务缺少 command 或 script")
	}
	// 通过 sh -c 执行命令字符串
	args := append([]string{"-c", content}, n.Args...)
	return exec.CommandContext(ctx, "sh", args...), nil, nil
}

// buildScriptCmd 构造解释型脚本命令（python）
// 优先使用 Command 指定的脚本文件；若提供 Script 内联内容则写入临时文件执行
func buildScriptCmd(ctx context.Context, n *model.Node, interpreter, ext string) (*exec.Cmd, func(), error) {
	// 解析解释器：优先用 PythonBin（支持多 venv），否则自动查找 python3 → python
	bin := interpreter
	if interpreter == "python" {
		if n.PythonBin != "" {
			bin = n.PythonBin
		} else if p, err := exec.LookPath("python3"); err == nil {
			bin = p
		}
	}

	if n.Script != "" {
		tmpFile, cleanup, err := writeTempScript(n.Script, ext)
		if err != nil {
			return nil, nil, err
		}
		args := append([]string{tmpFile}, n.Args...)
		return exec.CommandContext(ctx, bin, args...), cleanup, nil
	}

	if n.Command == "" {
		return nil, nil, fmt.Errorf("%s 任务缺少 command(脚本路径) 或 script(内联内容)", n.Type)
	}
	args := append([]string{n.Command}, n.Args...)
	return exec.CommandContext(ctx, bin, args...), nil, nil
}

// buildGolangCmd 构造 go run 命令
func buildGolangCmd(ctx context.Context, n *model.Node) (*exec.Cmd, func(), error) {
	if n.Script != "" {
		tmpFile, cleanup, err := writeTempScript(n.Script, "go")
		if err != nil {
			return nil, nil, err
		}
		args := append([]string{"run", tmpFile}, n.Args...)
		return exec.CommandContext(ctx, "go", args...), cleanup, nil
	}
	if n.Command == "" {
		return nil, nil, fmt.Errorf("golang 任务缺少 command(go 文件/目录) 或 script(内联内容)")
	}
	args := append([]string{"run", n.Command}, n.Args...)
	return exec.CommandContext(ctx, "go", args...), nil, nil
}

// writeTempScript 将内联脚本写入临时文件
func writeTempScript(content, ext string) (string, func(), error) {
	dir, err := os.MkdirTemp("", "dag-script-*")
	if err != nil {
		return "", nil, err
	}
	file := filepath.Join(dir, "script."+ext)
	if err := os.WriteFile(file, []byte(content), 0600); err != nil {
		os.RemoveAll(dir)
		return "", nil, err
	}
	cleanup := func() { os.RemoveAll(dir) }
	return file, cleanup, nil
}
