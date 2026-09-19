package codex

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"

	provider "github.com/yogasw/wick/internal/agents/provider"
	"github.com/yogasw/wick/internal/agents/provider/procgroup"
	"github.com/yogasw/wick/pkg/safeexec"
)

type compactProcess struct {
	cmd  *exec.Cmd
	out  *io.PipeReader
	done chan error
	once sync.Once
	bin  string
	args []string
}

func (p *compactProcess) Stdout() io.Reader     { return p.out }
func (p *compactProcess) Stdin() io.WriteCloser { return noopWriteCloser{} }
func (p *compactProcess) Wait() error           { return <-p.done }
func (p *compactProcess) Pid() int {
	if p.cmd.Process != nil {
		return p.cmd.Process.Pid
	}
	return 0
}
func (p *compactProcess) Binary() string { return p.bin }
func (p *compactProcess) Argv() []string { return append([]string(nil), p.args...) }
func (p *compactProcess) Env() []string  { return nil }
func (p *compactProcess) Kill() error {
	var err error
	p.once.Do(func() {
		if p.cmd.Process != nil {
			err = p.cmd.Process.Kill()
		}
	})
	return err
}

type rpcEnvelope struct {
	ID     int    `json:"id,omitempty"`
	Method string `json:"method,omitempty"`
	Error  *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
	Params struct {
		Item struct {
			Type string `json:"type"`
			ID   string `json:"id"`
		} `json:"item"`
		TokenUsage struct {
			Last struct {
				TotalTokens int `json:"totalTokens"`
				InputTokens int `json:"inputTokens"`
			} `json:"last"`
		} `json:"tokenUsage"`
		Turn struct {
			Status string `json:"status"`
		} `json:"turn"`
	} `json:"params,omitempty"`
}

func (s Spawner) spawnCompact(ctx context.Context, opt provider.SpawnOptions, bin string, routerEnv []string) (provider.Process, error) {
	resolved, err := safeexec.ResolveBin(bin)
	if err != nil {
		return nil, err
	}
	realBin, realArgs := resolved, []string{"app-server", "--stdio"}
	execBin, execArgs := termuxProotWrap(realBin, realArgs)
	cmd := safeexec.CommandContext(ctx, execBin, execArgs...)
	cmd.Dir = opt.Workspace
	cmd.Env = append(os.Environ(), opt.ExtraEnv...)
	cmd.Env = append(cmd.Env, routerEnv...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	cmd.Stderr = os.Stderr
	procgroup.Apply(cmd)
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start codex app-server: %w", err)
	}
	pr, pw := io.Pipe()
	p := &compactProcess{cmd: cmd, out: pr, done: make(chan error, 1), bin: realBin, args: realArgs}
	go func() {
		err := runCompactRPC(ctx, stdin, stdout, pw, opt.ResumeID)
		_ = stdin.Close()
		_ = cmd.Process.Kill()
		waitErr := cmd.Wait()
		if err == nil && waitErr != nil && ctx.Err() == nil {
			err = waitErr
		}
		_ = pw.CloseWithError(err)
		p.done <- err
	}()
	return p, nil
}

func runCompactRPC(ctx context.Context, in io.Writer, out io.Reader, translated io.Writer, threadID string) error {
	enc := json.NewEncoder(in)
	send := func(v any) error { return enc.Encode(v) }
	if err := send(map[string]any{"method": "initialize", "id": 1, "params": map[string]any{"clientInfo": map[string]string{"name": "wick", "title": "Wick", "version": "1"}, "capabilities": map[string]bool{"experimentalApi": true}}}); err != nil {
		return err
	}
	if err := send(map[string]any{"method": "initialized", "params": map[string]any{}}); err != nil {
		return err
	}
	if err := send(map[string]any{"method": "thread/resume", "id": 2, "params": map[string]string{"threadId": threadID}}); err != nil {
		return err
	}
	scanner := bufio.NewScanner(out)
	pre, post := 0, 0
	resumed, compactSent, completed := false, false, false
	start := time.Now()
	for scanner.Scan() {
		var m rpcEnvelope
		if json.Unmarshal(scanner.Bytes(), &m) != nil {
			continue
		}
		if m.Error != nil {
			return fmt.Errorf("codex app-server: %s", m.Error.Message)
		}
		if m.Method == "thread/tokenUsage/updated" {
			n := m.Params.TokenUsage.Last.TotalTokens
			if !compactSent {
				pre = n
			} else if n > 0 {
				post = n
			}
		}
		if m.ID == 2 {
			resumed = true
		}
		if resumed && pre > 0 && !compactSent {
			if err := send(map[string]any{"method": "thread/compact/start", "id": 3, "params": map[string]string{"threadId": threadID}}); err != nil {
				return err
			}
			compactSent = true
		}
		if m.Method == "item/completed" && m.Params.Item.Type == "contextCompaction" {
			completed = true
		}
		if completed && m.Method == "turn/completed" {
			payload := map[string]any{"type": "wick.compaction", "compaction": map[string]any{"trigger": "manual", "pre_tokens": pre, "post_tokens": post, "dropped_tokens": max(0, pre-post), "duration_ms": time.Since(start).Milliseconds()}}
			b, _ := json.Marshal(payload)
			_, _ = translated.Write(append(b, '\n'))
			_, _ = io.WriteString(translated, "{\"type\":\"turn.completed\"}\n")
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return fmt.Errorf("codex app-server exited before compaction completed")
}
