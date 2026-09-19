package codex

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestRunCompactRPC(t *testing.T) {
	responses := strings.Join([]string{
		`{"id":1,"result":{}}`,
		`{"method":"thread/tokenUsage/updated","params":{"tokenUsage":{"last":{"totalTokens":16035}}}}`,
		`{"id":2,"result":{"thread":{"id":"thread-1"}}}`,
		`{"id":3,"result":{}}`,
		`{"method":"thread/tokenUsage/updated","params":{"tokenUsage":{"last":{"totalTokens":4944}}}}`,
		`{"method":"item/completed","params":{"item":{"type":"contextCompaction","id":"compact-1"}}}`,
		`{"method":"turn/completed","params":{"turn":{"status":"completed"}}}`,
	}, "\n") + "\n"

	var requests bytes.Buffer
	var translated bytes.Buffer
	if err := runCompactRPC(context.Background(), &requests, strings.NewReader(responses), &translated, "thread-1"); err != nil {
		t.Fatal(err)
	}
	if got := requests.String(); !strings.Contains(got, `"method":"thread/resume"`) || !strings.Contains(got, `"method":"thread/compact/start"`) {
		t.Fatalf("missing RPC requests: %s", got)
	}
	got := translated.String()
	if !strings.Contains(got, `"pre_tokens":16035`) || !strings.Contains(got, `"post_tokens":4944`) || !strings.Contains(got, `"type":"turn.completed"`) {
		t.Fatalf("bad translated stream: %s", got)
	}
}
