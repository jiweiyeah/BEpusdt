package task

import (
	"context"
	"testing"
	"time"

	"github.com/smallnest/chanx"
)

func TestEvmRetryDelayIsBounded(t *testing.T) {
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 0, want: 2 * time.Second},
		{attempt: 1, want: 2 * time.Second},
		{attempt: 2, want: 5 * time.Second},
		{attempt: 3, want: 15 * time.Second},
		{attempt: 4, want: 30 * time.Second},
		{attempt: 5, want: time.Minute},
		{attempt: 100, want: time.Minute},
	}

	for _, tt := range tests {
		if got := evmRetryDelay(tt.attempt); got != tt.want {
			t.Fatalf("attempt %d delay = %s, want %s", tt.attempt, got, tt.want)
		}
	}
}

// newTestEvm 构造一个只用于重试逻辑测试的 evm，不注册任何后台任务。
func newTestEvm() *evm {
	return &evm{
		Network:        "bsc",
		blockScanQueue: chanx.NewUnboundedChan[evmBlock](context.Background(), 30),
	}
}

func TestEvmScheduleBlockRetryCoalescesSameBatch(t *testing.T) {
	e := newTestEvm()
	b := evmBlock{From: 100, To: 109}

	// 同一批次连续调度多次，只应保留一个待重试任务。
	e.scheduleBlockRetry(b, time.Minute)
	e.scheduleBlockRetry(b, time.Minute)
	e.scheduleBlockRetry(b, time.Minute)

	e.retryMu.Lock()
	scheduled := len(e.retryScheduled)
	e.retryMu.Unlock()

	if scheduled != 1 {
		t.Fatalf("scheduled retries = %d, want 1", scheduled)
	}

	e.retryMu.Lock()
	attempts := e.retryAttempts[b.From]
	e.retryMu.Unlock()

	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

func TestEvmScheduleBlockRetryEnqueuesAfterDelay(t *testing.T) {
	e := newTestEvm()
	b := evmBlock{From: 200, To: 209}

	e.scheduleBlockRetry(b, 10*time.Millisecond)

	select {
	case got := <-e.blockScanQueue.Out:
		if got != b {
			t.Fatalf("enqueued block = %+v, want %+v", got, b)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for retry to be enqueued")
	}
}

func TestEvmResetBlockRetryClearsState(t *testing.T) {
	e := newTestEvm()
	b := evmBlock{From: 300, To: 309}

	e.scheduleBlockRetry(b, time.Hour)
	e.resetBlockRetry(b)

	e.retryMu.Lock()
	attempts := len(e.retryAttempts)
	scheduled := len(e.retryScheduled)
	e.retryMu.Unlock()

	if attempts != 0 || scheduled != 0 {
		t.Fatalf("after reset: attempts = %d, scheduled = %d, want 0, 0", attempts, scheduled)
	}

	// reset 之后队列里不应再有该批次。
	select {
	case got := <-e.blockScanQueue.Out:
		t.Fatalf("unexpected enqueued block after reset: %+v", got)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestEvmScheduleBlockRetryTracksBatchesIndependently(t *testing.T) {
	e := newTestEvm()
	first := evmBlock{From: 400, To: 409}
	second := evmBlock{From: 410, To: 419}

	e.scheduleBlockRetry(first, time.Hour)
	e.scheduleBlockRetry(second, time.Hour)

	e.retryMu.Lock()
	scheduled := len(e.retryScheduled)
	e.retryMu.Unlock()

	if scheduled != 2 {
		t.Fatalf("scheduled retries = %d, want 2", scheduled)
	}
}
