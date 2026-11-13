package queue

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gocolly/colly/v2"
)

// countQueueGoroutines returns how many live goroutines have a stack frame in
// the queue package. Unlike runtime.NumGoroutine it ignores goroutines owned by
// the HTTP transport (keep-alive connection pools), which are unrelated to the
// queue lifecycle and may linger after the test server is done.
func countQueueGoroutines() int {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	var count int
	for _, block := range bytes.Split(buf[:n], []byte("\n\n")) {
		if bytes.Contains(block, []byte("gocolly/v2/queue.(")) ||
			bytes.Contains(block, []byte("gocolly/v2/queue.")) {
			count++
		}
	}
	return count
}

// newTestQueue is a thin test helper around New that fails fast on setup.
func newTestQueue(threads int, s Storage) *Queue {
	q, err := New(threads, s)
	if err != nil {
		panic(err)
	}
	return q
}

// testServer returns an httptest server whose handlers sleep for the delay
// encoded in the request URL (/delay?t=<duration>) and respond with 200 OK.
func testServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/delay" {
			http.NotFound(w, req)
			return
		}
		d, err := time.ParseDuration(req.URL.Query().Get("t"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		time.Sleep(d)
		w.WriteHeader(http.StatusOK)
	}))
}

// newTestCollector counts every successful response through the returned
// atomic counter. URL revisits are allowed.
func newTestCollector() (*colly.Collector, *uint32) {
	var processed uint32
	c := colly.NewCollector(colly.AllowURLRevisit())
	c.OnResponse(func(resp *colly.Response) {
		if resp.StatusCode == http.StatusOK {
			atomic.AddUint32(&processed, 1)
		}
	})
	c.OnError(func(resp *colly.Response, err error) {
		panic(fmt.Sprintf("unexpected request error: %v", err))
	})
	return c, &processed
}

func testURL(server *httptest.Server, n int) string {
	return fmt.Sprintf("%s/delay?t=%dus", server.URL, n%50)
}

// runAsync runs q.Run in a goroutine and returns a channel that delivers its
// result when the round finishes.
func runAsync(t *testing.T, q *Queue, c *colly.Collector) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		done <- q.Run(c)
	}()
	return done
}

// waitDone fails the test if done does not resolve within the timeout.
func waitDone(t *testing.T, done <-chan error, timeout time.Duration, msg string) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		t.Fatalf("%s: Run did not return within %s", msg, timeout)
		return nil
	}
}

// waitFor fails the test if cond is not satisfied within the timeout.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("%s: condition not met within %s", msg, timeout)
}

// TestRunRestartAfterDrain reproduces the original crash: after a Run round
// drains the queue, starting another round must work instead of panicking.
func TestRunRestartAfterDrain(t *testing.T) {
	server := testServer()
	defer server.Close()

	q := newTestQueue(4, &InMemoryQueueStorage{MaxSize: 1000})
	for round := 0; round < 3; round++ {
		c, processed := newTestCollector()
		const n = 20
		for i := 0; i < n; i++ {
			if err := q.AddURL(testURL(server, i)); err != nil {
				t.Fatalf("AddURL: %v", err)
			}
		}
		if err := q.Run(c); err != nil {
			t.Fatalf("round %d: Run: %v", round, err)
		}
		if got := atomic.LoadUint32(processed); got != n {
			t.Fatalf("round %d: processed %d requests, want %d", round, got, n)
		}
		if !q.IsEmpty() {
			t.Fatalf("round %d: queue should be empty after drain", round)
		}
	}
}

// TestAddToIdleQueueThenRun reproduces the original permanent hang: adding to
// an idle queue (before the first Run as well as after a round finished) must
// return promptly and the requests must be consumed by the next Run.
func TestAddToIdleQueueThenRun(t *testing.T) {
	server := testServer()
	defer server.Close()

	q := newTestQueue(4, &InMemoryQueueStorage{MaxSize: 1000})

	addIdle := func(label string, n int) {
		done := make(chan struct{})
		go func() {
			defer close(done)
			for i := 0; i < n; i++ {
				if err := q.AddURL(testURL(server, i)); err != nil {
					t.Errorf("%s: AddURL: %v", label, err)
					return
				}
			}
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: adding to idle queue blocked forever", label)
		}
	}

	const first, second = 15, 25
	addIdle("before-first-run", first)

	c, processed := newTestCollector()
	if err := q.Run(c); err != nil {
		t.Fatalf("first Run: %v", err)
	}
	if got := atomic.LoadUint32(processed); got != first {
		t.Fatalf("first round: processed %d, want %d", got, first)
	}

	// The queue is now idle again; re-arming must not block.
	addIdle("after-drain", second)
	c2, processed2 := newTestCollector()
	if err := q.Run(c2); err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if got := atomic.LoadUint32(processed2); got != second {
		t.Fatalf("second round: processed %d, want %d", got, second)
	}
}

// TestAddWhileRunning makes sure requests added to a running queue are picked
// up without another Run call. The adds happen while a slow request is still in
// flight, so the round is guaranteed to be alive (active > 0) and the new
// requests are consumed within the same round.
func TestAddWhileRunning(t *testing.T) {
	server := testServer()
	defer server.Close()

	q := newTestQueue(2, &InMemoryQueueStorage{MaxSize: 1000})
	c, processed := newTestCollector()
	// Two slow requests keep both workers busy and the round alive while the
	// extra requests are enqueued from another goroutine.
	if err := q.AddURL(server.URL + "/delay?t=400ms"); err != nil {
		t.Fatalf("AddURL: %v", err)
	}
	if err := q.AddURL(server.URL + "/delay?t=400ms"); err != nil {
		t.Fatalf("AddURL: %v", err)
	}
	done := runAsync(t, q, c)

	// Give the workers time to pick up the slow requests, but stay well within
	// their 400ms execution window so the round is still running.
	time.Sleep(100 * time.Millisecond)

	const n = 10
	addDone := make(chan struct{})
	go func() {
		defer close(addDone)
		for i := 0; i < n; i++ {
			if err := q.AddURL(testURL(server, i)); err != nil {
				t.Errorf("AddURL while running: %v", err)
				return
			}
		}
	}()
	select {
	case <-addDone:
	case <-time.After(5 * time.Second):
		t.Fatal("AddRequest blocked while queue was running")
	}

	if err := waitDone(t, done, 10*time.Second, "drain-after-live-add"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := atomic.LoadUint32(processed); got != 2+n {
		t.Fatalf("processed %d requests, want %d", got, 2+n)
	}
}

// TestAddAfterDrainConsumedByNextRun pins the drain semantics: once a round has
// fully drained and Run has returned, requests added to the now-idle queue must
// not block the producer and must be processed by the next Run call.
func TestAddAfterDrainConsumedByNextRun(t *testing.T) {
	server := testServer()
	defer server.Close()

	q := newTestQueue(2, &InMemoryQueueStorage{MaxSize: 1000})
	c, processed := newTestCollector()
	if err := q.AddURL(testURL(server, 1)); err != nil {
		t.Fatalf("AddURL: %v", err)
	}
	if err := q.Run(c); err != nil {
		t.Fatalf("first Run: %v", err)
	}
	if got := atomic.LoadUint32(processed); got != 1 {
		t.Fatalf("first round processed %d, want 1", got)
	}

	// The round has fully drained; adding now must return immediately.
	const n = 8
	blocked := make(chan struct{})
	go func() {
		defer close(blocked)
		for i := 0; i < n; i++ {
			if err := q.AddURL(testURL(server, i)); err != nil {
				t.Errorf("AddURL after drain: %v", err)
				return
			}
		}
	}()
	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("AddRequest on the drained idle queue blocked forever")
	}
	if size, _ := q.Size(); size != n {
		t.Fatalf("idle queue size = %d, want %d", size, n)
	}

	c2, processed2 := newTestCollector()
	if err := q.Run(c2); err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if got := atomic.LoadUint32(processed2); got != n {
		t.Fatalf("second round processed %d, want %d", got, n)
	}
	if !q.IsEmpty() {
		t.Fatal("queue should be empty after second drain")
	}
}

// TestStopIsIdempotentAndRestartable covers the Stop lifecycle: Stop returns
// quickly, in-flight requests finish, queued requests survive, repeated Stop
// calls are harmless, and a later Run drains the remaining work.
func TestStopIsIdempotentAndRestartable(t *testing.T) {
	// A dedicated server that signals when each slow request starts, so the
	// test can wait until the in-flight requests are genuinely executing
	// before calling Stop (rather than merely popped from storage).
	var started int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/slow" {
			atomic.AddInt32(&started, 1)
			time.Sleep(150 * time.Millisecond)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	q := newTestQueue(2, &InMemoryQueueStorage{MaxSize: 1000})
	c, processed := newTestCollector()
	// Two slow requests keep both worker threads busy; the rest stays queued.
	const slow, queued = 2, 20
	for i := 0; i < slow; i++ {
		if err := q.AddURL(server.URL + "/slow"); err != nil {
			t.Fatalf("AddURL slow: %v", err)
		}
	}
	for i := 0; i < queued; i++ {
		if err := q.AddURL(testURL(server, i)); err != nil {
			t.Fatalf("AddURL queued: %v", err)
		}
	}
	done := runAsync(t, q, c)

	// Wait until both slow requests are actually executing in a worker.
	waitFor(t, 2*time.Second, func() bool {
		return atomic.LoadInt32(&started) == slow
	}, "slow requests never started executing")

	q.Stop()
	// Repeated Stop on a stopping/idle queue must be a safe no-op.
	q.Stop()

	if err := waitDone(t, done, 10*time.Second, "after-Stop"); err != nil {
		t.Fatalf("Run after Stop: %v", err)
	}
	if got := atomic.LoadUint32(processed); got < slow {
		t.Fatalf("in-flight requests were not allowed to finish: %d < %d", got, slow)
	}
	// Everything not processed remains in storage; nothing was lost.
	wantRemaining := slow + queued - int(atomic.LoadUint32(processed))
	size, err := q.Size()
	if err != nil {
		t.Fatalf("Size: %v", err)
	}
	if size != wantRemaining {
		t.Fatalf("remaining queue size = %d, want %d", size, wantRemaining)
	}

	// Add more work while idle, then restart: all remaining plus new work
	// must be processed.
	const extra = 5
	for i := 0; i < extra; i++ {
		if err := q.AddURL(testURL(server, 100+i)); err != nil {
			t.Fatalf("AddURL after stop: %v", err)
		}
	}
	c2, processed2 := newTestCollector()
	if err := q.Run(c2); err != nil {
		t.Fatalf("Run after Stop: %v", err)
	}
	if got := int(atomic.LoadUint32(processed2)); got != wantRemaining+extra {
		t.Fatalf("post-restart processed %d requests, want %d", got, wantRemaining+extra)
	}
	if !q.IsEmpty() {
		t.Fatal("queue should be empty after restart drain")
	}
}

// TestStopWhileIdle verifies calling Stop before any Run, and after a Run,
// does not poison the lifecycle of subsequent rounds.
func TestStopWhileIdle(t *testing.T) {
	server := testServer()
	defer server.Close()

	q := newTestQueue(2, &InMemoryQueueStorage{MaxSize: 1000})
	q.Stop() // must be a no-op

	const n = 10
	for i := 0; i < n; i++ {
		if err := q.AddURL(testURL(server, i)); err != nil {
			t.Fatalf("AddURL: %v", err)
		}
	}
	c, processed := newTestCollector()
	if err := q.Run(c); err != nil {
		t.Fatalf("Run after idle Stop: %v", err)
	}
	if got := atomic.LoadUint32(processed); got != n {
		t.Fatalf("processed %d, want %d", got, n)
	}
	q.Stop() // idle again: still a no-op
}

// TestDuplicateRunPanics locks in the historical duplicate-Run semantics.
func TestDuplicateRunPanics(t *testing.T) {
	server := testServer()
	defer server.Close()

	q := newTestQueue(2, &InMemoryQueueStorage{MaxSize: 1000})
	// Keep the first round unambiguously busy: many slow requests guarantee
	// it is still running when the duplicate call is made.
	for i := 0; i < 20; i++ {
		if err := q.AddURL(server.URL + "/delay?t=200ms"); err != nil {
			t.Fatalf("AddURL: %v", err)
		}
	}
	c, _ := newTestCollector()
	done := runAsync(t, q, c)

	// Wait until the round is observably consuming work, so the duplicate
	// call cannot race with round teardown.
	waitFor(t, 2*time.Second, func() bool {
		size, _ := q.Size()
		return size < 20
	}, "first round never started")

	// The duplicate call happens in its own goroutine, so the expected panic
	// must be recovered there rather than allowed to crash the test binary.
	panicked := make(chan struct{})
	go func() {
		defer close(panicked)
		defer func() {
			_ = recover()
		}()
		q.Run(c)
	}()
	select {
	case <-panicked:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent Queue.Run calls should panic")
	}
	q.Stop()
	if err := waitDone(t, done, 10*time.Second, "stopped-round"); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// After the round ends, Run must be usable again and must drain every
	// request that survived Stop plus the newly added one.
	remaining, err := q.Size()
	if err != nil {
		t.Fatalf("Size after stop: %v", err)
	}
	c2, processed := newTestCollector()
	if err := q.AddURL(testURL(server, 1)); err != nil {
		t.Fatalf("AddURL: %v", err)
	}
	if err := q.Run(c2); err != nil {
		t.Fatalf("Run after stopped round: %v", err)
	}
	if got := int(atomic.LoadUint32(processed)); got != remaining+1 {
		t.Fatalf("processed %d, want %d (leftover %d + 1)", got, remaining+1, remaining)
	}
	if !q.IsEmpty() {
		t.Fatal("queue should be empty after restart drain")
	}
}

// TestConcurrentAddNoLossNoLeak hammers AddRequest from many goroutines both
// while the queue is running and while it is idle. Every added request must be
// processed exactly once and no AddRequest goroutine may remain blocked.
func TestConcurrentAddNoLossNoLeak(t *testing.T) {
	server := testServer()
	defer server.Close()

	q := newTestQueue(8, &InMemoryQueueStorage{MaxSize: 0}) // unlimited
	c, processed := newTestCollector()

	const producers, perProducer = 12, 100
	var added uint32
	var wg sync.WaitGroup

	// Phase 1: concurrent adds into the idle queue.
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				if err := q.AddURL(testURL(server, i)); err != nil {
					t.Errorf("idle AddURL: %v", err)
					return
				}
				atomic.AddUint32(&added, 1)
			}
		}()
	}
	wg.Wait()
	if size, _ := q.Size(); size != producers*perProducer {
		t.Fatalf("idle queue size = %d, want %d", size, producers*perProducer)
	}

	// Phase 2: run while more requests are added concurrently.
	done := runAsync(t, q, c)
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				if err := q.AddURL(testURL(server, i)); err != nil {
					t.Errorf("live AddURL: %v", err)
					return
				}
				atomic.AddUint32(&added, 1)
			}
		}()
	}
	wg.Wait()
	if err := waitDone(t, done, 30*time.Second, "concurrent-drain"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := atomic.LoadUint32(processed); got != added {
		t.Fatalf("processed %d requests, want %d (no task may be lost)", got, added)
	}
	if !q.IsEmpty() {
		t.Fatal("queue should be empty after concurrent drain")
	}

	// Phase 3: the queue must still be reusable and accept adds without
	// blocking (regression guard for the leaked-sender hang).
	blocked := make(chan struct{})
	go func() {
		_ = q.AddURL(testURL(server, 1))
		close(blocked)
	}()
	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("AddRequest on the drained queue blocked forever")
	}
	c2, processed2 := newTestCollector()
	if err := q.Run(c2); err != nil {
		t.Fatalf("restart Run: %v", err)
	}
	if got := atomic.LoadUint32(processed2); got != 1 {
		t.Fatalf("processed %d, want 1", got)
	}
}

// TestStopUnderConcurrentAdd ensures Stop while producers are active neither
// loses requests nor leaves producers blocked; a subsequent Run finishes all
// work and no goroutines leak.
func TestStopUnderConcurrentAdd(t *testing.T) {
	server := testServer()
	defer server.Close()

	q := newTestQueue(4, &InMemoryQueueStorage{MaxSize: 0})
	c, processed := newTestCollector()
	for i := 0; i < 4; i++ {
		if err := q.AddURL(server.URL + "/delay?t=100ms"); err != nil {
			t.Fatalf("AddURL: %v", err)
		}
	}
	done := runAsync(t, q, c)

	var added uint32
	var producers sync.WaitGroup
	stopProducers := make(chan struct{})
	const producerCount, perProducer = 8, 60
	for p := 0; p < producerCount; p++ {
		producers.Add(1)
		go func() {
			defer producers.Done()
			for i := 0; i < perProducer; i++ {
				select {
				case <-stopProducers:
					return
				default:
				}
				if err := q.AddURL(testURL(server, i)); err != nil {
					t.Errorf("AddURL: %v", err)
					return
				}
				atomic.AddUint32(&added, 1)
				// Yield so producers interleave with consumers instead of
				// exhausting local TCP ports in a tight allocation loop.
				runtime.Gosched()
			}
		}()
	}

	// Let producers race with a running round, then stop once they have had
	// time to enqueue a substantial batch.
	time.Sleep(100 * time.Millisecond)
	q.Stop()
	if err := waitDone(t, done, 10*time.Second, "stop-under-load"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	close(stopProducers)

	producersDone := make(chan struct{})
	go func() {
		producers.Wait()
		close(producersDone)
	}()
	select {
	case <-producersDone:
	case <-time.After(5 * time.Second):
		t.Fatal("AddRequest producers stayed blocked after Stop")
	}

	// Storage accounting must be exact: nothing was lost.
	want := int(added) + 4 - int(atomic.LoadUint32(processed))
	size, _ := q.Size()
	if size != want {
		t.Fatalf("remaining size = %d, want %d", size, want)
	}

	// Restart and drain everything.
	c2, processed2 := newTestCollector()
	if err := q.Run(c2); err != nil {
		t.Fatalf("restart Run: %v", err)
	}
	if int(atomic.LoadUint32(processed2)) != want {
		t.Fatalf("post-restart processed %d, want %d", processed2, want)
	}

	// After the full lifecycle no queue-owned goroutine (loop or worker) may
	// remain. HTTP transport keep-alive goroutines are deliberately excluded.
	waitFor(t, 5*time.Second, func() bool {
		return countQueueGoroutines() == 0
	}, "queue goroutines leaked after full lifecycle")
}
