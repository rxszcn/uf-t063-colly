package queue

import (
	"net/url"
	"sync"

	whatwgUrl "github.com/nlnwa/whatwg-url/url"

	"github.com/gocolly/colly/v2"
)

var urlParser = whatwgUrl.NewParser(whatwgUrl.WithPercentEncodeSinglePercentSign())

// Storage is the interface of the queue's storage backend
// Storage must be concurrently safe for multiple goroutines.
type Storage interface {
	// Init initializes the storage
	Init() error
	// AddRequest adds a serialized request to the queue
	AddRequest([]byte) error
	// GetRequest pops the next request from the queue
	// or returns error if the queue is empty
	GetRequest() ([]byte, error)
	// QueueSize returns with the size of the queue
	QueueSize() (int, error)
}

// qState is the lifecycle state of a Queue.
type qState int

const (
	// stateIdle means no Run call is active. It is the initial state and the
	// state entered after every Run round finishes (normal drain or Stop).
	stateIdle qState = iota
	// stateRunning means a Run call is active and its dispatch loop is
	// driving the consumer threads.
	stateRunning
)

// Queue is a request queue which uses a Collector to consume
// requests in multiple threads
type Queue struct {
	// Threads defines the number of consumer threads
	Threads int
	storage Storage
	// mut guards state, wake, stopCh and stopped.
	mut sync.Mutex
	// state is the lifecycle state of the queue.
	state qState
	// wake nudges the dispatch loop to re-check storage for new requests.
	// It is non-nil only while state == stateRunning, has capacity one so
	// concurrent signals coalesce instead of blocking, and is always sent
	// to in a non-blocking way.
	wake chan struct{}
	// stopCh is closed once by Stop to request the active round to shut
	// down. It is non-nil only while state == stateRunning.
	stopCh chan struct{}
	// stopped records whether Stop has been requested for the active round,
	// making repeated Stop calls safe no-ops. Guarded by mut.
	stopped bool
}

// InMemoryQueueStorage is the default implementation of the Storage interface.
// InMemoryQueueStorage holds the request queue in memory.
type InMemoryQueueStorage struct {
	// MaxSize defines the capacity of the queue.
	// New requests are discarded if the queue size reaches MaxSize
	MaxSize int
	lock    *sync.RWMutex
	size    int
	first   *inMemoryQueueItem
	last    *inMemoryQueueItem
}

type inMemoryQueueItem struct {
	Request []byte
	Next    *inMemoryQueueItem
}

// New creates a new queue with a Storage specified in argument
// A standard InMemoryQueueStorage is used if Storage argument is nil.
func New(threads int, s Storage) (*Queue, error) {
	if s == nil {
		s = &InMemoryQueueStorage{MaxSize: 100000}
	}
	if err := s.Init(); err != nil {
		return nil, err
	}
	return &Queue{
		Threads: threads,
		storage: s,
		// state defaults to stateIdle, the zero value of qState.
	}, nil
}

// IsEmpty returns true if the queue is empty
func (q *Queue) IsEmpty() bool {
	s, _ := q.Size()
	return s == 0
}

// AddURL adds a new URL to the queue
func (q *Queue) AddURL(URL string) error {
	u, err := urlParser.Parse(URL)
	if err != nil {
		return err
	}
	u2, err := url.Parse(u.Href(false))
	if err != nil {
		return err
	}
	r := &colly.Request{
		URL:    u2,
		Method: "GET",
	}
	return q.AddRequest(r)
}

// AddRequest adds a new Request to the queue.
//
// AddRequest never blocks and never loses a request: the request is persisted
// to storage first, and the dispatch loop is only nudged with a non-blocking
// signal when a Run round is active. When the queue is idle (before the first
// Run, after a round has drained, or after Stop) the request simply remains in
// storage and is consumed by the next Run call.
func (q *Queue) AddRequest(r *colly.Request) error {
	if err := q.storeRequest(r); err != nil {
		return err
	}
	q.mut.Lock()
	wake := q.wake
	q.mut.Unlock()
	if wake == nil {
		return nil
	}
	// Capacity one means a pending nudge is already enough to make the loop
	// re-check storage; if the channel is full the signal is coalesced
	// without dropping the request that was just persisted.
	select {
	case wake <- struct{}{}:
	default:
	}
	return nil
}

func (q *Queue) storeRequest(r *colly.Request) error {
	d, err := r.Marshal()
	if err != nil {
		return err
	}
	return q.storage.AddRequest(d)
}

// Size returns the size of the queue
func (q *Queue) Size() (int, error) {
	return q.storage.QueueSize()
}

// Run starts consumer threads and calls the Collector
// to perform requests. Run blocks while the queue has active requests.
// The given Storage must not be used directly while Run blocks.
//
// Run panics when invoked while another round is already active, preserving
// the historical duplicate-Run behavior. Once a round finishes — either by
// draining the queue or by Stop — Run may be invoked again; requests added to
// the idle queue are then consumed by the new round.
func (q *Queue) Run(c *colly.Collector) error {
	q.mut.Lock()
	if q.state == stateRunning {
		q.mut.Unlock()
		panic("cannot call duplicate Queue.Run")
	}
	wake := make(chan struct{}, 1)
	stopCh := make(chan struct{})
	q.wake = wake
	q.stopCh = stopCh
	q.stopped = false
	q.state = stateRunning
	q.mut.Unlock()

	// Both channels are unbuffered, so handing a request to a worker is a
	// synchronous handshake and the number of outstanding requests always
	// equals the number of in-flight requests (<= Threads). This keeps the
	// loop's active counter exact and prevents completion signals from being
	// lost or buffered past the round's lifetime.
	requestc := make(chan *colly.Request)
	complete := make(chan struct{})
	errc := make(chan error, 1)

	var wg sync.WaitGroup
	for i := 0; i < q.Threads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			independentRunner(requestc, complete)
		}()
	}
	go q.loop(c, requestc, complete, wake, stopCh, errc)

	err := <-errc
	// Closing requestc releases workers blocked waiting for work. Workers
	// already executing a request finish it first; Run then waits for every
	// worker goroutine to wind down before returning.
	close(requestc)
	wg.Wait()

	q.mut.Lock()
	q.state = stateIdle
	q.wake = nil
	q.stopCh = nil
	q.stopped = false
	q.mut.Unlock()
	return err
}

// Stop requests the running queue to stop at the next safe point.
//
// Requests currently being executed are allowed to finish, while requests
// still held in storage are preserved and consumed by a subsequent Run call.
// Stop is idempotent and safe to call on an idle queue; in that case it is a
// no-op and has no effect on a later Run call.
func (q *Queue) Stop() {
	q.mut.Lock()
	defer q.mut.Unlock()
	if q.state != stateRunning || q.stopped {
		return
	}
	q.stopped = true
	close(q.stopCh)
}

// loop is the dispatch loop of a single Run round. The wake and stopCh
// channels are fixed for the whole round, and only the concurrency-safe
// Storage is otherwise shared, so the loop needs no locking of its own.
//
// On Stop the loop stops dispatching new requests but keeps draining
// completion signals until every in-flight request finishes. This lets the
// workers complete their current request and exit via the closed requestc
// channel during Run's teardown, so no worker goroutine leaks and no in-flight
// task is aborted.
func (q *Queue) loop(c *colly.Collector, requestc chan<- *colly.Request,
	complete <-chan struct{}, wake <-chan struct{}, stopCh <-chan struct{},
	errc chan<- error,
) {
	// active counts requests handed to workers whose completion has not been
	// observed yet. It is local to the round, so it can never carry stale
	// state across Run calls.
	var active int
	for {
		// Once Stop has been requested, dispatch nothing further and only wait
		// for in-flight requests to settle before terminating.
		if q.isStopped() {
			for active > 0 {
				<-complete
				active--
			}
			errc <- nil
			return
		}
		size, err := q.storage.QueueSize()
		if err != nil {
			errc <- err
			return
		}
		// Terminate when the queue is empty and no request is in flight.
		if size == 0 && active == 0 {
			errc <- nil
			return
		}
		sent := requestc
		var req *colly.Request
		var buf []byte
		if size > 0 {
			buf, req, err = q.loadRequest(c)
			if err != nil {
				// Ignore an error returned by GetRequest() or
				// UnmarshalRequest() (e.g. a corrupt queue entry).
				continue
			}
		} else {
			sent = nil
		}
	Sent:
		for {
			select {
			case sent <- req:
				active++
				break Sent
			case <-wake:
				// A new request may have been enqueued while all workers were
				// busy; re-check storage on the next outer iteration.
				if sent == nil {
					break Sent
				}
			case <-complete:
				active--
				if sent == nil && active == 0 {
					break Sent
				}
			case <-stopCh:
				// A request popped from storage but never handed to a worker
				// is returned to storage so the next Run round processes it.
				// With an unbuffered requestc, a handed-off request is already
				// owned by a worker and needs no requeuing here.
				if req != nil {
					if aErr := q.storage.AddRequest(buf); aErr != nil {
						errc <- aErr
						return
					}
				}
				// Drain any requests already executing in workers before
				// terminating; Run then closes requestc and the workers exit.
				for active > 0 {
					<-complete
					active--
				}
				errc <- nil
				return
			}
		}
	}
}

// isStopped reports whether Stop has been requested for the active round.
func (q *Queue) isStopped() bool {
	q.mut.Lock()
	defer q.mut.Unlock()
	return q.stopped
}

// independentRunner consumes requests until requestc is closed. It keeps the
// original hot-path structure (a range over the request channel) so workers
// stay continuously ready to accept the next request, which is important for
// HTTP keep-alive connection reuse. Stop never aborts a request that is already
// executing: Run waits for every worker to finish via its WaitGroup before
// returning, and the closed requestc then lets the range loop exit cleanly.
func independentRunner(requestc <-chan *colly.Request, complete chan<- struct{}) {
	for req := range requestc {
		req.Do()
		complete <- struct{}{}
	}
}

func (q *Queue) loadRequest(c *colly.Collector) ([]byte, *colly.Request, error) {
	buf, err := q.storage.GetRequest()
	if err != nil {
		return nil, nil, err
	}
	copied := make([]byte, len(buf))
	copy(copied, buf)
	req, err := c.UnmarshalRequest(copied)
	if err != nil {
		return nil, nil, err
	}
	return copied, req, nil
}

// Init implements Storage.Init() function
func (q *InMemoryQueueStorage) Init() error {
	q.lock = &sync.RWMutex{}
	return nil
}

// AddRequest implements Storage.AddRequest() function
func (q *InMemoryQueueStorage) AddRequest(r []byte) error {
	q.lock.Lock()
	defer q.lock.Unlock()
	// Discard URLs if size limit exceeded
	if q.MaxSize > 0 && q.size >= q.MaxSize {
		return colly.ErrQueueFull
	}
	i := &inMemoryQueueItem{Request: r}
	if q.first == nil {
		q.first = i
	} else {
		q.last.Next = i
	}
	q.last = i
	q.size++
	return nil
}

// GetRequest implements Storage.GetRequest() function
func (q *InMemoryQueueStorage) GetRequest() ([]byte, error) {
	q.lock.Lock()
	defer q.lock.Unlock()
	if q.size == 0 {
		return nil, nil
	}
	r := q.first.Request
	q.first = q.first.Next
	q.size--
	return r, nil
}

// QueueSize implements Storage.QueueSize() function
func (q *InMemoryQueueStorage) QueueSize() (int, error) {
	q.lock.Lock()
	defer q.lock.Unlock()
	return q.size, nil
}
