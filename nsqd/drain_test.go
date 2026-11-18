package nsqd

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/test"
	"github.com/nsqio/nsq/nsqlookupd"
)

type pubOutcome int

const (
	// the server confirmed the publish (it is queued)
	pubConfirmed pubOutcome = iota
	// the server explicitly rejected the publish (it is not queued)
	pubRejected
	// the publish outcome is unknown (transport error or timeout)
	pubUncertain
)

// drainPublisher publishes synchronously, keeping one raw connection per node
type drainPublisher struct {
	conns map[string]net.Conn
}

func newDrainPublisher() *drainPublisher {
	return &drainPublisher{conns: make(map[string]net.Conn)}
}

func (p *drainPublisher) reset(addr string) {
	if conn, ok := p.conns[addr]; ok {
		_ = conn.Close()
		delete(p.conns, addr)
	}
}

func (p *drainPublisher) publish(addr string, topicName string, body []byte) pubOutcome {
	conn, ok := p.conns[addr]
	if !ok {
		var err error
		conn, err = net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			// the message was never sent, this is a definitive failure
			return pubRejected
		}
		if _, err := conn.Write(nsq.MagicV2); err != nil {
			_ = conn.Close()
			return pubRejected
		}
		p.conns[addr] = conn
	}

	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))

	if _, err := nsq.Publish(topicName, body).WriteTo(conn); err != nil {
		p.reset(addr)
		return pubUncertain
	}

	resp, err := nsq.ReadResponse(conn)
	if err != nil {
		p.reset(addr)
		return pubUncertain
	}
	frameType, data, err := nsq.UnpackResponse(resp)
	if err != nil {
		p.reset(addr)
		return pubUncertain
	}
	if frameType == nsq.FrameTypeResponse && string(data) == "OK" {
		return pubConfirmed
	}
	if frameType == nsq.FrameTypeError {
		return pubRejected
	}
	return pubUncertain
}

func (p *drainPublisher) close() {
	for addr := range p.conns {
		p.reset(addr)
	}
}

func lookupProducerAddrs(t *testing.T, lookupdHTTPAddr string, topicName string) []string {
	var lr struct {
		Producers []struct {
			BroadcastAddress string `json:"broadcast_address"`
			TCPPort          int    `json:"tcp_port"`
		} `json:"producers"`
	}
	endpoint := fmt.Sprintf("http://%s/lookup?topic=%s", lookupdHTTPAddr, topicName)
	err := http_api.NewClient(nil, ConnectTimeout, RequestTimeout).GETV1(endpoint, &lr)
	test.Nil(t, err)
	var addrs []string
	for _, p := range lr.Producers {
		addrs = append(addrs, net.JoinHostPort(p.BroadcastAddress, strconv.Itoa(p.TCPPort)))
	}
	return addrs
}

func waitForProducers(t *testing.T, lookupdHTTPAddr string, topicName string, num int) []string {
	deadline := time.Now().Add(10 * time.Second)
	for {
		addrs := lookupProducerAddrs(t, lookupdHTTPAddr, topicName)
		if len(addrs) == num {
			return addrs
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d producers of %s, have %v", num, topicName, addrs)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func topicDepth(t *testing.T, httpAddr net.Addr, topicName string) int64 {
	var stats struct {
		Topics []struct {
			TopicName string `json:"topic_name"`
			Depth     int64  `json:"depth"`
		} `json:"topics"`
	}
	endpoint := fmt.Sprintf("http://%s/stats?format=json", httpAddr)
	err := http_api.NewClient(nil, ConnectTimeout, RequestTimeout).GETV1(endpoint, &stats)
	test.Nil(t, err)
	for _, ts := range stats.Topics {
		if ts.TopicName == topicName {
			return ts.Depth
		}
	}
	return 0
}

// TestDrainNoMessageLoss removes a node from the cluster while publishers
// with slightly stale topology views keep writing, and asserts that no
// message is lost or duplicated: every publish gets a definitive outcome
// and every confirmed message ends up queued exactly once.
func TestDrainNoMessageLoss(t *testing.T) {
	drainClusterTest(t, 500*time.Millisecond, true)
}

// TestDrainTinyNotificationPeriod repeats the same test with the
// notification period tuned to (almost) zero. Without the drain logic
// this reproduces the message loss; with it, messages must still be
// neither lost nor duplicated.
func TestDrainTinyNotificationPeriod(t *testing.T) {
	drainClusterTest(t, 0, false)
}

func drainClusterTest(t *testing.T, drainMinWait time.Duration, assertNoUncertain bool) {
	lopts := nsqlookupd.NewOptions()
	lopts.Logger = test.NewTestLogger(t)
	lopts.BroadcastAddress = "127.0.0.1"
	_, _, lookupd := mustStartNSQLookupd(lopts)
	defer lookupd.Exit()

	newNodeOpts := func() *Options {
		opts := NewOptions()
		opts.Logger = test.NewTestLogger(t)
		opts.BroadcastAddress = "127.0.0.1"
		opts.NSQLookupdTCPAddresses = []string{lookupd.RealTCPAddr().String()}
		opts.DrainMinWait = drainMinWait
		opts.DrainMaxWait = 5 * time.Second
		return opts
	}

	optsA := newNodeOpts()
	_, httpAddrA, nodeA := mustStartNSQD(optsA)
	defer func() { _ = os.RemoveAll(optsA.DataPath) }()

	optsB := newNodeOpts()
	_, httpAddrB, nodeB := mustStartNSQD(optsB)
	defer func() { _ = os.RemoveAll(optsB.DataPath) }()
	defer nodeB.Exit()

	topicName := "drain_test" + strconv.Itoa(int(time.Now().UnixNano()))

	// create the topic on both nodes so that both register with nsqlookupd
	for _, httpAddr := range []net.Addr{httpAddrA, httpAddrB} {
		url := fmt.Sprintf("http://%s/topic/create?topic=%s", httpAddr, topicName)
		err := http_api.NewClient(nil, ConnectTimeout, RequestTimeout).POSTV1(url, nil, nil)
		test.Nil(t, err)
	}

	lookupdHTTPAddr := lookupd.RealHTTPAddr().String()
	waitForProducers(t, lookupdHTTPAddr, topicName, 2)

	// the shared cluster view, refreshed from nsqlookupd with some delay,
	// simulating producers with a stale topology view
	var view atomic.Value
	view.Store(waitForProducers(t, lookupdHTTPAddr, topicName, 2))
	stopRefresh := make(chan struct{})
	defer close(stopRefresh)
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopRefresh:
				return
			case <-ticker.C:
				if addrs := lookupProducerAddrs(t, lookupdHTTPAddr, topicName); len(addrs) > 0 {
					view.Store(addrs)
				}
			}
		}
	}()

	const numMessages = 1000
	const numPublishers = 8

	var nextSeq int64
	var confirmed int64
	var rejected int64
	var uncertain int64
	var undelivered int64

	// remove node A from the cluster mid-stream
	exitDone := make(chan struct{})
	go func() {
		for atomic.LoadInt64(&confirmed) < numMessages/3 {
			time.Sleep(5 * time.Millisecond)
		}
		nodeA.Exit()
		close(exitDone)
	}()

	var wg sync.WaitGroup
	for i := 0; i < numPublishers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pub := newDrainPublisher()
			defer pub.close()
			for {
				seq := atomic.AddInt64(&nextSeq, 1)
				if seq > numMessages {
					return
				}
				body := []byte(strconv.FormatInt(seq, 10))
				delivered := false
				var lastAddr string
				for attempt := 0; attempt < 500 && !delivered; attempt++ {
					addrs := view.Load().([]string)
					if len(addrs) == 0 {
						time.Sleep(20 * time.Millisecond)
						continue
					}
					addr := addrs[int(seq+int64(attempt))%len(addrs)]
					if addr == lastAddr && len(addrs) > 1 {
						// retry on another node
						addr = addrs[(int(seq+int64(attempt))+1)%len(addrs)]
					}
					switch pub.publish(addr, topicName, body) {
					case pubConfirmed:
						atomic.AddInt64(&confirmed, 1)
						delivered = true
					case pubRejected:
						atomic.AddInt64(&rejected, 1)
						lastAddr = addr
					case pubUncertain:
						atomic.AddInt64(&uncertain, 1)
						lastAddr = addr
					}
				}
				if !delivered {
					atomic.AddInt64(&undelivered, 1)
				}
				time.Sleep(2 * time.Millisecond)
			}
		}()
	}
	wg.Wait()
	<-exitDone

	test.Equal(t, int64(0), atomic.LoadInt64(&undelivered))
	test.Equal(t, int64(numMessages), atomic.LoadInt64(&confirmed))
	if assertNoUncertain {
		// with a notification period that covers the view refresh, every
		// publish must get a definitive outcome (OK or explicit error)
		test.Equal(t, int64(0), atomic.LoadInt64(&uncertain))
	}

	// the removed node must have unregistered itself from nsqlookupd
	waitForProducers(t, lookupdHTTPAddr, topicName, 1)

	depthB := topicDepth(t, httpAddrB, topicName)

	// restart node A from its data path to count what it actually persisted
	optsA2 := NewOptions()
	optsA2.Logger = test.NewTestLogger(t)
	optsA2.DataPath = optsA.DataPath
	_, httpAddrA2, nodeA2 := mustStartNSQD(optsA2)
	defer nodeA2.Exit()
	// mustStartNSQD does not load metadata; opening the topic recovers
	// the depth persisted in its disk queue
	nodeA2.GetTopic(topicName)
	depthA := topicDepth(t, httpAddrA2, topicName)

	t.Logf("confirmed=%d rejected=%d uncertain=%d depthA=%d depthB=%d",
		confirmed, rejected, uncertain, depthA, depthB)

	// no message was lost after being confirmed and no message was
	// queued twice because of a retried publish
	test.Equal(t, int64(numMessages), depthA+depthB)
}

// TestDrainRejectsNewPublishes verifies that during the drain window new
// publishes fail explicitly (E_DRAINING) instead of being silently dropped
func TestDrainRejectsNewPublishes(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.DrainMinWait = 300 * time.Millisecond
	opts.DrainMaxWait = 5 * time.Second
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()

	topicName := "drain_reject" + strconv.Itoa(int(time.Now().UnixNano()))

	conn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer func() { _ = conn.Close() }()

	// publishes work before draining
	_, err = nsq.Publish(topicName, []byte("before")).WriteTo(conn)
	test.Nil(t, err)
	readValidate(t, conn, frameTypeResponse, "OK")

	start := time.Now()
	exitDone := make(chan struct{})
	go func() {
		nsqd.Exit()
		close(exitDone)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for !nsqd.IsDraining() {
		if time.Now().After(deadline) {
			t.Fatal("nsqd did not start draining")
		}
		time.Sleep(5 * time.Millisecond)
	}

	mpubCmd, err := nsq.MultiPublish(topicName, [][]byte{[]byte("during")})
	test.Nil(t, err)
	dpubCmd := nsq.DeferredPublish(topicName, time.Second, []byte("during"))

	for i, cmd := range []*nsq.Command{
		nsq.Publish(topicName, []byte("during")),
		mpubCmd,
		dpubCmd,
	} {
		_, err := cmd.WriteTo(conn)
		test.Nil(t, err)
		resp, err := nsq.ReadResponse(conn)
		test.Nil(t, err)
		frameType, data, err := nsq.UnpackResponse(resp)
		test.Nil(t, err)
		if frameType != frameTypeError {
			t.Fatalf("cmd %d: expected error frame, got %d %s", i, frameType, data)
		}
		if !strings.Contains(string(data), "E_DRAINING") {
			t.Fatalf("cmd %d: expected E_DRAINING, got %s", i, data)
		}
	}

	<-exitDone
	if elapsed := time.Since(start); elapsed < 300*time.Millisecond {
		t.Fatalf("drain completed before the notification period: %s", elapsed)
	}
}

// TestDrainWaitsForInFlightPubs verifies that the drain window adapts to
// the number of in-flight publishes
func TestDrainWaitsForInFlightPubs(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.DrainMinWait = 0
	opts.DrainMaxWait = 5 * time.Second
	_, _, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()

	if !nsqd.beginPub() {
		t.Fatal("beginPub should succeed before draining")
	}

	exitDone := make(chan struct{})
	go func() {
		nsqd.Exit()
		close(exitDone)
	}()

	select {
	case <-exitDone:
		t.Fatal("Exit completed while a publish was still in-flight")
	case <-time.After(200 * time.Millisecond):
	}

	nsqd.endPub()

	select {
	case <-exitDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Exit did not complete after the in-flight publish finished")
	}
}

// TestDrainMaxWaitBoundsWindow verifies that the drain window is bounded
// by DrainMaxWait so that node removal cannot stall
func TestDrainMaxWaitBoundsWindow(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.DrainMinWait = time.Hour
	opts.DrainMaxWait = 300 * time.Millisecond
	_, _, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()

	if !nsqd.beginPub() {
		t.Fatal("beginPub should succeed before draining")
	}
	// never released: the max wait must still bound the drain window

	start := time.Now()
	nsqd.Exit()
	elapsed := time.Since(start)

	if elapsed < 300*time.Millisecond {
		t.Fatalf("drain completed before the max wait: %s", elapsed)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("drain window was not bounded by max wait: %s", elapsed)
	}
}
