package nsqd

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nsqio/nsq/internal/test"
)

// ensure that we can push a message through a topic and get it out of a channel
func TestPutMessage(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()
	defer nsqd.Exit()

	topicName := "test_put_message" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel1 := topic.GetChannel("ch")

	var id MessageID
	msg := NewMessage(id, []byte("test"))
	err := topic.PutMessage(msg)
	test.Nil(t, err)

	outputMsg := <-channel1.memoryMsgChan
	test.Equal(t, msg.ID, outputMsg.ID)
	test.Equal(t, msg.Body, outputMsg.Body)
}

// ensure that both channels get the same message
func TestPutMessage2Chan(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()
	defer nsqd.Exit()

	topicName := "test_put_message_2chan" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel1 := topic.GetChannel("ch1")
	channel2 := topic.GetChannel("ch2")

	var id MessageID
	msg := NewMessage(id, []byte("test"))
	err := topic.PutMessage(msg)
	test.Nil(t, err)

	outputMsg1 := <-channel1.memoryMsgChan
	test.Equal(t, msg.ID, outputMsg1.ID)
	test.Equal(t, msg.Body, outputMsg1.Body)

	outputMsg2 := <-channel2.memoryMsgChan
	test.Equal(t, msg.ID, outputMsg2.ID)
	test.Equal(t, msg.Body, outputMsg2.Body)
}

func TestInFlightWorker(t *testing.T) {
	count := 250

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.MsgTimeout = 100 * time.Millisecond
	opts.QueueScanRefreshInterval = 100 * time.Millisecond
	_, _, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()
	defer nsqd.Exit()

	topicName := "test_in_flight_worker" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("channel")

	for i := 0; i < count; i++ {
		msg := NewMessage(topic.GenerateID(), []byte("test"))
		err := channel.StartInFlightTimeout(msg, 0, opts.MsgTimeout)
		test.Nil(t, err)
	}

	channel.Lock()
	inFlightMsgs := len(channel.inFlightMessages)
	channel.Unlock()
	test.Equal(t, count, inFlightMsgs)

	channel.inFlightMutex.Lock()
	inFlightPQMsgs := len(channel.inFlightPQ)
	channel.inFlightMutex.Unlock()
	test.Equal(t, count, inFlightPQMsgs)

	// the in flight worker has a resolution of 100ms so we need to wait
	// at least that much longer than our msgTimeout (in worst case)
	time.Sleep(4 * opts.MsgTimeout)

	channel.Lock()
	inFlightMsgs = len(channel.inFlightMessages)
	channel.Unlock()
	test.Equal(t, 0, inFlightMsgs)

	channel.inFlightMutex.Lock()
	inFlightPQMsgs = len(channel.inFlightPQ)
	channel.inFlightMutex.Unlock()
	test.Equal(t, 0, inFlightPQMsgs)
}

func TestDeferredQueuePaused(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()
	defer nsqd.Exit()

	topicName := "test_deferred_paused" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("channel")

	err := channel.Pause()
	test.Nil(t, err)

	msg := NewMessage(topic.GenerateID(), []byte("test"))
	err = channel.StartDeferredTimeout(msg, 50*time.Millisecond)
	test.Nil(t, err)

	// well after the deadline, the message must stay in the deferred queue
	// instead of being moved to the ready queue while paused
	time.Sleep(300 * time.Millisecond)

	test.Equal(t, 1, channel.DeferredCount())
	test.Equal(t, 0, len(channel.memoryMsgChan))
	test.Equal(t, int64(0), channel.Depth())

	// the scan worker must not report the channel dirty while paused
	now := time.Now().UnixNano()
	test.Equal(t, false, channel.processDeferredQueue(now))

	err = channel.UnPause()
	test.Nil(t, err)

	// after resume the deadline was shifted by the pause duration, so the
	// message is still deferred (not released instantly in a burst)
	test.Equal(t, 1, channel.DeferredCount())
	test.Equal(t, 0, len(channel.memoryMsgChan))

	channel.deferredMutex.Lock()
	item := channel.deferredPQ[0]
	channel.deferredMutex.Unlock()
	test.Equal(t, true, item.Priority > now)
}

func TestDeferredQueueResumeReschedule(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()
	defer nsqd.Exit()

	topicName := "test_deferred_resume" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("channel")

	err := channel.Pause()
	test.Nil(t, err)

	// a message that becomes due during the pause
	dueMsg := NewMessage(topic.GenerateID(), []byte("due"))
	err = channel.StartDeferredTimeout(dueMsg, 30*time.Millisecond)
	test.Nil(t, err)

	// a message whose deadline stays in the future across the pause
	futureMsg := NewMessage(topic.GenerateID(), []byte("future"))
	err = channel.StartDeferredTimeout(futureMsg, 10*time.Second)
	test.Nil(t, err)

	time.Sleep(150 * time.Millisecond)

	pausedAt := atomic.LoadInt64(&channel.pausedAt)

	channel.deferredMutex.Lock()
	originalFuturePriority := channel.deferredMessages[futureMsg.ID].Priority
	channel.deferredMutex.Unlock()

	err = channel.UnPause()
	test.Nil(t, err)

	resumeAt := time.Now().UnixNano()

	channel.deferredMutex.Lock()
	dueItem := channel.deferredMessages[dueMsg.ID]
	futureItem := channel.deferredMessages[futureMsg.ID]
	duePriority := dueItem.Priority
	futurePriority := futureItem.Priority
	channel.deferredMutex.Unlock()

	// the due message gets pushed out by roughly the pause duration
	test.Equal(t, true, duePriority >= resumeAt)
	test.Equal(t, true, duePriority-pausedAt > int64(100*time.Millisecond))

	// the not-yet-due message keeps its original (unshifted) deadline
	test.Equal(t, originalFuturePriority, futurePriority)
	test.Equal(t, true, futurePriority > duePriority)

	// the heap still orders by priority
	channel.deferredMutex.Lock()
	minPriority := channel.deferredPQ[0].Priority
	channel.deferredMutex.Unlock()
	test.Equal(t, duePriority, minPriority)
}

func TestDeferredQueuePauseKeepsBacklogVisible(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()
	defer nsqd.Exit()

	topicName := "test_deferred_backlog" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("channel")

	err := channel.Pause()
	test.Nil(t, err)

	for i := 0; i < 5; i++ {
		msg := NewMessage(topic.GenerateID(), []byte("test"))
		err = channel.StartDeferredTimeout(msg, 25*time.Millisecond)
		test.Nil(t, err)
	}

	time.Sleep(200 * time.Millisecond)

	stats := NewChannelStats(channel, nil, 0)
	test.Equal(t, 5, stats.DeferredCount)
	test.Equal(t, true, stats.Paused)
	test.Equal(t, 5, channel.DeferredCount())

	err = channel.UnPause()
	test.Nil(t, err)
	test.Equal(t, 5, channel.DeferredCount())
}

func TestChannelEmpty(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()
	defer nsqd.Exit()

	topicName := "test_channel_empty" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("channel")

	msgs := make([]*Message, 0, 25)
	for i := 0; i < 25; i++ {
		msg := NewMessage(topic.GenerateID(), []byte("test"))
		err := channel.StartInFlightTimeout(msg, 0, opts.MsgTimeout)
		test.Nil(t, err)
		msgs = append(msgs, msg)
	}

	err := channel.RequeueMessage(0, msgs[len(msgs)-1].ID, 100*time.Millisecond)
	test.Nil(t, err)
	test.Equal(t, 24, len(channel.inFlightMessages))
	test.Equal(t, 24, len(channel.inFlightPQ))
	test.Equal(t, 1, len(channel.deferredMessages))
	test.Equal(t, 1, len(channel.deferredPQ))

	err = channel.Empty()
	test.Nil(t, err)

	test.Equal(t, 0, len(channel.inFlightMessages))
	test.Equal(t, 0, len(channel.inFlightPQ))
	test.Equal(t, 0, len(channel.deferredMessages))
	test.Equal(t, 0, len(channel.deferredPQ))
	test.Equal(t, int64(0), channel.Depth())
}

func TestChannelEmptyConsumer(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()
	defer nsqd.Exit()

	conn, _ := mustConnectNSQD(tcpAddr)
	defer func() { _ = conn.Close() }()

	topicName := "test_channel_empty" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("channel")
	client := newClientV2(0, conn, nsqd)
	client.SetReadyCount(25)
	err := channel.AddClient(client.ID, client)
	test.Equal(t, err, nil)

	for i := 0; i < 25; i++ {
		msg := NewMessage(topic.GenerateID(), []byte("test"))
		err = channel.StartInFlightTimeout(msg, 0, opts.MsgTimeout)
		test.Nil(t, err)
		client.SendingMessage()
	}

	for _, cl := range channel.clients {
		stats := cl.Stats("").(ClientV2Stats)
		test.Equal(t, int64(25), stats.InFlightCount)
	}

	err = channel.Empty()
	test.Nil(t, err)

	for _, cl := range channel.clients {
		stats := cl.Stats("").(ClientV2Stats)
		test.Equal(t, int64(0), stats.InFlightCount)
	}
}

func TestMaxChannelConsumers(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.MaxChannelConsumers = 1
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()
	defer nsqd.Exit()

	conn, _ := mustConnectNSQD(tcpAddr)
	defer func() { _ = conn.Close() }()

	topicName := "test_max_channel_consumers" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("channel")

	client1 := newClientV2(1, conn, nsqd)
	client1.SetReadyCount(25)
	err := channel.AddClient(client1.ID, client1)
	test.Equal(t, err, nil)

	client2 := newClientV2(2, conn, nsqd)
	client2.SetReadyCount(25)
	err = channel.AddClient(client2.ID, client2)
	test.NotEqual(t, err, nil)
}

func TestChannelHealth(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.MemQueueSize = 2

	_, httpAddr, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test")

	channel := topic.GetChannel("channel")

	channel.backend = &errorBackendQueue{}

	msg := NewMessage(topic.GenerateID(), make([]byte, 100))
	err := channel.PutMessage(msg)
	test.Nil(t, err)

	msg = NewMessage(topic.GenerateID(), make([]byte, 100))
	err = channel.PutMessage(msg)
	test.Nil(t, err)

	msg = NewMessage(topic.GenerateID(), make([]byte, 100))
	err = channel.PutMessage(msg)
	test.NotNil(t, err)

	url := fmt.Sprintf("http://%s/ping", httpAddr)
	resp, err := http.Get(url)
	test.Nil(t, err)
	test.Equal(t, 500, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	err = resp.Body.Close()
	test.Nil(t, err)
	test.Equal(t, "NOK - never gonna happen", string(body))

	channel.backend = &errorRecoveredBackendQueue{}

	msg = NewMessage(topic.GenerateID(), make([]byte, 100))
	err = channel.PutMessage(msg)
	test.Nil(t, err)

	resp, err = http.Get(url)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ = io.ReadAll(resp.Body)
	err = resp.Body.Close()
	test.Nil(t, err)
	test.Equal(t, "OK", string(body))
}
