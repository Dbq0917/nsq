package nsqd

import (
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/test"
)

// TestManualPauseDeferredDeliveryRate is an on-demand validation of the
// pause/deferred-queue coordination over a long maintenance-style window.
//
// It publishes deferred messages whose deadlines all fall inside the pause
// window, keeps the channel paused for pauseWindow, then resumes and records
// the wall-clock delivery time of every message. With the fix, deadlines are
// shifted by the pause duration on resume, so messages drain gradually instead
// of being delivered in one instantaneous burst.
//
// Run with:
//
//	go test ./nsqd/ -run TestManualPauseDeferredDeliveryRate -v -count=1 -timeout 5m
//
// Set NSQ_PAUSE_WINDOW (e.g. 10m) to reproduce a full ten-minute window.
func TestManualPauseDeferredDeliveryRate(t *testing.T) {
	const numMessages = 30

	pauseWindow := 30 * time.Second
	if d := os.Getenv("NSQ_PAUSE_WINDOW"); d != "" {
		parsed, err := time.ParseDuration(d)
		if err != nil {
			t.Fatalf("invalid NSQ_PAUSE_WINDOW %q: %s", d, err)
		}
		pauseWindow = parsed
	}

	topicName := "manual_pause_defer" + strconv.Itoa(int(time.Now().Unix()))

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	// the test connection is idle for the whole maintenance-style pause window
	opts.ClientTimeout = pauseWindow + 2*time.Minute
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer func() { _ = conn.Close() }()

	identify(t, conn, nil, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("ch")

	// deadlines spread across the first half of the pause window
	spacing := pauseWindow / (numMessages * 2)
	if spacing < 10*time.Millisecond {
		spacing = 10 * time.Millisecond
	}
	for i := 0; i < numMessages; i++ {
		msg := NewMessage(topic.GenerateID(), []byte("test"))
		channel.PutMessageDeferred(msg, time.Duration(i+1)*spacing)
	}

	test.Nil(t, channel.Pause())
	t.Logf("channel paused for %s; %d deferred messages queued, all deadlines inside the window",
		pauseWindow, numMessages)

	time.Sleep(pauseWindow)

	if got := channel.DeferredCount(); got != numMessages {
		t.Fatalf("expected %d messages held deferred during pause, got %d", numMessages, got)
	}

	_, err = nsq.Ready(numMessages).WriteTo(conn)
	test.Nil(t, err)

	resumeAt := time.Now()
	test.Nil(t, channel.UnPause())

	deliveryTimes := make([]time.Time, 0, numMessages)
	for i := 0; i < numMessages; i++ {
		conn.SetReadDeadline(time.Now().Add(pauseWindow + time.Minute))
		resp, err := nsq.ReadResponse(conn)
		if err != nil {
			t.Fatalf("message %d: %s", i, err)
		}
		frameType, data, err := nsq.UnpackResponse(resp)
		test.Nil(t, err)
		if frameType != frameTypeMessage {
			i-- // heartbeat or other control frame; keep waiting
			continue
		}
		msg, _ := decodeMessage(data)
		_, _ = nsq.Finish(nsq.MessageID(msg.ID)).WriteTo(conn)
		deliveryTimes = append(deliveryTimes, time.Now())
	}

	firstAt := deliveryTimes[0]
	lastAt := deliveryTimes[len(deliveryTimes)-1]
	drainDuration := lastAt.Sub(firstAt)

	t.Logf("resume->first delivery: %s", firstAt.Sub(resumeAt))
	t.Logf("all %d messages drained over %s (%.2f msg/s avg)",
		numMessages, drainDuration, float64(numMessages)/drainDuration.Seconds())

	// report per-second delivery buckets so the rate shape is visible
	buckets := map[int64]int32{}
	for _, at := range deliveryTimes {
		bucket := int64(at.Sub(firstAt) / time.Second)
		buckets[bucket]++
	}
	for b := int64(0); b <= int64(drainDuration/time.Second); b++ {
		t.Logf("  t+%02ds: %d messages", b, buckets[b])
	}

	// The core guarantee: no burst at resume. Before the fix every message had
	// already been shifted into the ready queue during the pause and drained
	// within one read loop (drainDuration ~= 0). Here the drain must be spread
	// over a meaningful time window and capped per second instead.
	if drainDuration < pauseWindow/4 {
		t.Fatalf("drain too bursty: all messages delivered within %s of resume",
			drainDuration)
	}
	if peak := peakBucket(buckets); peak >= numMessages {
		t.Fatalf("all %d messages delivered in the same second (burst)", peak)
	}
}

func peakBucket(buckets map[int64]int32) int {
	peak := 0
	for _, n := range buckets {
		if int(n) > peak {
			peak = int(n)
		}
	}
	return peak
}
