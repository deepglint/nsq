package nsqd

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/bitly/go-simplejson"
	"github.com/deepglint/nsq/internal/http_api"
	"github.com/deepglint/nsq/nsqlookupd"
)

func assert(t *testing.T, condition bool, msg string, v ...interface{}) {
	if !condition {
		_, file, line, _ := runtime.Caller(1)
		t.Logf("\033[31m%s:%d: "+msg+"\033[39m\n\n",
			append([]interface{}{filepath.Base(file), line}, v...)...)
		t.FailNow()
	}
}

func equal(t *testing.T, act, exp interface{}) {
	if !reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		t.Logf("\033[31m%s:%d:\n\n\texp: %#v\n\n\tgot: %#v\033[39m\n\n",
			filepath.Base(file), line, exp, act)
		t.FailNow()
	}
}

func nequal(t *testing.T, act, exp interface{}) {
	if reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		t.Logf("\033[31m%s:%d:\n\n\texp: %#v\n\n\tgot: %#v\033[39m\n\n",
			filepath.Base(file), line, exp, act)
		t.FailNow()
	}
}

type tbLog interface {
	Log(...interface{})
}

type testLogger struct {
	tbLog
}

func (tl *testLogger) Output(maxdepth int, s string) error {
	tl.Log(s)
	return nil
}

func newTestLogger(tbl tbLog) logger {
	return &testLogger{tbl}
}

func getMetadata(n *NSQD) (*simplejson.Json, error) {
	fn := fmt.Sprintf(path.Join(n.getOpts().DataPath, "nsqd.%d.dat"), n.getOpts().ID)
	data, err := ioutil.ReadFile(fn)
	if err != nil {
		return nil, err
	}

	js, err := simplejson.NewJson(data)
	if err != nil {
		return nil, err
	}
	return js, nil
}

func TestStartup(t *testing.T) {
	iterations := 300
	doneExitChan := make(chan int)

	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts.MemQueueSize = 100
	opts.MaxBytesPerFile = 10240
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)

	origDataPath := opts.DataPath

	topicName := "nsqd_test" + strconv.Itoa(int(time.Now().Unix()))

	exitChan := make(chan int)
	go func() {
		<-exitChan
		nsqd.Exit()
		doneExitChan <- 1
	}()

	// verify nsqd metadata shows no topics
	err := nsqd.PersistMetadata()
	equal(t, err, nil)
	nsqd.setFlag(flagLoading, true)
	nsqd.GetTopic(topicName) // will not persist if `flagLoading`
	metaData, err := getMetadata(nsqd)
	equal(t, err, nil)
	topics, err := metaData.Get("topics").Array()
	equal(t, err, nil)
	equal(t, len(topics), 0)
	nsqd.DeleteExistingTopic(topicName)
	nsqd.setFlag(flagLoading, false)

	body := make([]byte, 256)
	topic := nsqd.GetTopic(topicName)
	for i := 0; i < iterations; i++ {
		msg := NewMessage(<-nsqd.idChan, body)
		topic.PutMessage(msg)
	}

	t.Logf("pulling from channel")
	channel1 := topic.GetChannel("ch1")

	t.Logf("read %d msgs", iterations/2)
	for i := 0; i < iterations/2; i++ {
		msg := <-channel1.clientMsgChan
		t.Logf("read message %d", i+1)
		equal(t, msg.Body, body)
	}

	for {
		if channel1.Depth() == int64(iterations/2) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// make sure metadata shows the topic
	metaData, err = getMetadata(nsqd)
	equal(t, err, nil)
	topics, err = metaData.Get("topics").Array()
	equal(t, err, nil)
	equal(t, len(topics), 1)
	observedTopicName, err := metaData.Get("topics").GetIndex(0).Get("name").String()
	equal(t, observedTopicName, topicName)
	equal(t, err, nil)

	exitChan <- 1
	<-doneExitChan

	// start up a new nsqd w/ the same folder

	opts = NewOptions()
	opts.Logger = newTestLogger(t)
	opts.MemQueueSize = 100
	opts.MaxBytesPerFile = 10240
	opts.DataPath = origDataPath
	_, _, nsqd = mustStartNSQD(opts)

	go func() {
		<-exitChan
		nsqd.Exit()
		doneExitChan <- 1
	}()

	topic = nsqd.GetTopic(topicName)
	// should be empty; channel should have drained everything
	count := topic.Depth()
	equal(t, count, int64(0))

	channel1 = topic.GetChannel("ch1")

	for {
		if channel1.Depth() == int64(iterations/2) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// read the other half of the messages
	for i := 0; i < iterations/2; i++ {
		msg := <-channel1.clientMsgChan
		t.Logf("read message %d", i+1)
		equal(t, msg.Body, body)
	}

	// verify we drained things
	equal(t, len(topic.memoryMsgChan), 0)
	equal(t, topic.backend.Depth(), int64(0))

	exitChan <- 1
	<-doneExitChan
}

func TestEphemeralTopicsAndChannels(t *testing.T) {
	// ephemeral topics/channels are lazily removed after the last channel/client is removed
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts.MemQueueSize = 100
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)

	topicName := "ephemeral_topic" + strconv.Itoa(int(time.Now().Unix())) + "#ephemeral"
	doneExitChan := make(chan int)

	exitChan := make(chan int)
	go func() {
		<-exitChan
		nsqd.Exit()
		doneExitChan <- 1
	}()

	body := []byte("an_ephemeral_message")
	topic := nsqd.GetTopic(topicName)
	ephemeralChannel := topic.GetChannel("ch1#ephemeral")
	client := newClientV2(0, nil, &context{nsqd})
	ephemeralChannel.AddClient(client.ID, client)

	msg := NewMessage(<-nsqd.idChan, body)
	topic.PutMessage(msg)
	msg = <-ephemeralChannel.clientMsgChan
	equal(t, msg.Body, body)

	ephemeralChannel.RemoveClient(client.ID)

	time.Sleep(100 * time.Millisecond)

	topic.Lock()
	numChannels := len(topic.channelMap)
	topic.Unlock()
	equal(t, numChannels, 0)

	nsqd.Lock()
	numTopics := len(nsqd.topicMap)
	nsqd.Unlock()
	equal(t, numTopics, 0)

	exitChan <- 1
	<-doneExitChan
}

func TestOnceTopicsAndChannels(t *testing.T) {
	// once or ephemeral topics/channels are lazily removed after the last channel/client is removed
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts.MemQueueSize = 100
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)

	topicName := "once_topic" + strconv.Itoa(int(time.Now().Unix())) + "#once"
	doneExitChan := make(chan int)

	exitChan := make(chan int)
	go func() {
		<-exitChan
		nsqd.Exit()
		doneExitChan <- 1
	}()

	body := []byte("an_once_message")
	topic := nsqd.GetTopic(topicName)
	ephemeralChannel := topic.GetChannel("ch1#once")
	client := newClientV2(0, nil, &context{nsqd})
	ephemeralChannel.AddClient(client.ID, client)

	msg := NewMessage(<-nsqd.idChan, body)
	topic.PutMessage(msg)
	msg = <-ephemeralChannel.clientMsgChan
	equal(t, msg.Body, body)

	ephemeralChannel.RemoveClient(client.ID)

	time.Sleep(100 * time.Millisecond)

	topic.Lock()
	numChannels := len(topic.channelMap)
	topic.Unlock()
	equal(t, numChannels, 0)

	nsqd.Lock()
	numTopics := len(nsqd.topicMap)
	nsqd.Unlock()
	equal(t, numTopics, 0)

	exitChan <- 1
	<-doneExitChan
}

func TestMemSizeTopicsAndChannels(t *testing.T) {
	// live or ephemeral topics/channels are lazily removed after the last channel/client is removed
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts.MemQueueSize = 100
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)

	topicName := "MemSize_topic_t" + strconv.Itoa(int(time.Now().Unix())) + "#memsize@10"
	doneExitChan := make(chan int)

	exitChan := make(chan int)
	go func() {
		<-exitChan
		nsqd.Exit()
		doneExitChan <- 1
	}()
	body := []byte("an_live_message")
	topic := nsqd.GetTopic(topicName)

	ephemeralChannel := topic.GetChannel("ch1_t#memsize@5")

	for i := 0; i < 102; i++ {
		msg := NewMessage(<-nsqd.idChan, body)
		topic.PutMessage(msg)
	}

	equal(t, len(topic.memoryMsgChan), 10)
	equal(t, len(ephemeralChannel.memoryMsgChan), 5)

	exitChan <- 1
	<-doneExitChan
}

func TestCircleTopicsAndChannels(t *testing.T) {
	// ephemeral topics/channels are lazily removed after the last channel/client is removed
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts.MemQueueSize = 10
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)

	topicName := "ephemeral_topic" + strconv.Itoa(int(time.Now().Unix())) + "#nodisk#circle"
	doneExitChan := make(chan int)

	exitChan := make(chan int)
	go func() {
		<-exitChan
		nsqd.Exit()
		doneExitChan <- 1
	}()

	topic := nsqd.GetTopic(topicName)
	ephemeralChannel := topic.GetChannel("ch1#nodisk#circle#memesize@2")
	client := newClientV2(0, nil, &context{nsqd})
	ephemeralChannel.AddClient(client.ID, client)

	for i := 1; i <= 20; i++ {
		body := []byte("an_ephemeral_message" + strconv.Itoa(i))
		msg := NewMessage(<-nsqd.idChan, body)
		topic.PutMessage(msg)
	}
	body0 := []byte("an_ephemeral_message" + strconv.Itoa(11))
	msg := NewMessage(<-nsqd.idChan, body0)
	msg = <-ephemeralChannel.clientMsgChan
	equal(t, msg.Body, body0)

	body1 := []byte("an_ephemeral_message" + strconv.Itoa(12))
	msg = <-ephemeralChannel.clientMsgChan
	equal(t, msg.Body, body1)

	body2 := []byte("an_ephemeral_message" + strconv.Itoa(13))
	msg = <-ephemeralChannel.clientMsgChan
	equal(t, msg.Body, body2)

	body3 := []byte("an_ephemeral_message" + strconv.Itoa(14))
	msg = <-ephemeralChannel.clientMsgChan
	equal(t, msg.Body, body3)

	ephemeralChannel.RemoveClient(client.ID)

	exitChan <- 1
	<-doneExitChan
}

func TestTtlTopics(t *testing.T) {
	// ephemeral topics/channels are lazily removed after the last channel/client is removed
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts.MemQueueSize = 10
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)

	topicName := "ephemeral_topic" + strconv.Itoa(int(time.Now().Unix())) + "#ttl@500"
	doneExitChan := make(chan int)

	exitChan := make(chan int)
	go func() {
		<-exitChan
		nsqd.Exit()
		doneExitChan <- 1
	}()

	topic := nsqd.GetTopic(topicName)

	body1 := []byte("an_ephemeral_message" + strconv.Itoa(1))
	msg1 := NewMessage(<-nsqd.idChan, body1)
	topic.PutMessage(msg1)

	time.Sleep(600 * time.Millisecond)

	body2 := []byte("an_ephemeral_message" + strconv.Itoa(2))
	msg2 := NewMessage(<-nsqd.idChan, body2)
	topic.PutMessage(msg2)

	ephemeralChannel := topic.GetChannel("ch1_ttl")
	client := newClientV2(0, nil, &context{nsqd})
	ephemeralChannel.AddClient(client.ID, client)

	body11 := []byte("an_ephemeral_message" + strconv.Itoa(2))
	msg11 := NewMessage(<-nsqd.idChan, body11)
	msg11 = <-ephemeralChannel.clientMsgChan
	equal(t, msg11.Body, body11)

	ephemeralChannel.RemoveClient(client.ID)

	exitChan <- 1
	<-doneExitChan
}

func TestTtlChannel(t *testing.T) {
	// ephemeral topics/channels are lazily removed after the last channel/client is removed
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts.MemQueueSize = 10
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)

	topicName := "ephemeral_topic_c" + strconv.Itoa(int(time.Now().Unix()))
	doneExitChan := make(chan int)

	exitChan := make(chan int)
	go func() {
		<-exitChan
		nsqd.Exit()
		doneExitChan <- 1
	}()

	topic := nsqd.GetTopic(topicName)

	body1 := []byte("an_ephemeral_message" + strconv.Itoa(1))
	msg1 := NewMessage(<-nsqd.idChan, body1)
	topic.PutMessage(msg1)

	body2 := []byte("an_ephemeral_message" + strconv.Itoa(2))
	msg2 := NewMessage(<-nsqd.idChan, body2)
	topic.PutMessage(msg2)

	ephemeralChannel := topic.GetChannel("ch1_c_ttl" + "#ttl@500")
	client := newClientV2(0, nil, &context{nsqd})
	ephemeralChannel.AddClient(client.ID, client)

	time.Sleep(600 * time.Millisecond)
	body3 := []byte("an_ephemeral_message" + strconv.Itoa(3))
	msg3 := NewMessage(<-nsqd.idChan, body3)
	topic.PutMessage(msg3)

	<-ephemeralChannel.clientMsgChan

	body11 := []byte("an_ephemeral_message" + strconv.Itoa(3))
	msg11 := NewMessage(<-nsqd.idChan, body11)
	msg11 = <-ephemeralChannel.clientMsgChan
	equal(t, msg11.Body, body11)

	ephemeralChannel.RemoveClient(client.ID)

	exitChan <- 1
	<-doneExitChan
}

func metadataForChannel(n *NSQD, topicIndex int, channelIndex int) *simplejson.Json {
	metadata, _ := getMetadata(n)
	mChannels := metadata.Get("topics").GetIndex(topicIndex).Get("channels")
	return mChannels.GetIndex(channelIndex)
}

func TestPauseMetadata(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	// avoid concurrency issue of async PersistMetadata() calls
	nsqd.setFlag(flagLoading, true)
	topicName := "pause_metadata" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("ch")
	nsqd.setFlag(flagLoading, false)
	nsqd.PersistMetadata()

	b, _ := metadataForChannel(nsqd, 0, 0).Get("paused").Bool()
	equal(t, b, false)

	channel.Pause()
	b, _ = metadataForChannel(nsqd, 0, 0).Get("paused").Bool()
	equal(t, b, false)

	nsqd.PersistMetadata()
	b, _ = metadataForChannel(nsqd, 0, 0).Get("paused").Bool()
	equal(t, b, true)

	channel.UnPause()
	b, _ = metadataForChannel(nsqd, 0, 0).Get("paused").Bool()
	equal(t, b, true)

	nsqd.PersistMetadata()
	b, _ = metadataForChannel(nsqd, 0, 0).Get("paused").Bool()
	equal(t, b, false)
}

func mustStartNSQLookupd(opts *nsqlookupd.Options) (*net.TCPAddr, *net.TCPAddr, *nsqlookupd.NSQLookupd) {
	opts.TCPAddress = "127.0.0.1:0"
	opts.HTTPAddress = "127.0.0.1:0"
	lookupd := nsqlookupd.New(opts)
	lookupd.Main()
	return lookupd.RealTCPAddr(), lookupd.RealHTTPAddr(), lookupd
}

func TestReconfigure(t *testing.T) {
	lopts := nsqlookupd.NewOptions()
	lopts.Logger = newTestLogger(t)
	_, _, lookupd1 := mustStartNSQLookupd(lopts)
	defer lookupd1.Exit()
	_, _, lookupd2 := mustStartNSQLookupd(lopts)
	defer lookupd2.Exit()
	_, _, lookupd3 := mustStartNSQLookupd(lopts)
	defer lookupd3.Exit()

	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	time.Sleep(50 * time.Millisecond)

	newOpts := *opts
	newOpts.NSQLookupdTCPAddresses = []string{lookupd1.RealTCPAddr().String()}
	nsqd.swapOpts(&newOpts)
	nsqd.triggerOptsNotification()
	equal(t, len(nsqd.getOpts().NSQLookupdTCPAddresses), 1)

	time.Sleep(50 * time.Millisecond)

	numLookupPeers := len(nsqd.lookupPeers.Load().([]*lookupPeer))
	equal(t, numLookupPeers, 1)

	newOpts = *opts
	newOpts.NSQLookupdTCPAddresses = []string{lookupd2.RealTCPAddr().String(), lookupd3.RealTCPAddr().String()}
	nsqd.swapOpts(&newOpts)
	nsqd.triggerOptsNotification()
	equal(t, len(nsqd.getOpts().NSQLookupdTCPAddresses), 2)

	time.Sleep(50 * time.Millisecond)

	var lookupPeers []string
	for _, lp := range nsqd.lookupPeers.Load().([]*lookupPeer) {
		lookupPeers = append(lookupPeers, lp.addr)
	}
	equal(t, len(lookupPeers), 2)
	equal(t, lookupPeers, newOpts.NSQLookupdTCPAddresses)
}

func TestCluster(t *testing.T) {
	lopts := nsqlookupd.NewOptions()
	lopts.Logger = newTestLogger(t)
	lopts.BroadcastAddress = "127.0.0.1"
	_, _, lookupd := mustStartNSQLookupd(lopts)

	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts.NSQLookupdTCPAddresses = []string{lookupd.RealTCPAddr().String()}
	opts.BroadcastAddress = "127.0.0.1"
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "cluster_test" + strconv.Itoa(int(time.Now().Unix()))

	hostname, err := os.Hostname()
	equal(t, err, nil)

	url := fmt.Sprintf("http://%s/topic/create?topic=%s", nsqd.RealHTTPAddr(), topicName)
	_, err = http_api.NegotiateV1("POST", url, nil)
	equal(t, err, nil)

	url = fmt.Sprintf("http://%s/channel/create?topic=%s&channel=ch", nsqd.RealHTTPAddr(), topicName)
	_, err = http_api.NegotiateV1("POST", url, nil)
	equal(t, err, nil)

	// allow some time for nsqd to push info to nsqlookupd
	time.Sleep(350 * time.Millisecond)

	data, err := http_api.NegotiateV1("GET", fmt.Sprintf("http://%s/debug", lookupd.RealHTTPAddr()), nil)
	equal(t, err, nil)

	topicData := data.Get("topic:" + topicName + ":")
	producers, _ := topicData.Array()
	equal(t, len(producers), 1)

	producer := topicData.GetIndex(0)
	equal(t, producer.Get("hostname").MustString(), hostname)
	equal(t, producer.Get("broadcast_address").MustString(), "127.0.0.1")
	equal(t, producer.Get("tcp_port").MustInt(), nsqd.RealTCPAddr().Port)
	equal(t, producer.Get("tombstoned").MustBool(), false)

	channelData := data.Get("channel:" + topicName + ":ch")
	producers, _ = channelData.Array()
	equal(t, len(producers), 1)

	producer = topicData.GetIndex(0)
	equal(t, producer.Get("hostname").MustString(), hostname)
	equal(t, producer.Get("broadcast_address").MustString(), "127.0.0.1")
	equal(t, producer.Get("tcp_port").MustInt(), nsqd.RealTCPAddr().Port)
	equal(t, producer.Get("tombstoned").MustBool(), false)

	data, err = http_api.NegotiateV1("GET", fmt.Sprintf("http://%s/lookup?topic=%s", lookupd.RealHTTPAddr(), topicName), nil)
	equal(t, err, nil)

	producers, _ = data.Get("producers").Array()
	equal(t, len(producers), 1)

	producer = data.Get("producers").GetIndex(0)
	equal(t, producer.Get("hostname").MustString(), hostname)
	equal(t, producer.Get("broadcast_address").MustString(), "127.0.0.1")
	equal(t, producer.Get("tcp_port").MustInt(), nsqd.RealTCPAddr().Port)

	channels, _ := data.Get("channels").Array()
	equal(t, len(channels), 1)

	channel := channels[0].(string)
	equal(t, channel, "ch")

	data, err = http_api.NegotiateV1("POST", fmt.Sprintf("http://%s/topic/delete?topic=%s", nsqd.RealHTTPAddr(), topicName), nil)
	equal(t, err, nil)

	// allow some time for nsqd to push info to nsqlookupd
	time.Sleep(350 * time.Millisecond)

	data, err = http_api.NegotiateV1("GET", fmt.Sprintf("http://%s/lookup?topic=%s", lookupd.RealHTTPAddr(), topicName), nil)
	equal(t, err, nil)

	producers, _ = data.Get("producers").Array()
	equal(t, len(producers), 0)

	data, err = http_api.NegotiateV1("GET", fmt.Sprintf("http://%s/debug", lookupd.RealHTTPAddr()), nil)
	equal(t, err, nil)

	producers, _ = data.Get("topic:" + topicName + ":").Array()
	equal(t, len(producers), 0)

	producers, _ = data.Get("channel:" + topicName + ":ch").Array()
	equal(t, len(producers), 0)
}
