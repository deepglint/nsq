package nsqd

import (
	"bytes"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	
	//"strings"
	"github.com/bitly/nsq/util"
	"time"
	//"github.com/bitly/go-simplejson"
	"encoding/json"
	"strings"
)

type Topic struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	messageCount uint64

	sync.RWMutex

	name              string
	channelMap        map[string]*Channel
	backend           BackendQueue
	incomingMsgChan   chan *Message
	memoryMsgChan     chan *Message
	exitChan          chan int
	channelUpdateChan chan int
	waitGroup         util.WaitGroupWrapper
	exitFlag          int32

	paused    int32
	pauseChan chan bool

	context *context
}

// Topic constructor
func NewTopic(topicName string, context *context) *Topic {
	diskQueue := newDiskQueue(topicName,
		context.nsqd.options.DataPath,
		context.nsqd.options.MaxBytesPerFile,
		context.nsqd.options.SyncEvery,
		context.nsqd.options.SyncTimeout)

	t := &Topic{
		name:              topicName,
		channelMap:        make(map[string]*Channel),
		backend:           diskQueue,
		incomingMsgChan:   make(chan *Message, 1),
		memoryMsgChan:     make(chan *Message, context.nsqd.options.MemQueueSize),
		exitChan:          make(chan int),
		channelUpdateChan: make(chan int),
		context:           context,
		pauseChan:         make(chan bool),
	}

	t.waitGroup.Wrap(func() { t.router() })
	t.waitGroup.Wrap(func() { t.messagePump() })

	go t.context.nsqd.Notify(t)

	return t
}

// Exiting returns a boolean indicating if this topic is closed/exiting
func (t *Topic) Exiting() bool {
	return atomic.LoadInt32(&t.exitFlag) == 1
}

// GetChannel performs a thread safe operation
// to return a pointer to a Channel object (potentially new)
// for the given Topic
func (t *Topic) GetChannel(channelName string) *Channel {
	t.Lock()
	channel, isNew := t.getOrCreateChannel(channelName)
	t.Unlock()

	if isNew {
		// update messagePump state
		select {
		case t.channelUpdateChan <- 1:
		case <-t.exitChan:
		}
	}

	return channel
}

// this expects the caller to handle locking
func (t *Topic) getOrCreateChannel(channelName string) (*Channel, bool) {
	channel, ok := t.channelMap[channelName]
	if !ok {
		deleteCallback := func(c *Channel) {
			t.DeleteExistingChannel(c.name)
		}
		channel = NewChannel(t.name, channelName, t.context, deleteCallback)
		t.channelMap[channelName] = channel
		log.Printf("TOPIC(%s): new channel(%s)", t.name, channel.name)
		return channel, true
	}
	return channel, false
}

func (t *Topic) GetExistingChannel(channelName string) (*Channel, error) {
	t.RLock()
	defer t.RUnlock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		return nil, errors.New("channel does not exist")
	}
	return channel, nil
}

// DeleteExistingChannel removes a channel from the topic only if it exists
func (t *Topic) DeleteExistingChannel(channelName string) error {
	t.Lock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		t.Unlock()
		return errors.New("channel does not exist")
	}
	delete(t.channelMap, channelName)
	// not defered so that we can continue while the channel async closes
	t.Unlock()

	log.Printf("TOPIC(%s): deleting channel %s", t.name, channel.name)

	// delete empties the channel before closing
	// (so that we dont leave any messages around)
	channel.Delete()

	// update messagePump state
	select {
	case t.channelUpdateChan <- 1:
	case <-t.exitChan:
	}

	return nil
}

// PutMessage writes to the appropriate incoming message channel
func (t *Topic) PutMessage(msg *Message) error {
	t.RLock()
	defer t.RUnlock()
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}
	if !t.context.nsqd.IsHealthy() {
		return errors.New("unhealthy")
	}
	t.incomingMsgChan <- msg
	atomic.AddUint64(&t.messageCount, 1)
	return nil
}

func (t *Topic) PutMessages(messages []*Message) error {
	t.RLock()
	defer t.RUnlock()
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}
	if !t.context.nsqd.IsHealthy() {
		return errors.New("unhealthy")
	}
	for _, m := range messages {
		t.incomingMsgChan <- m
		atomic.AddUint64(&t.messageCount, 1)
	}
	return nil
}

func (t *Topic) Depth() int64 {
	return int64(len(t.memoryMsgChan)) + t.backend.Depth()
}

// messagePump selects over the in-memory and backend queue and
// writes messages to every channel for this topic
func (t *Topic) messagePump() {
	var msg *Message
	var buf []byte
	var err error
	var chans []*Channel
	var memoryMsgChan chan *Message
	var backendChan chan []byte

	t.RLock()
	for _, c := range t.channelMap {
		chans = append(chans, c)
	}
	t.RUnlock()

	if len(chans) > 0 {
		memoryMsgChan = t.memoryMsgChan
		backendChan = t.backend.ReadChan()
	}

	for {
		select {
		case msg = <-memoryMsgChan:
		case buf = <-backendChan:
			msg, err = decodeMessage(buf)
			if err != nil {
				log.Printf("ERROR: failed to decode message - %s", err.Error())
				continue
			}
		case <-t.channelUpdateChan:
			chans = make([]*Channel, 0)
			t.RLock()
			for _, c := range t.channelMap {
				chans = append(chans, c)
			}
			t.RUnlock()
			if len(chans) == 0 || t.IsPaused() {
				memoryMsgChan = nil
				backendChan = nil
			} else {
				memoryMsgChan = t.memoryMsgChan
				backendChan = t.backend.ReadChan()
			}
			continue
		case pause := <-t.pauseChan:
			if pause || len(chans) == 0 {
				memoryMsgChan = nil
				backendChan = nil
			} else {
				memoryMsgChan = t.memoryMsgChan
				backendChan = t.backend.ReadChan()
			}
			continue
		case <-t.exitChan:
			goto exit
		}

		if(t.name=="Normal"){
			// log.Printf("comes the message !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!:)%s,%s",string(msg.Body),t.name)
			// var textMsg=string(msg.Body)
			// start:=strings.Index(textMsg,"StartTime")
			// datestr:=textMsg[start+12:start+41]
			// log.Printf("The Date is: %s",datestr)
			// const shortForm = "2006-08-28T18:15:57.336+08:00"
			// date,_:=time.Parse(shortForm,datestr)
			// log.Printf("********************%d",(time.Now().Unix()-date.Unix()))
			// if((time.Now().Unix()-date.Unix())>600){
			// 	log.Printf("Throw!!!")
			// 	continue
			// }
			var eventobj map[string] interface{}
			err:=json.Unmarshal(msg.Body,&eventobj)
			if err!=nil{
				log.Printf("the data is bed !")
				continue;
			}
			log.Printf("comes---%v",eventobj)
			//log.Printf("%s",eventobj["StartTime"])
			const shortForm1 = "2006-01-02T15:04:05.999ZMST"
			const shortForm2="2006-01-02T15:04:05.999-07:00"
			timestr:=eventobj["StartTime"].(string)
			timestr=strings.Trim(timestr," ")
			log.Printf("--@@--%s",timestr)
			date,err:=time.Parse(shortForm1,timestr)
			if err!=nil{
				date,_=time.Parse(shortForm2,timestr)
			}
			log.Printf("********************%d,%d,%d",time.Now().Unix(),date.Unix(),(time.Now().Unix()-date.Unix()))
			//if((time.Now()-msg.Timestamp)>60???)
			if((time.Now().Unix()-date.Unix())>60){
				log.Printf("Throw!!!")
				continue
			}
		}else{
			var eventobj map[string] interface{}
			err:=json.Unmarshal(msg.Body,&eventobj)
			if err!=nil{
				log.Printf("the data is bed !")
				continue;
			}
			log.Printf("comes---%v",eventobj)
			//log.Printf("%s",eventobj["StartTime"])
			const shortForm1 = "2006-01-02T15:04:05.999ZMST"
			const shortForm2="2006-01-02T15:04:05.999-07:00"
			timestr:=eventobj["StartTime"].(string)
			timestr=strings.Trim(timestr," ")
			log.Printf("--@@--%s",timestr)
			date,err:=time.Parse(shortForm1,timestr)
			if err!=nil{
				date,_=time.Parse(shortForm2,timestr)
			}
			log.Printf("********************%d,%d,%d",time.Now().Unix(),date.Unix(),(time.Now().Unix()-date.Unix()))
			if((time.Now().Unix()-date.Unix())>180){
				log.Printf("Throw!!!")
				continue
			}
		}
		for i, channel := range chans {
			log.Printf("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@!!!@@@@@@@@@@@@@!!!")
			chanMsg := msg
			// copy the message because each channel
			// needs a unique instance but...
			// fastpath to avoid copy if its the first channel
			// (the topic already created the first copy)
			if i > 0 {
				chanMsg = NewMessage(msg.ID, msg.Body)
				chanMsg.Timestamp = msg.Timestamp
			}
			err := channel.PutMessage(chanMsg)
			if err != nil {
				log.Printf("TOPIC(%s) ERROR: failed to put msg(%s) to channel(%s) - %s",
					t.name, msg.ID, channel.name, err)
			}
		}
	}

exit:
	log.Printf("TOPIC(%s): closing ... messagePump", t.name)
}

// router handles muxing of Topic messages including
// proxying messages to memory or backend
func (t *Topic) router() {
	var msgBuf bytes.Buffer
	for msg := range t.incomingMsgChan {
		select {
		case t.memoryMsgChan <- msg:
		default:
			err := writeMessageToBackend(&msgBuf, msg, t.backend)
			if err != nil {
				log.Printf("TOPIC(%s) ERROR: failed to write message to backend - %s",
					t.name, err)
				t.context.nsqd.SetHealth(err)
			}
		}
	}

	log.Printf("TOPIC(%s): closing ... router", t.name)
}

// Delete empties the topic and all its channels and closes
func (t *Topic) Delete() error {
	return t.exit(true)
}

// Close persists all outstanding topic data and closes all its channels
func (t *Topic) Close() error {
	return t.exit(false)
}

func (t *Topic) exit(deleted bool) error {
	if !atomic.CompareAndSwapInt32(&t.exitFlag, 0, 1) {
		return errors.New("exiting")
	}

	if deleted {
		log.Printf("TOPIC(%s): deleting", t.name)

		// since we are explicitly deleting a topic (not just at system exit time)
		// de-register this from the lookupd
		go t.context.nsqd.Notify(t)
	} else {
		log.Printf("TOPIC(%s): closing", t.name)
	}

	close(t.exitChan)

	t.Lock()
	close(t.incomingMsgChan)
	t.Unlock()

	// synchronize the close of router() and messagePump()
	t.waitGroup.Wait()

	if deleted {
		t.Lock()
		for _, channel := range t.channelMap {
			delete(t.channelMap, channel.name)
			channel.Delete()
		}
		t.Unlock()

		// empty the queue (deletes the backend files, too)
		t.Empty()
		return t.backend.Delete()
	}

	// close all the channels
	for _, channel := range t.channelMap {
		err := channel.Close()
		if err != nil {
			// we need to continue regardless of error to close all the channels
			log.Printf("ERROR: channel(%s) close - %s", channel.name, err.Error())
		}
	}

	// write anything leftover to disk
	t.flush()
	return t.backend.Close()
}

func (t *Topic) Empty() error {
	for {
		select {
		case <-t.memoryMsgChan:
		default:
			goto finish
		}
	}

finish:
	return t.backend.Empty()
}

func (t *Topic) flush() error {
	var msgBuf bytes.Buffer

	if len(t.memoryMsgChan) > 0 {
		log.Printf("TOPIC(%s): flushing %d memory messages to backend", t.name, len(t.memoryMsgChan))
	}

	for {
		select {
		case msg := <-t.memoryMsgChan:
			err := writeMessageToBackend(&msgBuf, msg, t.backend)
			if err != nil {
				log.Printf("ERROR: failed to write message to backend - %s", err.Error())
			}
		default:
			goto finish
		}
	}

finish:
	return nil
}

func (t *Topic) AggregateChannelE2eProcessingLatency() *util.Quantile {
	var latencyStream *util.Quantile
	for _, c := range t.channelMap {
		if c.e2eProcessingLatencyStream == nil {
			continue
		}
		if latencyStream == nil {
			latencyStream = util.NewQuantile(
				t.context.nsqd.options.E2EProcessingLatencyWindowTime,
				t.context.nsqd.options.E2EProcessingLatencyPercentiles)
		}
		latencyStream.Merge(c.e2eProcessingLatencyStream)
	}
	return latencyStream
}

func (t *Topic) Pause() error {
	return t.doPause(true)
}

func (t *Topic) UnPause() error {
	return t.doPause(false)
}

func (t *Topic) doPause(pause bool) error {
	if pause {
		atomic.StoreInt32(&t.paused, 1)
	} else {
		atomic.StoreInt32(&t.paused, 0)
	}

	select {
	case t.pauseChan <- pause:
		t.context.nsqd.Lock()
		defer t.context.nsqd.Unlock()
		// pro-actively persist metadata so in case of process failure
		// nsqd won't suddenly (un)pause a topic
		return t.context.nsqd.PersistMetadata()
	case <-t.exitChan:
	}

	return nil
}

func (t *Topic) IsPaused() bool {
	return atomic.LoadInt32(&t.paused) == 1
}
