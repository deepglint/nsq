package main

import (
	"flag"
	"fmt"
	"github.com/deepglint/go-nsq"
	"github.com/op/go-nanomsg"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	topic         = flag.String("topic", "events", "topic of nsqd to push msg")
	innernsqdaddr = flag.String("innernsqdaddr", "localhost:4150", "the inner nsqd address for message backup")
)
var pCfg = nsq.NewConfig()
var innerConsumer *nsq.Consumer

var innerProducer *nsq.Producer

var err error

var demoEvent = "{\"AlarmLevel\":0,\"EventType\":223,\"EventTypeProbability\":0.0,\"HotspotId\":\"DG.BLADE.H1\",\"Path\":[-372,7035,1497,-372,7035,1497,-372,7035,1497,-372,7035,1497,-372,7035,1497,-372,7035,1497,-372,7035,1497,-372,7035,1497,-372,7035,1497,-372,7035,1497,-372,7035,1497,-372,7035,1497,-372,7035,1497,-372,7035,1497,-372,7035,1497,-372,7035,1497],\"PeopleId\":\"e8118ad24d2e4828923dfc29099ad0a4\",\"PicBinary\":\"../io/tmp_event/266450724971_1600.jpg\",\"PlanetId\":\"DG\",\"SceneId\":\"DG.BLADE\",\"SensorId\":\"DG.BLADE.S12\",\"StartTime\":1421295756851,\"TimeLength\":533}"

type MemMsg struct {
	body      []byte
	timestamp int64
}

type IpcSource struct {
	addr   string
	socket *nanomsg.PullSocket
}

func (this *IpcSource) NanoReceiver() {
	for {
		tmpbuf, err2 := this.socket.Recv(0)
		if err2 != nil {
			fmt.Println("error comes")
			return
		}
		newMsg := new(MemMsg)
		newMsg.body = tmpbuf
		newMsg.timestamp = time.Now().UnixNano()
		memBuffer <- *newMsg
		println("push into membuffer from socket:%s", this.addr)
		//log.Println("comes the message :\n%s\nwith time :%d", string(tmpbuf), newMsg.timestamp)
	}
}

func NewIPCSource(addr string) (*IpcSource, error) {
	var ipc = new(IpcSource)
	ipc.addr = addr
	ipc.socket, err = nanomsg.NewPullSocket()
	if err != nil {
		log.Println(err)
		return nil, err
	}
	ipc.socket.Bind(addr)
	log.Println("Finishing initializing ipc source:%s", addr)
	return ipc, nil
}

var memBuffer chan MemMsg

func main() {
	flag.Parse()
	memBuffer = make(chan MemMsg)

	var ipc1, ipc2, ipc3, ipc4, ipc5, ipc6, ipc7, ipc8 *IpcSource
	ipc1, err = NewIPCSource("ipc:///tmp/libra_single_money.ipc")
	if err != nil {
		log.Println("Fail to init IPC")
	}
	ipc2, err = NewIPCSource("ipc:///tmp/libra_cutboard.ipc")
	if err != nil {
		log.Println("Fail to init IPC")
	}
	ipc3, err = NewIPCSource("ipc:///tmp/libra_fall.ipc")
	if err != nil {
		log.Println("Fail to init IPC")
	}
	ipc4, err = NewIPCSource("ipc:///tmp/libra_violence.ipc")
	if err != nil {
		log.Println("Fail to init IPC")
	}
	ipc5, err = NewIPCSource("ipc:///tmp/libra_latch.ipc")
	if err != nil {
		log.Println("Fail to init IPC")
	}
	ipc6, err = NewIPCSource("ipc:///tmp/libra_lens_protection.ipc")
	if err != nil {
		log.Println("Fail to init IPC")
	}
	ipc7, err = NewIPCSource("ipc:///tmp/libra_sos.ipc")
	if err != nil {
		log.Println("Fail to init IPC")
	}
	ipc8, err = NewIPCSource("ipc:///tmp/libra_imu.ipc")
	if err != nil {
		log.Println("Fail to init IPC")
	}

	innerProducer, err = nsq.NewProducer(*innernsqdaddr, pCfg)
	if err != nil {
		log.Fatalf("failed creating inner producer %s", err)
	}

	if err != nil {
		log.Println("Error connecting to local nsq")
		//return err
	}
	go ipc1.NanoReceiver()
	go ipc2.NanoReceiver()
	go ipc3.NanoReceiver()
	go ipc4.NanoReceiver()
	go ipc5.NanoReceiver()
	go ipc6.NanoReceiver()
	go ipc7.NanoReceiver()
	go ipc8.NanoReceiver()
	go sender()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	for {
		select {
		case <-sigChan:
			log.Println("exiting")
			return
		}
	}
}

func sender() {
	for {
		msg := <-memBuffer
		t := time.Now().UnixNano()

		err := innerProducer.Publish(*topic, msg.body)
		if err != nil {
			log.Println("Error for publishing")

		}

		log.Println("the latency of msg is :%d\n", t-msg.timestamp)
	}
}
