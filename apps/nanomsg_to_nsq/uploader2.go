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
	nanomsgDomain = flag.String("domain", "ipc:///tmp/default.ipc", "nanomsg domain")
	nsqdaddr      = flag.String("nsqdaddr", "", "nsqd tcp address")
	topic         = flag.String("topic", "", "topic of nsqd to push msg")

	innernsqdaddr = flag.String("innernsqdaddr", "localhost:4150", "the inner nsqd address for message backup")
)
var pCfg = nsq.NewConfig()
var innerConsumer *nsq.Consumer
var producer *nsq.Producer
var producer2 *nsq.Producer
var innerProducer *nsq.Producer
var socket *nanomsg.PullSocket
var err error
var backupLink *nsq.Consumer
var blockedNum = 0
var stoped = true

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
	if *nanomsgDomain == "" && *nsqdaddr == "" && *topic == "" {
		log.Println("the params need not to be null")
		return
	}
	///////
	// socket, err = nanomsg.NewPullSocket()
	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }
	// socket.Bind(*nanomsgDomain)

	///////
	var ipc1, ipc2, ipc3, ipc4, ipc5, ipc6, ipc7 *IpcSource
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
	producer, err = nsq.NewProducer(*nsqdaddr, pCfg)
	if err != nil {
		log.Fatalf("failed creating remote producer %s", err)
	}
	producer2, err = nsq.NewProducer(*nsqdaddr, pCfg)
	if err != nil {
		log.Fatalf("failed creating remote producer %s", err)
	}
	innerProducer, err = nsq.NewProducer(*innernsqdaddr, pCfg)
	if err != nil {
		log.Fatalf("failed creating inner producer %s", err)
	}
	innerConsumer, err = nsq.NewConsumer("uploaderBackup", "backup#ephemeral", pCfg)
	if err != nil {
		log.Println("Error")
		//return err
	}
	innerConsumer.AddHandler(nsq.HandlerFunc(nsqBackupWorker))
	err = innerConsumer.ConnectToNSQD(*innernsqdaddr)
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
	go sender()
	//go tester()
	go switcher()
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

func nanoReceiver() {
	for {
		tmpbuf, err2 := socket.Recv(0)
		if err2 != nil {
			fmt.Println("error comes")
			return
		}
		newMsg := new(MemMsg)
		newMsg.body = tmpbuf
		newMsg.timestamp = time.Now().UnixNano()
		memBuffer <- *newMsg
		println("push into membuffer")
		log.Println("comes the message :\n%s\nwith time :%d", string(tmpbuf), newMsg.timestamp)
	}
}

func tester() {
	for {
		newMsg := new(MemMsg)
		newMsg.body = []byte(demoEvent)
		memBuffer <- *newMsg
		println("push into membuffer")
		time.Sleep(time.Second * 1)
	}
}

func nsqBackupWorker(m *nsq.Message) error {
	err := producer2.Publish(*topic, m.Body)
	if err != nil {
		log.Println("Need to send local nsq here or save on disc,and resend in 15s")
		//innerProducer.Publish("uploaderBackup", msg.body)
		m.Requeue(time.Second * 15)

	}
	return nil
}

func switcher() {
	for {
		if blockedNum > 0 {
			innerConsumer.ChangeMaxInFlight(0)
			stoped = true
		} else {
			if stoped == true {
				stoped = false
				innerConsumer.ChangeMaxInFlight(50)
			}
		}
		time.Sleep(time.Second * 5)
	}
}

func sender() {
	for {
		msg := <-memBuffer
		t := time.Now().UnixNano()

		err := producer.Publish(*topic, msg.body)
		if err != nil {
			log.Println("Need to send local nsq here or save on disc")
			innerProducer.Publish("uploaderBackup", msg.body)
			blockedNum++
			continue
		}
		blockedNum = 0
		log.Println("the latency of msg is :%d\n", t-msg.timestamp)
	}
}
