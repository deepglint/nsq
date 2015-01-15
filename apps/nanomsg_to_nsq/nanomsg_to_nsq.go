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
	nanomsgDomain = flag.String("domain", "", "nanomsg domain")
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
	body []byte
}

var memBuffer chan MemMsg

func main() {
	flag.Parse()
	memBuffer = make(chan MemMsg)
	if *nanomsgDomain == "" && *nsqdaddr == "" && *topic == "" {
		log.Println("the params need not to be null")
		return
	}
	socket, err = nanomsg.NewPullSocket()
	if err != nil {
		fmt.Println(err)
		return
	}
	socket.Bind(*nanomsgDomain)
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
	go nanoReceiver()
	go sender()
	go tester()
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
		memBuffer <- *newMsg
		println("push into membuffer")
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
		err := producer.Publish(*topic, msg.body)
		if err != nil {
			log.Println("Need to send local nsq here or save on disc")
			innerProducer.Publish("uploaderBackup", msg.body)
			blockedNum++
			continue
		}
		blockedNum = 0
	}
}
