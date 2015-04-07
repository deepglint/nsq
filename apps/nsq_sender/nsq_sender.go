package main

import (
	"flag"
	"github.com/deepglint/go-nsq"
	"log"
	"time"
)

var (
	//nanomsgDomain = flag.String("domain", "", "nanomsg domain")
	nsqdaddr = flag.String("nsqdaddr", "192.168.2.100:4150", "nsqd tcp address")
	topic    = flag.String("topic", "vibo_events", "topic of nsqd to push msg")
)

var pCfg = nsq.NewConfig()
var producer *nsq.Producer
var tmpbuf = "{\"AlarmLevel\":0,\"EventType\":612,\"EventTypeProbability\":0,\"HotspotId\":\"\",\"Id\":\"54e02a428f1c2291e00a844c\",\"PeopleId\":\"\",\"PlanetId\":\"MF\",\"SceneId\":\"D1\",\"SensorId\":\"DG.ONI.S04\",\"StartTime\":1423093795283,\"TimeLength\":1000}"
var err error

func main() {
	flag.Parse()
	producer, err = nsq.NewProducer(*nsqdaddr, pCfg)
	if err != nil {
		log.Fatalf("failed creating producer %s", err)
	}
	for {
		producer.Publish(*topic, []byte(tmpbuf))
		time.Sleep(200 * 1000 * 1000)
	}
}
