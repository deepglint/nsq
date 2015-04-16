package main

import (
	"flag"
	"fmt"
	"github.com/deepglint/go-nsq"
	"github.com/op/go-nanomsg"
	"log"
	//"os"
)

var (
	nanomsgDomain = flag.String("domain", "", "nanomsg domain")
	nsqdaddr      = flag.String("nsqdaddr", "", "nsqd tcp address")
	topic         = flag.String("topic", "", "topic of nsqd to push msg")
)
var pCfg = nsq.NewConfig()
var producer *nsq.Producer

func main() {
	flag.Parse()
	if *nanomsgDomain == "" && *busaddr == "" && *topic == "" {
		log.Println("the params need not to be null")
		return
	}
	socket, err := nanomsg.NewPullSocket()
	if err != nil {
		fmt.Println(err)
		return
	}
	socket.Bind(*nanomsgDomain)
	producer, err = bus.NewProducer(*busaddr, pCfg)
	if err != nil {
		log.Fatalf("failed creating producer %s", err)
	}
	for {
		tmpbuf, err2 := socket.Recv(0)
		if err2 != nil {
			fmt.Println("error comes")
			return
		}
		producer.Publish(*topic, tmpbuf)
	}
}
