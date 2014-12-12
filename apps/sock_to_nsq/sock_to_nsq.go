package main

import (
	"log"
	"net"
	"os"
	// "runtime/pprof"
	"flag"
	"fmt"
	"github.com/bitly/go-nsq"
	"os/signal"
	"syscall"
)

var (
	topic    = flag.String("topic", "test", "nsq topic")
	nsq_addr = flag.String("address", "localhost:4150", "nsq address")
)

//var nsq_addr = "localhost:4150"
//var topic = "testsocket"
var pCfg = nsq.NewConfig()
var err error
var producer *nsq.Producer

func echoServer(c net.Conn) {
	for {
		//Set the buffer for 50k
		buf := make([]byte, 50000)
		nr, err := c.Read(buf)
		if err != nil {
			return
		}
		println(nr)
		data := buf[0:nr]
		println("Server got:", string(data))
		// _, err = c.Write(data)
		// if err != nil {
		//     log.Fatal("Write: ", err)
		// }

		producer.Publish(*topic, data)
	}
}

// func init() {
// 	flag.Var(&nsq_addr, "nsqd-tcp-address", "(deprecated) use --consumer-opt")
// 	flag.Var(&topic, "topic", "option to passthrough to nsq.Consumer (may be given multiple times, see http://godoc.org/github.com/bitly/go-nsq#Config)")
// 	//flag.Var(&producerOpts, "producer-opt", "option to passthrough to nsq.Producer (may be given multiple times, see http://godoc.org/github.com/bitly/go-nsq#Config)")

// }

func main() {
	flag.Parse()
	producer, err = nsq.NewProducer(*nsq_addr, pCfg)
	if err != nil {
		log.Fatalf("failed creating producer %s", err)
	}

	l, err := net.Listen("unix", "/tmp/nsqtest2.sock")
	if err != nil {
		log.Fatal("listen error:", err)
	}
	cc := make(chan os.Signal, 1)
	signal.Notify(cc, os.Interrupt)
	signal.Notify(cc, syscall.SIGTERM)
	go func() {
		<-cc
		cleanup()
		l.Close()
		os.Exit(1)
	}()

	for {
		fd, err := l.Accept()
		if err != nil {
			log.Fatal("accept error:", err)
		}

		go echoServer(fd)
	}

}

func cleanup() {
	fmt.Println("Clean up the socket file")
}
