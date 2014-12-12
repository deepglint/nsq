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
	topic      = flag.String("topic", "test", "nsq topic")
	nsq_addr   = flag.String("address", "localhost:4150", "nsq address")
	sockname   = flag.String("sockname", "/tmp/nsqtest2.sock", "unix socket")
	buffersize = flag.Int("size", 1000000, "socket buffer size")
)

//var nsq_addr = "localhost:4150"
//var topic = "testsocket"
var pCfg = nsq.NewConfig()
var err error
var producer *nsq.Producer

func echoServer(c net.Conn) {
	for {
		//Set the buffer for 50k
		buf := make([]byte, *buffersize)
		nr, err := c.Read(buf)
		if err != nil {
			return
		}
		println(nr)
		data := buf[0:nr]
		println("Server got a new message with size %d", &nr)

		producer.Publish(*topic, data)
	}
}

func main() {
	flag.Parse()
	producer, err = nsq.NewProducer(*nsq_addr, pCfg)
	if err != nil {
		log.Fatalf("failed creating producer %s", err)
	}

	l, err := net.Listen("unix", *sockname)
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
