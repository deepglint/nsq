package main

import (
	"flag"
	"fmt"
	"github.com/bitly/go-nsq"
	"os"
	"os/signal"
	"syscall"
	//"github.com/bitly/nsq/util"
)

func main() {

	fmt.Println("hello world")

	cfg := nsq.NewConfig()

	consumer, err := nsq.NewConsumer("", "", cfg)

	if err != nil {
		fmt.Println("Error")
	}

	consumer.AddHandler(nsq.HandlerFunc(func(m *nsq.Message) error {
		// handle the message
		fmt.Println("%v", m)
		return nil
	}))

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	err = consumer.ConnectToNSQD("")
	if err != nil {
		fmt.Println("Error connecting")
	}

	for {
		select {
		case <-consumer.StopChan:
			return
		case <-sigChan:
			consumer.Stop()
		}
	}
}
