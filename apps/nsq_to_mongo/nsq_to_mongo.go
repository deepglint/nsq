package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/bitly/go-nsq"
	//"github.com/bitly/nsq/util"
)

func main() {
	fmt.Println("hello world")
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	cfg := nsq.NewConfig()

	consumer, err := nsq.NewConsumer("hello", "world", cfg)

	if err != nil {
		fmt.Println("Error")
	}

	consumer.AddHandler(nsq.HandlerFunc(func(m *nsq.Message) error {
		// handle the message
		fmt.Println("%v", m)
		return nil
	}))

	err = consumer.ConnectToNSQD("localhost:4150")
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
