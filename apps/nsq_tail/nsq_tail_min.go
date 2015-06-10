package main

import (
	"fmt"
	"github.com/deepglint/go-nsq"
	"os"
	"os/signal"
	"syscall"
	"time"
	//"github.com/bitly/nsq/util"
)

func main() {
	fmt.Println("hello world")

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

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	for {
		err = consumer.ConnectToNSQD("localhost:4150")
		if err != nil {
			fmt.Println("Error connecting,reconnecting in 15s")
			time.Sleep(time.Second * 15)
			continue
		} else {
			break
		}
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
