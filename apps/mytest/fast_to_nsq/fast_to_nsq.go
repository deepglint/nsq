// This is an NSQ client that publishes incoming messages from
// stdin to the specified topic.

package main

import (
	"strconv"
	"time"
	//	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/deepglint/go-nsq"
	"github.com/deepglint/nsq/internal/app"
	"github.com/deepglint/nsq/internal/version"
)

var (
	topic     = flag.String("topic", "fast", "NSQ topic to publish to")
	delimiter = flag.String("delimiter", "\n", "character to split input from stdin (defaults to '\n')")

	destNsqdTCPAddrs = app.StringArray{}
	producerOpts     = app.StringArray{}
)

func init() {
	flag.Var(&producerOpts, "producer-opt", "option to passthrough to nsq.Producer (may be given multiple times, http://godoc.org/github.com/deepglint/go-nsq#Config)")
	flag.Var(&destNsqdTCPAddrs, "nsqd-tcp-address", "destination nsqd TCP address (may be given multiple times)")
}

func main() {
	flag.Parse()

	if len(*topic) == 0 {
		log.Fatal("--topic required")
	}

	if len(*delimiter) != 1 {
		log.Fatal("--delimiter must be a single byte")
	}

	stopChan := make(chan bool)
	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	cfg := nsq.NewConfig()
	cfg.UserAgent = fmt.Sprintf("to_nsq/%s go-nsq/%s", version.Binary, nsq.VERSION)

	err := app.ParseOpts(cfg, producerOpts)
	if err != nil {
		log.Fatal(err)
	}

	// make the producers
	producers := make(map[string]*nsq.Producer)
	for _, addr := range destNsqdTCPAddrs {
		producer, err := nsq.NewProducer(addr, cfg)
		if err != nil {
			log.Fatalf("failed to create nsq.Producer - %s", err)
		}
		producers[addr] = producer
	}

	if len(producers) == 0 {
		log.Fatal("--nsqd-tcp-address required")
	}

	//r := bufio.NewReader(os.Stdin)
	//delim := (*delimiter)[0]
	go func() {
		count := 1
		for {
			time.Sleep(time.Millisecond * 300)
			count = count + 1
			err := readAndPublish(count, producers)
			if err != nil {
				if err != io.EOF {
					log.Fatal(err)
				}
				close(stopChan)
				break
			}
		}
	}()

	select {
	case <-termChan:
	case <-stopChan:
	}

	for _, producer := range producers {
		producer.Stop()
	}
}

// readAndPublish reads to the delim from r and publishes the bytes
// to the map of producers.
func readAndPublish(count int, producers map[string]*nsq.Producer) error {
	//line, readErr := r.ReadBytes(delim)
	line := []byte("this is a msg" + strconv.Itoa(count))
	//if len(line) > 0 {
	//	// trim the delimiter
	//	line = line[:len(line)-1]
	//}

	//if len(line) == 0 {
	//	return readErr
	//}

	for _, producer := range producers {
		err := producer.Publish(*topic, line)
		if err != nil {
			return err
		}
	}

	return nil
}
