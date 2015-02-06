package main

import (
	"encoding/json"
	"flag"
	"github.com/deepglint/glog"
	"github.com/deepglint/go-nsq"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

type TimeSpan struct {
	From  string
	Count string
}

type SliceModel struct {
	FID       string
	SensorId  string
	StartTime string
}

var (
	sliceServer    = flag.String("sliceserver", "", "slice server http address")
	sensorId       = flag.String("sensorid", "default", "sensorId")
	requestTopic   = flag.String("reqtopic", "slice", "request topic ")
	requestChannel = flag.String("reqchan", "", "request channel,use #ephemeral for instance")
	responseTopic  = flag.String("resptopic", "sliceres", "the response topic")
	nsqdaddr       = flag.String("nsqdaddr", "", "the response nsqd address")
)

var spanchan chan nsq.Message
var producer *nsq.Producer
var table map[string]bool
var publisher *nsq.Producer
var err error

func main() {
	flag.Parse()
	spanchan = make(chan nsq.Message)
	table = make(map[string]bool)
	cfg := nsq.NewConfig()
	producer, err = nsq.NewProducer(*nsqdaddr, cfg)
	consumer, err3 := nsq.NewConsumer(*requestTopic, *requestChannel, cfg)

	if err3 != nil {
		glog.Errorln("Error")
	}

	consumer.AddHandler(nsq.HandlerFunc(HandleMsg))
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	err = consumer.ConnectToNSQD(*nsqdaddr)
	if err != nil {
		glog.Errorln("Error connecting")
	}
	go SliceWorker()
	for {
		select {
		case <-consumer.StopChan:
			return
		case <-sigChan:
			consumer.Stop()
		}
	}
}

func HandleMsg(m *nsq.Message) error {
	log.Println("Comes the slice request command")

	spanchan <- *m

	return nil
}

func SliceWorker() {
	for {
		m := <-spanchan
		log.Println(string(m.Body))
		//span := new(TimeSpan)
		span := make(map[string]int)
		json.Unmarshal(m.Body, &span)
		log.Println("ll ll %v", span)
		log.Println("%s", strconv.Itoa(span["From"]))
		var url = *sliceServer + "/api/slices/AfterTime/" + *sensorId + "/" + strconv.Itoa(span["From"]) + "/" + strconv.Itoa(span["Count"])

		log.Println(url)
		res, err := http.Get(url)
		if err != nil {
			glog.Errorln("Error to get from the slice server")
			continue
		}
		defer res.Body.Close()
		body, err := ioutil.ReadAll(res.Body)
		var slices = make([]SliceModel, 0)

		//glog.Infoln(string(body))
		log.Println(string(body))
		log.Println("%v", span)
		json.Unmarshal(body, &slices)
		log.Println("%d", len(slices))
		//log.Println(slices[0].FID)
		log.Println("%v", table)
		for _, content := range slices {
			log.Println(content)
			//if true means hte slice has not sent
			if table[content.FID] != false {
				log.Println("Already got the slice in central server")
				continue
			}
			var sliceUrl = *sliceServer + "/api/slices/fid/" + content.FID
			sliceres, err2 := http.Get(sliceUrl)
			if err2 != nil {
				glog.Errorln("Error to get slice file")
			}
			slicebody, err2 := ioutil.ReadAll(sliceres.Body)
			log.Println("sending body")
			table[content.FID] = true
			producer.Publish(*responseTopic, slicebody)
		}

	}
}
