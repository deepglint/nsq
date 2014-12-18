package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/deepglint/glog"
	"github.com/deepglint/go-nsq"
	"labix.org/v2/mgo"
	//"labix.org/v2/mgo/bson"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
	//"github.com/bitly/nsq/util"
)

var (
	topic      = flag.String("topic", "", "nsq topic")
	channel    = flag.String("channel", "", "nsq channel to subscribe")
	nsq_addr   = flag.String("address", "localhost:4150", "nsq address")
	database   = flag.String("db", "", "data base name to insert data")
	collection = flag.String("collection", "", "collection of mongo db")
	mongo_addr = flag.String("mongoadd", "", "address of mongo db")
	user       = flag.String("user", "", "user name for mongo db")
	password   = flag.String("password", "", "password for mongo db")
)

func main() {
	flag.Parse()
	/// to check if the params valid
	if *topic == "" {
		log.Fatalln("the topic should not be null, please use --topic=... ")
	}
	if *channel == "" {
		log.Fatalln("the channel should not be null, please use --channel=... ")
	}
	// if *nsq_addr == "" {
	// 	log.Fatalln("the address should not be null, please use --address=... ")
	// }
	if *collection == "" {
		log.Fatalln("the collection should not be null, please use --collection=... ")
	}
	if *user == "" {
		log.Fatalln("the username to access mongo should not be nul, please use --user=...")
	}
	if *password == "" {
		log.Fatalln("the password should not be null, please use --password=...")
	}
	if *mongo_addr == "" {
		log.Fatalln("the mongodb address should not be null, please use --mongoadd=...")
	}
	if *database == "" {
		log.Fatalln("the database name should not be null, please use --db=...")
	}
	///
	/// connect to mongodb
	println(*mongo_addr)
	session, err := mgo.Dial(*mongo_addr)
	for err != nil {
		glog.Errorf("MongoDB Dial Error: %v", err)
		glog.Errorf("waiting for 1 second ...")
		time.Sleep(1000 * time.Millisecond)
		session, err = mgo.Dial(*mongo_addr)
	}
	defer session.Close()
	//
	db := session.DB(*database)
	// err = db.Login(*user, *password)
	// if err != nil {
	// 	glog.Errorf("MongoDB Login Error: %v", err)
	// 	return
	// }
	// Optional. Switch the session to a monotonic behavior.
	session.SetMode(mgo.Monotonic, true)
	table := db.C(*collection)
	///

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	cfg := nsq.NewConfig()

	consumer, err := nsq.NewConsumer(*topic, *channel, cfg)

	if err != nil {
		fmt.Println("Error")
	}

	consumer.AddHandler(nsq.HandlerFunc(func(m *nsq.Message) error {
		// handle the message
		// go routine to write the message into db
		// var out = new(bson.D)
		fmt.Printf("%s", string(m.Body))
		//err := bson.Unmarshal(m.Body, &out)
		var out map[string]interface{}
		err := json.Unmarshal(m.Body, &out)
		if err != nil {
			glog.Errorf("Error to unmarshal the json")
		}
		err = table.Insert(out)
		//db.Insert(&this)
		if err != nil {
			glog.Errorf("Error saving event: %s", err)
			return err
		}
		return nil
	}))

	err = consumer.ConnectToNSQD(*nsq_addr)
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
