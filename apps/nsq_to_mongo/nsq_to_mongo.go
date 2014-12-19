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
	user       = flag.String("user", "timeline", "user name for mongo db")
	password   = flag.String("password", "deepdbdb", "password for mongo db")
)

type NsqToMongo struct {
	topic      string
	channel    string
	nsq_addr   string
	database   string
	collection string
	mongo_addr string
	user       string
	password   string
	avaliable  bool
	session    *mgo.Session
	table      *mgo.Collection
	db         *mgo.Database
	consumer   *nsq.Consumer
}

// (this *NsqToMongo) func check() error{
// 	if this.topic==""{

// 	}
// }
///connet to the nsqd and returns the error message
func (this *NsqToMongo) ConnectToNsq() error {
	cfg := nsq.NewConfig()
	consumer, err := nsq.NewConsumer(this.topic, this.channel, cfg)

	if err != nil {
		fmt.Println("Error")
		return err
	}

	consumer.AddHandler(nsq.HandlerFunc(this.HandleMessage))

	err = consumer.ConnectToNSQD(this.nsq_addr)
	if err != nil {
		fmt.Println("Error connecting")
		return err
	}
	this.consumer = consumer
	return nil
}
func (this *NsqToMongo) ConnectToMongo() error {

	mongo_addr := this.mongo_addr
	session, err := mgo.Dial(mongo_addr)
	for err != nil {
		glog.Errorf("MongoDB Dial Error: %v", err)
		glog.Errorf("waiting for 1 second ...")
		time.Sleep(1000 * time.Millisecond)
		session, err = mgo.Dial(mongo_addr)
	}
	//defer session.Close()
	db := session.DB(*database)
	err = db.Login(this.user, this.password)
	if err != nil {
		glog.Errorf("MongoDB Login Error: %v", err)
		this.Close()
	}
	session.SetMode(mgo.Monotonic, true)
	table := db.C(*collection)
	this.session = session
	this.table = table
	this.db = db
	return nil
}

func (this *NsqToMongo) HandleMessage(m *nsq.Message) error {
	fmt.Printf("%s", string(m.Body))
	//err := bson.Unmarshal(m.Body, &out)
	var out map[string]interface{}
	err := json.Unmarshal(m.Body, &out)
	if err != nil {
		glog.Errorf("Error to unmarshal the json")
	}
	err = this.table.Insert(out)
	//db.Insert(&this)
	if err != nil {
		glog.Errorf("Error saving event: %s", err)
		//this.ConnectToMongo()
		//err = this.table.Insert(out)
		//return err
		this.Close()
	}
	return nil
}

func (this *NsqToMongo) Close() error {
	this.session.Close()
	this.consumer.Stop()
	return nil
}

func main() {
	flag.Parse()
	// /// to check if the params valid
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
	// if *user == "" {
	// 	log.Fatalln("the username to access mongo should not be nul, please use --user=...")
	// }
	// if *password == "" {
	// 	log.Fatalln("the password should not be null, please use --password=...")
	// }
	if *mongo_addr == "" {
		log.Fatalln("the mongodb address should not be null, please use --mongoadd=...")
	}
	if *database == "" {
		log.Fatalln("the database name should not be null, please use --db=...")
	}

	nsqtomongo := &NsqToMongo{
		topic:      *topic,
		channel:    *channel,
		nsq_addr:   *nsq_addr,
		collection: *collection,
		user:       *user,
		password:   *password,
		mongo_addr: *mongo_addr,
		database:   *database,
	}
	println(nsqtomongo.user, nsqtomongo.password)
	_ = nsqtomongo.ConnectToMongo()

	_ = nsqtomongo.ConnectToNsq()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case <-nsqtomongo.consumer.StopChan:
			return
		case <-sigChan:
			nsqtomongo.Close()
		}
	}
}
