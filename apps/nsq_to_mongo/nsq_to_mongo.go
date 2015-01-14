package main

import (
	//"bytes"
	//"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	// "github.com/deepglint/glog"
	"github.com/deepglint/go-nsq"
	"labix.org/v2/mgo"
	//"labix.org/v2/mgo/bson"
	"log"
	"os"
	"os/signal"
	"reflect"
	"syscall"
	"time"
	//"github.com/bitly/nsq/util"
	"github.com/deepglint/muses/models"
	//"github.com/deepglint/muses/util/jsontime"
)

var (
	topic      = flag.String("topic", "db_events", "nsq topic")
	channel    = flag.String("channel", "nsq2mongo", "nsq channel to subscribe")
	nsq_addr   = flag.String("address", "192.168.2.100:4150", "nsq address")
	database   = flag.String("db", "libra", "data base name to insert data")
	collection = flag.String("collection", "events", "collection of mongo db")
	mongo_addr = flag.String("mongoadd", "192.168.2.100:27017", "address of mongo db")
	user       = flag.String("user", "libra", "user name for mongo db")
	password   = flag.String("password", "deepdbdb", "password for mongo db")
)
var bsonEventChan chan models.BsonEvent
var bsonEventChan2 chan models.BsonEvent
var bsonEventChan3 chan models.BsonEvent
var bsonEventChan4 chan models.BsonEvent
var bsonEventChan5 chan models.BsonEvent

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

var total = 0

// type BsonEvent struct {
// 	Id        bson.ObjectId       "_id" //`json:"_id,omitempty"` //auto generate, don't fill it
// 	StartTime *jsontime.Timestamp //`bson:"starttime,omitempty" json:"starttime,omitempty"`
// 	Raw       []byte
// }

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

	consumer.AddHandler(nsq.HandlerFunc(this.EventMessageRouter))

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
		log.Printf("MongoDB Dial Error: %v", err)
		log.Printf("waiting for 1 second ...")
		time.Sleep(1000 * time.Millisecond)
		session, err = mgo.Dial(mongo_addr)
	}
	//defer session.Close()
	db := session.DB(*database)
	err = db.Login(this.user, this.password)
	if err != nil {
		log.Printf("MongoDB Login Error: %v", err)
		this.Close()
	}
	session.SetMode(mgo.Monotonic, true)
	table := db.C(*collection)
	this.session = session
	this.table = table
	this.db = db

	return nil
}

func (this *NsqToMongo) EventMessageHandler() {
	session := this.session.Copy()
	db := session.DB(this.database)
	err := db.Login(this.user, this.password)
	if err != nil {
		log.Printf("MongoDB Login Error: %v", err)
		this.Close()
	}
	table := db.C(this.collection)
	//var newBsonEvent models.BsonEvent
	for {
		newBsonEvent := <-bsonEventChan
		log.Println("Got the bsonEvent obj:%v", newBsonEvent)
		err := table.Insert(newBsonEvent)
		if err != nil {
			log.Printf("Error saving event: %s", err)
			session.Close()
			break
		}
		log.Println("Saved Successful")
	}
}

func (this *NsqToMongo) EventMessageHandler2() {
	session := this.session.Copy()
	db := session.DB(this.database)
	err := db.Login(this.user, this.password)
	if err != nil {
		log.Printf("MongoDB Login Error: %v", err)
		this.Close()
	}
	table := db.C(this.collection)
	//var newBsonEvent models.BsonEvent
	for {
		newBsonEvent := <-bsonEventChan2
		log.Println("Got the bsonEvent obj:%v", newBsonEvent)
		err := table.Insert(newBsonEvent)
		if err != nil {
			log.Printf("Error saving event: %s", err)
			session.Close()
			break
		}
		log.Println("Saved Successful")
	}
}
func (this *NsqToMongo) EventMessageHandler3() {
	session := this.session.Copy()
	db := session.DB(this.database)
	err := db.Login(this.user, this.password)
	if err != nil {
		log.Printf("MongoDB Login Error: %v", err)
		this.Close()
	}
	table := db.C(this.collection)
	//var newBsonEvent models.BsonEvent
	for {
		newBsonEvent := <-bsonEventChan3
		log.Println("Got the bsonEvent obj:%v", newBsonEvent)
		err := table.Insert(newBsonEvent)
		if err != nil {
			log.Printf("Error saving event: %s", err)
			session.Close()
			break
		}
		log.Println("Saved Successful")
	}
}
func (this *NsqToMongo) EventMessageHandler4() {
	session := this.session.Copy()
	db := session.DB(this.database)
	err := db.Login(this.user, this.password)
	if err != nil {
		log.Printf("MongoDB Login Error: %v", err)
		this.Close()
	}
	table := db.C(this.collection)
	//var newBsonEvent models.BsonEvent
	for {
		newBsonEvent := <-bsonEventChan4
		log.Println("Got the bsonEvent obj:%v", newBsonEvent)
		err := table.Insert(newBsonEvent)
		if err != nil {
			log.Printf("Error saving event: %s", err)
			session.Close()
			break
		}
		log.Println("Saved Successful")
	}
}
func (this *NsqToMongo) EventMessageHandler5() {
	session := this.session.Copy()
	db := session.DB(this.database)
	err := db.Login(this.user, this.password)
	if err != nil {
		log.Printf("MongoDB Login Error: %v", err)
		this.Close()
	}
	table := db.C(this.collection)
	//var newBsonEvent models.BsonEvent
	for {
		newBsonEvent := <-bsonEventChan5
		log.Println("Got the bsonEvent obj:%v", newBsonEvent)
		err := table.Insert(newBsonEvent)
		if err != nil {
			log.Printf("Error saving event: %s", err)
			session.Close()
			break
		}
		log.Println("Saved Successful")
	}
}

func (this *NsqToMongo) EventMessageRouter(m *nsq.Message) error {
	var tmpTest = make(map[string]interface{}, 0)

	err := json.Unmarshal(m.Body[:], &tmpTest)
	k := reflect.ValueOf(tmpTest["Id"])
	log.Println("The Id is %s", k)
	if k.String() == "" {
		return nil
	}
	log.Println("Come the msg:%s", string(m.Body))
	var tmpEvent models.Event
	err = json.Unmarshal(m.Body[:], &tmpEvent)
	if err != nil {
		log.Println("Error when convert nsq msg body into Event obj")
		return err
	}
	//tmpEvent.Id = bson.NewObjectId()
	bsonEvent, err2 := models.EventToBsonEvent(tmpEvent)
	if err2 != nil {
		log.Println("Error when convert event obj into bson obj")
		return err2
	}
	total++
	total = total % 5
	if total == 0 {
		bsonEventChan <- *bsonEvent
	}
	if total == 1 {
		bsonEventChan2 <- *bsonEvent
	}
	if total == 2 {
		bsonEventChan3 <- *bsonEvent
	}
	if total == 3 {
		bsonEventChan4 <- *bsonEvent
	}
	if total == 4 {
		bsonEventChan5 <- *bsonEvent
	}
	return nil
}

// func (this *NsqToMongo) EventMessageRouter(m *nsq.Message) error {
// 	fmt.Printf("%s", string(m.Body))
// 	//err := bson.Unmarshal(m.Body, &out)
// 	//var out map[string]interface{}
// 	var tmp models.Event
// 	err := json.Unmarshal(m.Body[:], &tmp)
// 	if err != nil {
// 		log.Printf("event unmarshal error: %v", err)
// 		return nil
// 	}

// 	out := new(BsonEvent)
// 	out.Id = tmp.Id
// 	out.StartTime = tmp.StartTime
// 	//log.Printf("bson object:%v", out)
// 	var b bytes.Buffer
// 	//w := gzip.NewWriter(&b)
// 	w, _ := gzip.NewWriterLevel(&b, 9)
// 	w.Write(m.Body)
// 	w.Close()
// 	out.Raw = b.Bytes()
// 	log.Printf("the event bin obj:%v", out)
// 	err = this.table.Insert(out)
// 	//db.Insert(&this)
// 	if err != nil {
// 		log.Printf("Error saving event: %s", err)
// 		//this.ConnectToMongo()
// 		//err = this.table.Insert(out)
// 		//return err
// 		this.Close()
// 	}
// 	return nil
// }

func (this *NsqToMongo) Close() error {
	this.session.Close()
	this.consumer.Stop()
	return nil
}

func main() {
	bsonEventChan = make(chan models.BsonEvent)
	bsonEventChan2 = make(chan models.BsonEvent)
	bsonEventChan3 = make(chan models.BsonEvent)
	bsonEventChan4 = make(chan models.BsonEvent)
	bsonEventChan5 = make(chan models.BsonEvent)
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

	go nsqtomongo.EventMessageHandler()
	go nsqtomongo.EventMessageHandler2()
	go nsqtomongo.EventMessageHandler3()
	go nsqtomongo.EventMessageHandler4()
	go nsqtomongo.EventMessageHandler5()

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
