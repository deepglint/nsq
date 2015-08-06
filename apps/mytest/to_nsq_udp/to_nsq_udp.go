package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"net"
	"os"
	"time"
)

var host = flag.String("host", "127.0.0.1", "host")
var port = flag.String("port", "4148", "port")
var topic = flag.String("topic", "yadda", "topic")
var message = flag.String("message", "this is a message", "message")
var count = flag.Int("count", 1, "message count")
var revResponse = flag.Bool("rr", false, "is revResponse")

//go run timeclient.go -host time.nist.gov
func main() {
	flag.Parse()
	addr, err := net.ResolveUDPAddr("udp", *host+":"+*port)
	if err != nil {
		fmt.Println("Can't resolve address: ", err)
		os.Exit(1)
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		fmt.Println("Can't dial: ", err)
		os.Exit(1)
	}
	defer conn.Close()
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(len(*message)))

	senddata := make([]byte, 0)
	a := []byte("PUB " + *topic + "\n")
	c := []byte(*message)
	senddata = append(senddata, a...)
	senddata = append(senddata, b...)
	senddata = append(senddata, c...)
	fmt.Println(senddata)
	for i := 0; i < *count; i++ {
		time.Sleep(50 * time.Millisecond)
		_, err = conn.Write(senddata)
		if err != nil {
			fmt.Println("failed:", err)
			os.Exit(1)
		}
		if *revResponse {
			data := make([]byte, 1024)
			_, err = conn.Read(data)
			if err != nil {
				fmt.Println("failed to read UDP msg because of ", err)
				os.Exit(1)
			}
			if string(data[:]) != "OK" {
				fmt.Println(string(data[:]))
			}

		}
	}

	//t := binary.BigEndian.Uint32(data)

	os.Exit(0)
}
