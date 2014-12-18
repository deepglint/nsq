package main

import (
	"io"
	"io/ioutil"
	"log"
	"net"
	"time"
)

func reader(r io.Reader) {
	buf := make([]byte, 1024)
	for {
		n, err := r.Read(buf[:])
		if err != nil {
			return
		}
		println("Client got:", string(buf[0:n]))
	}
}

func main() {
	println("read the test data file")
	dat1, err := ioutil.ReadFile("test1.txt")
	if err != nil {
		panic(err)
	}
	dat2, err := ioutil.ReadFile("test2.txt")
	if err != nil {
		panic(err)
	}
	dat3, err := ioutil.ReadFile("test3.txt")
	if err != nil {
		panic(err)
	}
	dat4, err := ioutil.ReadFile("test4.txt")
	if err != nil {
		panic(err)
	}
	c, err := net.Dial("unix", "/tmp/testsock.sock")
	if err != nil {
		panic(err)
	}
	defer c.Close()

	go reader(c)
	n := 0
	var dat *[]byte
	for {
		go func() {

			switch n % 4 {
			case 0:
				dat = &dat1
				break
			case 1:
				dat = &dat2
				break
			case 2:
				dat = &dat3
				break
			case 3:
				dat = &dat4
				break
			}
			_, err := c.Write(*dat)
			println(len(*dat))
			if err != nil {
				log.Fatal("write error:", err)
				//break
			}
		}()
		n++
		time.Sleep(30)
	}
}
