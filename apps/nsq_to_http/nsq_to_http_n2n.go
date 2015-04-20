//This is the Nsq to Http with N2N model version
//In this program ,User should note the positon of config file or the web url for the config infomations

package main

import (
	"github.com/deepglint/muses/util/config"
	"log"
	"net/http"
)

type Node struct {
	Address        string
	Topic          string
	Channel        string
	Mode           string
	MaxInFlightNum int
	ProcesserNum   int
	HttpTimeout    int
	ContentType    string
	Route          []string
}

func main() {

}
