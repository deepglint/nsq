package protocol

import (
	//"fmt"
	"github.com/deepglint/nsq/parser"
	"regexp"
)

var validTopicChannelNameRegex = regexp.MustCompile(`^[\.a-zA-Z0-9_-]+(#ephemeral)?(#[\w#@]*)?$`)

// IsValidTopicName checks a topic name for correctness
func IsValidTopicName(name string) bool {
	return isValidName(name)
}

// IsValidChannelName checks a channel name for correctness
func IsValidChannelName(name string) bool {
	return isValidName(name)
}

func isValidName(name string) bool {
	realName := parser.GetRealName(name)
	if !parser.IsValidName(name) {
		return false
	}
	name = realName
	//fmt.Println(name)
	if len(name) > 64 || len(name) < 1 {
		return false
	}
	return validTopicChannelNameRegex.MatchString(name)
}
