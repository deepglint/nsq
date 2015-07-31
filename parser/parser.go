package parser

import (
	"regexp"
	"strconv"
	"strings"
	"time"
)

var validFlagNameRegex = regexp.MustCompile(`^[\.a-zA-Z0-9_-]+((#ephemeral)|(#once)|(#ttl@[\d]+)|(#nodisk)|(#memsize@[\d]+)|(#circle))*$`)

func IsValidName(name string) bool {
	if !strings.Contains(name, "#") {
		return true
	} else {
		return validFlagNameRegex.MatchString(name)
	}
}
func GetAllFlagNames() []string {
	names := []string{"circle", "once", "ttl", "nodisk", "memsize@\\d"}
	return names
}
func GetRealName(name string) string {
	if !strings.Contains(name, "#") {
		return name
	} else {
		index := strings.Index(name, "#")
		if strings.Contains(name, "#ephemeral") {
			return name[:index] + "#ephemeral"
		} else {
			return name[:index]
		}

	}
}
func Parse(name string) map[string]interface{} {
	tmpMap := make(map[string]interface{})
	if !strings.Contains(name, "#") {
		return tmpMap
	}
	flags := strings.Split(name, "#")
	for i := 0; i < len(flags); i++ {
		if flags[i] == "" || flags[i] == "ephemeral" {
			continue
		}
		if strings.Contains(flags[i], "@") {
			index := strings.Index(flags[i], "@")
			flagName := flags[i][:index]
			flagNum, _ := strconv.Atoi(flags[i][index+1:])
			tmpMap[flagName] = flagNum
		} else {
			tmpMap[flags[i]] = true
		}

	}
	return tmpMap
}

func Time2NowInMillisecond(createTime int64) int {
	t := time.Now().UnixNano() - createTime
	return (int)(t / int64(time.Millisecond))
}
