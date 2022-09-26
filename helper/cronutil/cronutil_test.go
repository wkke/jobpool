package cronutil

import (
	"fmt"
	"regexp"
	"testing"
	"time"
)

func TestCron(t *testing.T) {
	spec := "TZ=Asia/Shanghai */1 * * * *"
	now := time.Now()
	time, err := Next(spec, now)
	if err != nil {
		fmt.Println("error ocur")
		fmt.Println(err)
		// t.Fatalf("fail next cron ", err)
	}
	fmt.Println(time)

}


func TestAfterNoonCron(t *testing.T) {
	spec := "01 00 * * *"
	now := time.Now()
	time, err := NextWithZone(spec,"", now)
	if err != nil {
		fmt.Println("error ocur")
		fmt.Println(err)
		// t.Fatalf("fail next cron ", err)
	}
	fmt.Println(time)

}

func TestRegex(t *testing.T) {
	var hasNamespaceFilter = regexp.MustCompile(`Namespace[\s]+==`)
	info := `Namespace   == "default"`
	if hasNamespaceFilter.Match([]byte(info)) {
		fmt.Println("ok")
	}else {
		fmt.Println("not")
	}
}