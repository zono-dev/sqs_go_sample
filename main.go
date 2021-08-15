package main

import (
	"flag"
	"fmt"
	"time"
)

func parseArg() (*string, *string, *string, error) {
	queue := flag.String("q", "", "The name of the queue")
	profile := flag.String("p", "", "The name of the AWS profile")
	region := flag.String("r", "", "The name of the AWS region")

	flag.Parse()

	if *queue == "" || *profile == "" || *region == "" {
		fmt.Println("You must supply the name of a queue (-q QUEUE) and profile (-p PROFILE)")
		return nil, nil, nil, nil
	}
	return queue, profile, region, nil
}

func main() {

	queue, profile, region, err := parseArg()
	if err != nil {
		fmt.Println(err)
		return
	}

	sqs, err := NewSQS(profile, queue, region)
	if err != nil {
		fmt.Println(err)
		return
	}

	amap := map[string]string{
		"Attr1": "abc",
		"Attr2": "def",
		"Attr3": "ghi",
	}
	attr := MakeAttributes(amap)

	fmt.Println(attr)

	// Send
	message := fmt.Sprintf("Message:%s", time.Now().String())

	err = sqs.SendMsg(message, int64(10), attr)
	if err != nil {
		fmt.Println(err)
	}

	time.Sleep(time.Second * 2)

	// Get
	res, err := sqs.GetMsgs(10, 20)

	if err != nil {
		fmt.Println(err)
	}

	fmt.Printf("Number of received Messages:%d\n", len(res.Messages))

	// Show and delete
	for _, m := range res.Messages {
		fmt.Printf("=== Message ===\n%s\n", m.GoString())
		if err := sqs.DeleteMsg(m); err != nil {
			fmt.Println(err)
		}
		time.Sleep(time.Second * 1)
	}
}
