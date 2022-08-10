package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type Payload struct {
	TxnHash  string   `json:"txnHash"`
	Owner    string   `json:"owner"`
	TokenId  string   `json:"tokenId"`
	PartsIds []string `json:"partsIds"`
}

// Usage:
// go run sqs_sendmessage.go
func main() {

	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)

	sess, _ := session.NewSession(&aws.Config{
		Region:      aws.String("us-west-2"),
		Credentials: credentials.NewStaticCredentials("", "", ""),
	})

	svc := sqs.New(sess)

	// URL to our queue
	qURL := ""

	payload := &Payload{
		TxnHash:  "",
		Owner:    "",
		TokenId:  strconv.Itoa(r1.Intn(100)),
		PartsIds: []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", strconv.Itoa(r1.Intn(100))},
	}

	rawData, err := json.Marshal(payload)
	if err != nil {
		fmt.Println(err)
		return
	}

	result, err := svc.SendMessage(&sqs.SendMessageInput{
		MessageBody:    aws.String(string(rawData)),
		MessageGroupId: aws.String(""),
		QueueUrl:       &qURL,
	})

	if err != nil {
		fmt.Println("Error", err)
		return
	}

	fmt.Println("Success", *result.MessageId)
}
