package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
)

var (
	sqsSvc *sqs.SQS
	s3Svc  *s3.S3
)

type Payload struct {
	TxnHash  string   `json:"txnHash"`
	Owner    string   `json:"owner"`
	TokenId  string   `json:"tokenId"`
	PartsIds []string `json:"partsIds"`
}

func handleMessage(msg *sqs.Message, qURL string) {
	fmt.Println("RECEIVING MESSAGE >>> ")

	receivedData := *msg.Body

	sqsSvc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(qURL),
		ReceiptHandle: msg.ReceiptHandle,
	})

	fmt.Println(receivedData)

	payload := &Payload{}

	if err := json.Unmarshal([]byte(receivedData), payload); err != nil {
		fmt.Println(err)
		return
	}

	partsListJoin := ""
	for i, id := range payload.PartsIds {
		partsListJoin += id
		if i != len(payload.PartsIds)-1 {
			partsListJoin += "-"
		}
	}

	data, _ := os.ReadFile("./face.jpg")
	reader := bytes.NewReader(data)

	_, err := s3Svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(""),
		Key:    aws.String(payload.TokenId + "_" + partsListJoin + ".jpg"),
		Body:   reader,
	})

	if err != nil {
		fmt.Println(err)
		return
	}
}

func main() {

	// Initialize a session that the SDK will use to load
	// credentials from the shared credentials file. (~/.aws/credentials).
	sess, _ := session.NewSession(&aws.Config{
		Region:      aws.String("us-west-2"),
		Credentials: credentials.NewStaticCredentials("", "", ""),
	})

	sqsSvc = sqs.New(sess)
	s3Svc = s3.New(sess)

	qURL := ""

	start := time.Now()
	output, err := sqsSvc.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(qURL),
		MaxNumberOfMessages: aws.Int64(1),
	})
	elapsed := time.Since(start)

	fmt.Printf("received elapsed: %s\n", elapsed)
	if err != nil {
		fmt.Printf("failed to fetch sqs message %v", err)
	}

	for _, m := range output.Messages {
		start := time.Now()

		handleMessage(m, qURL)

		elapsed := time.Since(start)

		fmt.Printf("delete elapsed: %s\n", elapsed)
	}
}
