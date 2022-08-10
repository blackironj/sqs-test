package main

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go/aws"
)

type SQSconfig struct {
	AwsS3Region  string
	AwsAccessKey string
	AwsSecretKey string
	SQSurl       string
}

type SQS struct {
	client *sqs.Client
	url    string
}

func NewSQSmanager(sqsConf SQSconfig) (*SQS, error) {
	cred := credentials.NewStaticCredentialsProvider(
		sqsConf.AwsAccessKey,
		sqsConf.AwsSecretKey,
		"",
	)
	awsConf, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithCredentialsProvider(cred),
		config.WithRegion(sqsConf.AwsS3Region),
	)

	if err != nil {
		return nil, err
	}

	return &SQS{
		client: sqs.NewFromConfig(awsConf),
	}, nil
}

func (thiz *SQS) SendMessage(msgBodyStruct interface{}, groupID string) error {
	rawPayload, err := json.Marshal(msgBodyStruct)
	if err != nil {
		return err
	}

	sendInput := &sqs.SendMessageInput{
		MessageBody:    aws.String(string(rawPayload)),
		QueueUrl:       aws.String(thiz.url),
		MessageGroupId: aws.String(groupID),
	}

	_, err = thiz.client.SendMessage(
		context.TODO(),
		sendInput,
	)

	return err
}

func (thiz *SQS) ReceiveMessage(numMsg int32) (*sqs.ReceiveMessageOutput, error) {
	receiveInput := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(thiz.url),
		MaxNumberOfMessages: numMsg,
	}

	output, err := thiz.client.ReceiveMessage(
		context.TODO(),
		receiveInput,
	)
	if err != nil {
		return nil, err
	}

	return output, nil
}

func (thiz *SQS) DeleteMessage(receiptHandle *string) error {
	deleteInput := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(thiz.url),
		ReceiptHandle: receiptHandle,
	}
	_, err := thiz.client.DeleteMessage(
		context.TODO(),
		deleteInput,
	)
	return err
}
