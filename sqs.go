package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type SQS struct {
	URL  *string
	SVC  *sqs.SQS
	SESS *session.Session
}

func NewSQS(profile, queueName, region *string) (*SQS, error) {
	s := &SQS{}

	sess, err := session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region: aws.String(*region),
		},
		Profile: *profile,
	})

	if err != nil {
		return nil, err
	}

	s.SESS = sess
	s.SVC = sqs.New(sess)

	result, err := s.SVC.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: queueName,
	})
	if err != nil {
		return nil, err
	}
	s.URL = result.QueueUrl

	return s, nil
}

func (s SQS) DeleteMsg(msg *sqs.Message) error {
	params := &sqs.DeleteMessageInput{
		QueueUrl:      s.URL,
		ReceiptHandle: aws.String(*msg.ReceiptHandle),
	}
	_, err := s.SVC.DeleteMessage(params)
	if err != nil {
		return err
	}
	return nil
}

func (s SQS) GetMsgs(num, timeout int) (*sqs.ReceiveMessageOutput, error) {
	res, err := s.SVC.ReceiveMessage(&sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl:            s.URL,
		MaxNumberOfMessages: aws.Int64(int64(num)),
		VisibilityTimeout:   aws.Int64(int64(timeout)),
	})
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (s SQS) SendMsg(message string, delaysec int64, attributes map[string]*sqs.MessageAttributeValue) error {

	_, err := s.SVC.SendMessage(&sqs.SendMessageInput{
		DelaySeconds:      aws.Int64(delaysec),
		MessageAttributes: attributes,
		MessageBody:       aws.String(message),
		QueueUrl:          s.URL,
	})
	if err != nil {
		return err
	}
	return nil
}

func MakeAttributes(amap map[string]string) map[string]*sqs.MessageAttributeValue {
	attr := map[string]*sqs.MessageAttributeValue{}
	for k, v := range amap {
		attr[k] = &sqs.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(v),
		}
	}
	return attr
}
