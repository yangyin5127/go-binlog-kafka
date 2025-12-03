package main

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/yangyin5127/go-binlog-kafka/logger"
)

var (
	producer sarama.SyncProducer
)

func InitKafka(kafkaAddrs []string) (err error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.MaxMessageBytes = 104857600
	config.Producer.Partitioner = sarama.NewHashPartitioner
	config.Producer.Flush.Messages = 1
	config.Producer.Flush.MaxMessages = 1
	config.Net.MaxOpenRequests = 1
	config.Producer.Return.Successes = true
	producer, err = sarama.NewSyncProducer(kafkaAddrs, config)
	return
}

func sendKafkaMsg(ctx context.Context, Msg []byte, topicName string) (err error) {

	msg := &sarama.ProducerMessage{}
	msg.Topic = topicName
	msg.Value = sarama.ByteEncoder(Msg)

	if _, _, err := producer.SendMessage(msg); err != nil {
		logger.ErrorWith(ctx, err).Interface("msg", msg).Msg("send kafka message error")
		return err
	}
	return nil
}
