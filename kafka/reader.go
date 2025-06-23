package kafka

import (
	"fmt"
	"github.com/segmentio/kafka-go"
	"os"
)

func NewKafkaReader(topic string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{fmt.Sprintf("%s:%s", os.Getenv("KAFKA_ADDRESS"), os.Getenv("KAFKA_PORT"))},
		Topic:   topic,
	})
}
