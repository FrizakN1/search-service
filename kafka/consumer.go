package kafka

import (
	"context"
	"log"
)

type ConsumerManger interface {
	StartAll(ctx context.Context)
	CloseAll()
}

type Consumer interface {
	Start(ctx context.Context) error
	Close() error
}

type DefaultConsumerManager struct {
	consumers []Consumer
}

func NewConsumerManager(consumers []Consumer) ConsumerManger {
	return &DefaultConsumerManager{
		consumers: consumers,
	}
}

func (m *DefaultConsumerManager) StartAll(ctx context.Context) {
	for _, consumer := range m.consumers {
		go func(consumer Consumer) {
			if err := consumer.Start(ctx); err != nil {
				log.Printf("Error starting consumer: %v\n", err)
			}
		}(consumer)
	}
}

func (m *DefaultConsumerManager) CloseAll() {
	for _, consumer := range m.consumers {
		if err := consumer.Close(); err != nil {
			log.Printf("Error closing consumer: %v\n", err)
		}
	}
}
