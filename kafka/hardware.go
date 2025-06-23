package kafka

import (
	"context"
	"encoding/json"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/segmentio/kafka-go"
	"log"
	"search-service/proto/searchpb"
	"search-service/search"
)

type HardwareConsumer struct {
	reader *kafka.Reader
	search.HardwareSearch
}

type IndexHardwareMessage struct {
	Type           string               `json:"type"`
	HardwareSingle *searchpb.Hardware   `json:"hardware_single"`
	Hardware       []*searchpb.Hardware `json:"hardware"`
}

func NewHardwareConsumer(reader *kafka.Reader, esClient *elasticsearch.Client) Consumer {
	return &HardwareConsumer{
		reader:         reader,
		HardwareSearch: &search.DefaultHardwareSearch{Elastic: esClient},
	}
}

func (c *HardwareConsumer) Start(ctx context.Context) error {
	for {
		m, err := c.reader.ReadMessage(ctx)
		if err != nil {
			return err
		}

		var msg IndexHardwareMessage
		if err = json.Unmarshal(m.Value, &msg); err != nil {
			log.Printf("HardwareConsumer: failed to unmarshal: %v\n", err)
			continue
		}

		switch msg.Type {
		case "single":
			if msg.HardwareSingle != nil {
				if err = c.HardwareSearch.EnsureIndexHardware(ctx); err != nil {
					log.Printf("HardwareConsumer: failed to ensure index: %v\n", err)
				}

				if err = c.HardwareSearch.IndexHardwareSingle(ctx, msg.HardwareSingle); err != nil {
					log.Printf("HardwareConsumer: failed to index single hardware: %v\n", err)
				}
			}
		case "batch":
			if len(msg.Hardware) > 0 {
				if err = c.HardwareSearch.EnsureIndexHardware(ctx); err != nil {
					log.Printf("HardwareConsumer: failed to ensure index: %v\n", err)
				}

				if err = c.HardwareSearch.IndexHardware(ctx, msg.Hardware); err != nil {
					log.Printf("HardwareConsumer: failed to index batch hardware: %v\n", err)
				}
			}
		default:
			log.Printf("HardwareConsumer: unknown type: %s\n", msg.Type)
		}
	}
}

func (c *HardwareConsumer) Close() error {
	return c.reader.Close()
}
