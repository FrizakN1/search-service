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

type AddressConsumer struct {
	reader *kafka.Reader
	search.AddressSearch
}

type IndexAddressMessage struct {
	Type      string              `json:"type"`
	Address   *searchpb.Address   `json:"address"`
	Addresses []*searchpb.Address `json:"addresses"`
}

func NewAddressConsumer(reader *kafka.Reader, esClient *elasticsearch.Client) Consumer {
	return &AddressConsumer{
		reader:        reader,
		AddressSearch: &search.DefaultAddressSearch{Elastic: esClient},
	}
}

func (c *AddressConsumer) Start(ctx context.Context) error {
	for {
		m, err := c.reader.ReadMessage(ctx)
		if err != nil {
			return err
		}

		var msg IndexAddressMessage
		if err = json.Unmarshal(m.Value, &msg); err != nil {
			log.Printf("AddressConsumer: failed to unmarshal: %v\n", err)
			continue
		}

		if len(msg.Addresses) > 0 {
			if err = c.AddressSearch.EnsureIndexAddress(ctx); err != nil {
				log.Printf("AddressConsumer: failed to ensure index: %v\n", err)
			}

			if err = c.AddressSearch.IndexAddresses(ctx, msg.Addresses); err != nil {
				log.Printf("AddressConsumer: failed to index batch addresses: %v\n", err)
			}
		}

		//switch msg.Type {
		//case "single":
		//	if msg.Address != nil {
		//		if err = c.AddressSearch.IndexAddress(ctx, msg.Address); err != nil {
		//			log.Printf("AddressConsumer: failed to index single address: %v\n", err)
		//		}
		//	}
		//case "batch":
		//	if len(msg.Addresses) > 0 {
		//		if err = c.AddressSearch.IndexAddresses(ctx, msg.Addresses); err != nil {
		//			log.Printf("AddressConsumer: failed to index batch addresses: %v\n", err)
		//		}
		//	}
		//default:
		//	log.Printf("AddressConsumer: unknown type: %s\n", msg.Type)
		//}
	}
}

func (c *AddressConsumer) Close() error {
	return c.reader.Close()
}
