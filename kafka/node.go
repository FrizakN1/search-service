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

type NodeConsumer struct {
	reader *kafka.Reader
	search.NodeSearch
}

type IndexNodeMessage struct {
	Type  string           `json:"type"`
	Node  *searchpb.Node   `json:"node"`
	Nodes []*searchpb.Node `json:"nodes"`
}

func NewNodeConsumer(reader *kafka.Reader, esClient *elasticsearch.Client) Consumer {
	return &NodeConsumer{
		reader:     reader,
		NodeSearch: &search.DefaultNodeSearch{Elastic: esClient},
	}
}

func (c *NodeConsumer) Start(ctx context.Context) error {
	for {
		m, err := c.reader.ReadMessage(ctx)
		if err != nil {
			return err
		}

		var msg IndexNodeMessage
		if err = json.Unmarshal(m.Value, &msg); err != nil {
			log.Printf("NodeConsumer: failed to unmarshal: %v\n", err)
			continue
		}

		switch msg.Type {
		case "single":
			if msg.Node != nil {
				if err = c.NodeSearch.EnsureIndexNode(ctx); err != nil {
					log.Printf("NodeConsumer: failed to ensure index: %v\n", err)
				}

				if err = c.NodeSearch.IndexNode(ctx, msg.Node); err != nil {
					log.Printf("NodeConsumer: failed to index single node: %v\n", err)
				}
			}
		case "batch":
			if len(msg.Nodes) > 0 {
				if err = c.NodeSearch.EnsureIndexNode(ctx); err != nil {
					log.Printf("NodeConsumer: failed to ensure index: %v\n", err)
				}

				if err = c.NodeSearch.IndexNodes(ctx, msg.Nodes); err != nil {
					log.Printf("NodeConsumer: failed to index batch nodes: %v\n", err)
				}
			}
		default:
			log.Printf("NodeConsumer: unknown type: %s\n", msg.Type)
		}
	}
}

func (c *NodeConsumer) Close() error {
	return c.reader.Close()
}
