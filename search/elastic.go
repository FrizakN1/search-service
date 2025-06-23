package search

import (
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
)

func NewElasticClient(address string) (*elasticsearch.Client, error) {
	fmt.Println(address)
	cfg := elasticsearch.Config{
		Addresses: []string{
			fmt.Sprintf("http://%s", address),
		},
	}

	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	res, err := es.Info()
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	return es, nil
}
