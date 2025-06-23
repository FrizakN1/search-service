package search

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"search-service/proto/searchpb"
	"strconv"
)

type NodeSearch interface {
	SearchNodes(ctx context.Context, search *searchpb.Search, filter *searchpb.SearchNodeFilter) ([]int32, int32, error)
	IndexNodes(ctx context.Context, nodes []*searchpb.Node) error
	IndexNode(ctx context.Context, node *searchpb.Node) error
	EnsureIndexNode(ctx context.Context) error
}

type DefaultNodeSearch struct {
	Elastic *elasticsearch.Client
}

func (s *DefaultNodeSearch) IndexNode(ctx context.Context, node *searchpb.Node) error {
	node.IsDelete = node.GetIsDelete()
	node.IsPassive = node.GetIsPassive()

	data, err := json.Marshal(node)
	if err != nil {
		return err
	}

	req := bytes.NewReader(data)

	_, err = s.Elastic.Index(
		"nodes",
		req,
		s.Elastic.Index.WithDocumentID(fmt.Sprint(node.Id)),
		s.Elastic.Index.WithRefresh("true"),
		s.Elastic.Index.WithContext(ctx),
	)

	return err
}

func (s *DefaultNodeSearch) IndexNodes(ctx context.Context, nodes []*searchpb.Node) error {
	var buf bytes.Buffer

	for _, node := range nodes {
		meta := []byte(fmt.Sprintf(`{ "index" : { "_id" : "%d" } }%s`, node.Id, "\n"))

		node.IsDelete = node.GetIsDelete()
		node.IsPassive = node.GetIsPassive()

		data, err := json.Marshal(node)
		if err != nil {
			return err
		}

		data = append(data, '\n')

		buf.Grow(len(meta) + len(data))

		buf.Write(meta)
		buf.Write(data)
	}

	res, err := s.Elastic.Bulk(
		bytes.NewReader(buf.Bytes()),
		s.Elastic.Bulk.WithIndex("nodes"),
		s.Elastic.Bulk.WithRefresh("true"),
		s.Elastic.Bulk.WithContext(ctx),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	var bulkResp struct {
		Errors bool `json:"errors"`
	}

	if err = json.NewDecoder(res.Body).Decode(&bulkResp); err != nil {
		return err
	}

	if bulkResp.Errors {
		return fmt.Errorf("bulk indexing had errors")
	}

	return nil
}

func (s *DefaultNodeSearch) SearchNodes(ctx context.Context, search *searchpb.Search, filter *searchpb.SearchNodeFilter) ([]int32, int32, error) {
	var buf bytes.Buffer

	searchQuery := map[string]interface{}{
		"from": search.Offset,
		"size": search.Limit,
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					{
						"multi_match": map[string]interface{}{
							"query":  search.Query,
							"fields": []string{"name.edge", "zone.edge", "owner.edge", "address.street_name.edge", "address.street_type.edge", "address.house_name.edge", "address.house_type.edge", "type.edge"},
						},
					},
				},
				"filter": buildNodeFilter(filter),
			},
		},
	}

	if err := json.NewEncoder(&buf).Encode(searchQuery); err != nil {
		return nil, 0, err
	}

	res, err := s.Elastic.Search(
		s.Elastic.Search.WithContext(ctx),
		s.Elastic.Search.WithIndex("nodes"),
		s.Elastic.Search.WithBody(&buf),
	)
	if err != nil {
		return nil, 0, err
	}
	defer res.Body.Close()

	var r struct {
		Hits struct {
			Total struct {
				Value int32 `json:"value"`
			} `json:"total"`
			Hits []struct {
				ID string `json:"_id"`
			} `json:"hits"`
		} `json:"hits"`
	}

	if err = json.NewDecoder(res.Body).Decode(&r); err != nil {
		return nil, 0, err
	}

	var ids []int32

	for _, hit := range r.Hits.Hits {
		id, err := strconv.Atoi(hit.ID)
		if err != nil {
			return nil, 0, fmt.Errorf("invalid id %s: %v", hit.ID, err)
		}

		ids = append(ids, int32(id))
	}

	return ids, r.Hits.Total.Value, nil
}

func (s *DefaultNodeSearch) EnsureIndexNode(ctx context.Context) error {
	res, err := s.Elastic.Indices.Exists([]string{"nodes"}, s.Elastic.Indices.Exists.WithContext(ctx))
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode == 200 {
		return nil
	}

	fieldParams := map[string]interface{}{
		"type": "text",
		"fields": map[string]interface{}{
			"edge": map[string]interface{}{
				"type":            "text",
				"analyzer":        "edge_ngram_analyzer",
				"search_analyzer": "standard",
			},
		},
	}

	settings := map[string]interface{}{
		"settings": map[string]interface{}{
			"analysis": map[string]interface{}{
				"filter": map[string]interface{}{
					"edge_ngram_filter": map[string]interface{}{
						"type":     "edge_ngram",
						"min_gram": 2,
						"max_gram": 20,
					},
				},
				"analyzer": map[string]interface{}{
					"edge_ngram_analyzer": map[string]interface{}{
						"type":      "custom",
						"tokenizer": "standard",
						"filter":    []string{"lowercase", "edge_ngram_filter"},
					},
				},
			},
		},
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{
				"name":                fieldParams,
				"zone":                fieldParams,
				"owner":               fieldParams,
				"address.street_name": fieldParams,
				"address.street_type": fieldParams,
				"address.house_name":  fieldParams,
				"address.house_type":  fieldParams,
				"type":                fieldParams,
				"is_delete": map[string]interface{}{
					"type":       "boolean",
					"null_value": false,
				},
				"is_passive": map[string]interface{}{
					"type":       "boolean",
					"null_value": false,
				},
			},
		},
	}

	var buf bytes.Buffer
	if err = json.NewEncoder(&buf).Encode(settings); err != nil {
		return err
	}

	createRes, err := s.Elastic.Indices.Create(
		"nodes",
		s.Elastic.Indices.Create.WithBody(&buf),
		s.Elastic.Indices.Create.WithContext(ctx),
	)
	if err != nil {
		return err
	}
	defer createRes.Body.Close()

	return nil
}

func buildNodeFilter(filter *searchpb.SearchNodeFilter) []map[string]interface{} {
	var filters []map[string]interface{}

	if filter.GetUseIsDelete() {
		if filter.GetIsDelete() {
			filters = append(filters, map[string]interface{}{
				"term": map[string]interface{}{
					"is_delete": true,
				},
			})
		} else {
			filters = append(filters, map[string]interface{}{
				"bool": map[string]interface{}{
					"must_not": map[string]interface{}{
						"exists": map[string]interface{}{
							"field": "is_delete",
						},
					},
				},
			})
		}
	}

	if filter.GetUseIsPassive() {
		if filter.GetIsPassive() {
			filters = append(filters, map[string]interface{}{
				"term": map[string]interface{}{
					"is_passive": true,
				},
			})
		} else {
			filters = append(filters, map[string]interface{}{
				"bool": map[string]interface{}{
					"must_not": map[string]interface{}{
						"exists": map[string]interface{}{
							"field": "is_passive",
						},
					},
				},
			})
		}
	}

	return filters
}
