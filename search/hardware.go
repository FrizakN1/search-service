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

type HardwareSearch interface {
	EnsureIndexHardware(ctx context.Context) error
	IndexHardware(ctx context.Context, hardware []*searchpb.Hardware) error
	IndexHardwareSingle(ctx context.Context, hardware *searchpb.Hardware) error
	SearchHardware(ctx context.Context, search *searchpb.Search, filter *searchpb.SearchHardwareFilter) ([]int32, int32, error)
}

type DefaultHardwareSearch struct {
	Elastic *elasticsearch.Client
}

func (s *DefaultHardwareSearch) IndexHardwareSingle(ctx context.Context, hardware *searchpb.Hardware) error {
	hardware.IsDelete = hardware.GetIsDelete()

	data, err := json.Marshal(hardware)
	if err != nil {
		return err
	}

	req := bytes.NewReader(data)

	_, err = s.Elastic.Index(
		"hardware",
		req,
		s.Elastic.Index.WithDocumentID(fmt.Sprint(hardware.Id)),
		s.Elastic.Index.WithRefresh("true"),
		s.Elastic.Index.WithContext(ctx),
	)

	return err
}

func (s *DefaultHardwareSearch) IndexHardware(ctx context.Context, hardware []*searchpb.Hardware) error {
	var buf bytes.Buffer

	for _, h := range hardware {
		meta := []byte(fmt.Sprintf(`{ "index" : { "_id" : "%d" } }%s`, h.Id, "\n"))

		h.IsDelete = h.GetIsDelete()

		data, err := json.Marshal(h)
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
		s.Elastic.Bulk.WithIndex("hardware"),
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

func (s *DefaultHardwareSearch) SearchHardware(ctx context.Context, search *searchpb.Search, filter *searchpb.SearchHardwareFilter) ([]int32, int32, error) {
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
							"fields": []string{"type.edge", "node_name.edge", "model_name.edge", "ip_address.edge", "address.street_name.edge", "address.street_type.edge", "address.house_name.edge", "address.house_type.edge"},
						},
					},
				},
				"filter": buildHardwareFilter(filter),
			},
		},
	}

	if err := json.NewEncoder(&buf).Encode(searchQuery); err != nil {
		return nil, 0, err
	}

	res, err := s.Elastic.Search(
		s.Elastic.Search.WithContext(ctx),
		s.Elastic.Search.WithIndex("hardware"),
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
			}
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

func (s *DefaultHardwareSearch) EnsureIndexHardware(ctx context.Context) error {
	res, err := s.Elastic.Indices.Exists([]string{"hardware"}, s.Elastic.Indices.Exists.WithContext(ctx))
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
				"type":                fieldParams,
				"node_name":           fieldParams,
				"model_name":          fieldParams,
				"ip_address":          fieldParams,
				"address.street_name": fieldParams,
				"address.street_type": fieldParams,
				"address.house_name":  fieldParams,
				"address.house_type":  fieldParams,
				"is_delete": map[string]interface{}{
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
		"hardware",
		s.Elastic.Indices.Create.WithBody(&buf),
		s.Elastic.Indices.Create.WithContext(ctx),
	)
	if err != nil {
		return err
	}
	defer createRes.Body.Close()

	return nil
}

func buildHardwareFilter(filter *searchpb.SearchHardwareFilter) []map[string]interface{} {
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

	return filters
}
