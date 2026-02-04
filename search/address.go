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

type AddressSearch interface {
	SearchAddresses(ctx context.Context, search *searchpb.SearchAddress) ([]int32, int32, error)
	IndexAddresses(ctx context.Context, addresses []*searchpb.Address) error
	IndexAddress(ctx context.Context, address *searchpb.Address) error
	EnsureIndexAddress(ctx context.Context) error
}

type DefaultAddressSearch struct {
	Elastic *elasticsearch.Client
}

func (s *DefaultAddressSearch) IndexAddress(ctx context.Context, address *searchpb.Address) error {
	data, err := json.Marshal(address)
	if err != nil {
		return err
	}

	req := bytes.NewReader(data)

	_, err = s.Elastic.Index(
		"addresses",
		req,
		s.Elastic.Index.WithDocumentID(fmt.Sprint(address.HouseId)),
		s.Elastic.Index.WithRefresh("true"),
		s.Elastic.Index.WithContext(ctx),
	)

	return err
}

func (s *DefaultAddressSearch) IndexAddresses(ctx context.Context, addresses []*searchpb.Address) error {
	var buf bytes.Buffer

	for _, address := range addresses {
		meta := []byte(fmt.Sprintf(`{ "index" : { "_id" : "%d" } }%s`, address.HouseId, "\n"))

		data, err := json.Marshal(address)
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
		s.Elastic.Bulk.WithIndex("addresses"),
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

func (s *DefaultAddressSearch) SearchAddresses(ctx context.Context, search *searchpb.SearchAddress) ([]int32, int32, error) {

	//searchQuery := map[string]interface{}{
	//	"from": search.Offset,
	//	"size": search.Limit,
	//	"query": map[string]interface{}{
	//		"bool": map[string]interface{}{
	//			"must": []map[string]interface{}{
	//				{
	//					"multi_match": map[string]interface{}{
	//						"query":  search.Query,
	//						"fields": []string{"street_name.edge^3", "street_type.edge^1", "house_name.edge^2", "house_type.edge^1"},
	//					},
	//				},
	//			},
	//		},
	//		"should": []map[string]interface{}{
	//			{
	//				"multi_match": map[string]interface{}{
	//					"query":  search.Query,
	//					"fields": []string{"street_name", "street_type", "house_name", "house_type"},
	//					"type":   "best_fields",
	//					"boost":  1,
	//				},
	//			},
	//		},
	//	},
	//}
	var searchQuery map[string]interface{}

	if search.HouseQuery == "" {
		searchQuery = map[string]interface{}{
			"from": search.Offset,
			"size": search.Limit,
			"query": map[string]interface{}{
				"match": map[string]interface{}{
					"street_name.edge": map[string]interface{}{
						"query": search.StreetQuery,
						"boost": 3,
					},
				},
			},
			"_source": []string{
				"street_name",
				"street_type_short_name",
			},
			"sort": []map[string]interface{}{
				{
					"_score": map[string]interface{}{
						"order": "desc",
					},
				},
				{
					"house_name.keyword": map[string]interface{}{
						"order": "asc",
					},
				},
			},
		}
	} else {
		searchQuery = map[string]interface{}{
			"from": search.Offset,
			"size": search.Limit,
			"query": map[string]interface{}{
				"function_score": map[string]interface{}{
					"query": map[string]interface{}{
						"bool": map[string]interface{}{
							"must": []map[string]interface{}{
								{
									"match": map[string]interface{}{
										"street_name.edge": map[string]interface{}{
											"query": search.StreetQuery,
											"boost": 3,
										},
									},
								},
							},
							"should": []map[string]interface{}{
								{
									"match": map[string]interface{}{
										"house_name.edge": map[string]interface{}{
											"query": search.HouseQuery,
										},
									},
								},
							},
							"minimum_should_match": 1,
							//"filter": []map[string]interface{}{
							//	{
							//		"bool": map[string]interface{}{
							//			"should": []map[string]interface{}{
							//				{
							//					"match": map[string]interface{}{
							//						"house_name.edge": search.HouseQuery,
							//					},
							//				},
							//				{
							//					"bool": map[string]interface{}{
							//						"must_not": map[string]interface{}{
							//							"exists": map[string]interface{}{
							//								"field": "house_name",
							//							},
							//						},
							//					},
							//				},
							//			},
							//			"minimum_should_match": 0,
							//		},
							//	},
							//},
						},
					},
					"functions": []map[string]interface{}{
						{
							"filter": map[string]interface{}{
								"term": map[string]interface{}{
									"house_name.keyword": search.HouseQuery,
								},
							},
							"weight": 100,
						},
						{
							"filter": map[string]interface{}{
								"bool": map[string]interface{}{
									"must": []map[string]interface{}{
										{
											"prefix": map[string]interface{}{
												"house_name.keyword": search.HouseQuery,
											},
										},
										{
											"regexp": map[string]interface{}{
												"house_name.keyword": search.HouseQuery + "[^0-9].*",
											},
										},
									},
								},
							},
							"weight": 50,
						},
						{
							"filter": map[string]interface{}{
								"match": map[string]interface{}{
									"house_name.edge": search.HouseQuery,
								},
							},
							"weight": 25,
						},
						//{
						//	"filter": map[string]interface{}{
						//		"regexp": map[string]interface{}{
						//			"house_name.keyword": ".*\\/.*",
						//		},
						//	},
						//	"weight": 0.8,
						//},
						//{
						//	"filter": map[string]interface{}{
						//		"bool": map[string]interface{}{
						//			"must_not": map[string]interface{}{
						//				"regexp": map[string]interface{}{
						//					"house_name.keyword": ".*\\/.*",
						//				},
						//			},
						//		},
						//	},
						//	"weight": 1.2,
						//},
					},
					"score_mode": "sum",
				},
			},
			"sort": []map[string]interface{}{
				{
					"_score": map[string]interface{}{
						"order": "desc",
					},
				},
				{
					"house_name.keyword": map[string]interface{}{
						"order": "asc",
					},
				},
			},
			"track_total_hits": true,
		}
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(searchQuery); err != nil {
		return nil, 0, err
	}

	res, err := s.Elastic.Search(
		s.Elastic.Search.WithContext(ctx),
		s.Elastic.Search.WithIndex("addresses"),
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

func (s *DefaultAddressSearch) EnsureIndexAddress(ctx context.Context) error {
	res, err := s.Elastic.Indices.Exists([]string{"addresses"}, s.Elastic.Indices.Exists.WithContext(ctx))
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode == 200 {
		return nil
	}

	//fieldParams := map[string]interface{}{
	//	"type": "text",
	//	"fields": map[string]interface{}{
	//		"edge": map[string]interface{}{
	//			"type":            "text",
	//			"analyzer":        "edge_ngram_analyzer",
	//			"search_analyzer": "standard",
	//		},
	//	},
	//}
	//
	//settings := map[string]interface{}{
	//	"settings": map[string]interface{}{
	//		"analysis": map[string]interface{}{
	//			"filter": map[string]interface{}{
	//				"edge_ngram_filter": map[string]interface{}{
	//					"type":     "edge_ngram",
	//					"min_gram": 2,
	//					"max_gram": 20,
	//				},
	//			},
	//			"analyzer": map[string]interface{}{
	//				"edge_ngram_analyzer": map[string]interface{}{
	//					"type":      "custom",
	//					"tokenizer": "standard",
	//					"filter":    []string{"lowercase", "edge_ngram_filter"},
	//				},
	//			},
	//		},
	//	},
	//	"mappings": map[string]interface{}{
	//		"properties": map[string]interface{}{
	//			"street_name": fieldParams,
	//			"street_type": fieldParams,
	//			"house_name":  fieldParams,
	//			"house_type":  fieldParams,
	//		},
	//	},
	//}

	settings := map[string]interface{}{
		"settings": map[string]interface{}{
			"analysis": map[string]interface{}{
				"filter": map[string]interface{}{
					"edge_ngram_filter": map[string]interface{}{
						"type":     "edge_ngram",
						"min_gram": 1,
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
				"street_name": map[string]interface{}{
					"type": "text",
					"fields": map[string]interface{}{
						"edge": map[string]interface{}{
							"type":            "text",
							"analyzer":        "edge_ngram_analyzer",
							"search_analyzer": "standard", // Для точного поиска при вводе
						},
						"keyword": map[string]interface{}{
							"type": "keyword", // Для точной сортировки
						},
					},
				},
				"street_type_short_name": map[string]interface{}{
					"type": "keyword",
				},
				"house_name": map[string]interface{}{
					"type": "text",
					"fields": map[string]interface{}{
						"edge": map[string]interface{}{
							"type":            "text",
							"analyzer":        "edge_ngram_analyzer",
							"search_analyzer": "standard",
						},
						"keyword": map[string]interface{}{
							"type": "keyword",
						},
					},
				},
				"house_type_short_name": map[string]interface{}{
					"type": "keyword",
				},
			},
		},
	}

	var buf bytes.Buffer
	if err = json.NewEncoder(&buf).Encode(settings); err != nil {
		return err
	}

	createRes, err := s.Elastic.Indices.Create(
		"addresses",
		s.Elastic.Indices.Create.WithBody(&buf),
		s.Elastic.Indices.Create.WithContext(ctx),
	)
	if err != nil {
		return err
	}
	defer createRes.Body.Close()

	return nil
}
