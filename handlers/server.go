package handlers

import (
	"search-service/proto/searchpb"
	"search-service/search"
)

type SearchServiceServer struct {
	searchpb.SearchServiceServer
	NodeSearch     search.NodeSearch
	HardwareSearch search.HardwareSearch
	AddressSearch  search.AddressSearch
}
