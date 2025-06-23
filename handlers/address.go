package handlers

import (
	"context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"search-service/proto/searchpb"
)

func (s *SearchServiceServer) IndexAddress(ctx context.Context, req *searchpb.Address) (*searchpb.Empty, error) {
	if err := s.AddressSearch.IndexAddress(ctx, req); err != nil {
		return nil, status.Error(codes.Internal, "failed to index address")
	}

	return &searchpb.Empty{}, nil
}

func (s *SearchServiceServer) IndexAddresses(ctx context.Context, req *searchpb.IndexAddressesRequest) (*searchpb.Empty, error) {
	if err := s.AddressSearch.EnsureIndexAddress(ctx); err != nil {
		return nil, status.Error(codes.Internal, "failed to ensure index")
	}

	if err := s.AddressSearch.IndexAddresses(ctx, req.Addresses); err != nil {
		return nil, status.Error(codes.Internal, "failed to index addresses")
	}

	return &searchpb.Empty{}, nil
}

func (s *SearchServiceServer) SearchAddresses(ctx context.Context, req *searchpb.SearchAddress) (*searchpb.SearchAddressesResponse, error) {
	ids, total, err := s.AddressSearch.SearchAddresses(ctx, req)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to search addresses")
	}

	return &searchpb.SearchAddressesResponse{HousesIDs: ids, Total: total}, nil
}
