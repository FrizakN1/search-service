package handlers

import (
	"context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"search-service/proto/searchpb"
)

func (s *SearchServiceServer) IndexHardwareSingle(ctx context.Context, req *searchpb.Hardware) (*searchpb.Empty, error) {
	if err := s.HardwareSearch.IndexHardwareSingle(ctx, req); err != nil {
		return nil, status.Error(codes.Internal, "failed to index hardware")
	}

	return &searchpb.Empty{}, nil
}

func (s *SearchServiceServer) IndexHardware(ctx context.Context, req *searchpb.IndexHardwareRequest) (*searchpb.Empty, error) {
	if err := s.HardwareSearch.EnsureIndexHardware(ctx); err != nil {
		return nil, status.Error(codes.Internal, "failed to ensure hardware")
	}

	if err := s.HardwareSearch.IndexHardware(ctx, req.Hardware); err != nil {
		return nil, status.Error(codes.Internal, "failed to index hardware")
	}

	return &searchpb.Empty{}, nil
}

func (s *SearchServiceServer) SearchHardware(ctx context.Context, req *searchpb.SearchHardwareRequest) (*searchpb.SearchHardwareResponse, error) {
	ids, total, err := s.HardwareSearch.SearchHardware(ctx, req.Search, req.SearchFilter)
	if err != nil {
		log.Println(err)
		return nil, status.Error(codes.Internal, "failed to search hardware")
	}

	return &searchpb.SearchHardwareResponse{HardwareIDs: ids, Total: total}, nil
}
