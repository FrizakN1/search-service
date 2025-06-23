package handlers

import (
	"context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"search-service/proto/searchpb"
)

func (s *SearchServiceServer) IndexNode(ctx context.Context, req *searchpb.Node) (*searchpb.Empty, error) {
	if err := s.NodeSearch.IndexNode(ctx, req); err != nil {
		return nil, status.Error(codes.Internal, "failed to index node")
	}

	return &searchpb.Empty{}, nil
}

func (s *SearchServiceServer) IndexNodes(ctx context.Context, req *searchpb.IndexNodesRequest) (*searchpb.Empty, error) {
	if err := s.NodeSearch.EnsureIndexNode(ctx); err != nil {
		return nil, status.Error(codes.Internal, "failed to ensure index")
	}

	if err := s.NodeSearch.IndexNodes(ctx, req.Nodes); err != nil {
		return nil, status.Error(codes.Internal, "failed to index nodes")
	}

	return &searchpb.Empty{}, nil
}

func (s *SearchServiceServer) SearchNodes(ctx context.Context, req *searchpb.SearchNodesRequest) (*searchpb.SearchNodesResponse, error) {
	ids, total, err := s.NodeSearch.SearchNodes(ctx, req.Search, req.SearchFilter)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to search nodes")
	}

	return &searchpb.SearchNodesResponse{NodesIDs: ids, Total: total}, nil
}
