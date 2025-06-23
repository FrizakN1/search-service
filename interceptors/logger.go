package interceptors

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
)

func LoggingInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		resp, err := handler(ctx, req)

		statusCode := codes.OK
		if err != nil {
			log.Println(err)
			st, _ := status.FromError(err)
			statusCode = st.Code()
		}

		message := fmt.Sprintf("method=%s status=%s", info.FullMethod, statusCode)
		log.Println(message)

		return resp, err
	}
}
