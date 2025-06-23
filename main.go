package main

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"search-service/handlers"
	"search-service/interceptors"
	"search-service/kafka"
	"search-service/proto/searchpb"
	"search-service/search"
)

func main() {
	esClient, err := search.NewElasticClient(fmt.Sprintf("%s:%s", os.Getenv("ELASTICSEARCH_ADDRESS"), os.Getenv("ELASTICSEARCH_PORT")))
	if err != nil {
		log.Fatalln(err)
		return
	}

	consumerManager := kafka.NewConsumerManager([]kafka.Consumer{
		kafka.NewNodeConsumer(kafka.NewKafkaReader("index-node"), esClient),
		kafka.NewHardwareConsumer(kafka.NewKafkaReader("index-hardware"), esClient),
		kafka.NewAddressConsumer(kafka.NewKafkaReader("index-address"), esClient),
	})

	consumerManager.StartAll(context.Background())
	defer consumerManager.CloseAll()

	searchService := &handlers.SearchServiceServer{
		NodeSearch:     &search.DefaultNodeSearch{Elastic: esClient},
		HardwareSearch: &search.DefaultHardwareSearch{Elastic: esClient},
		AddressSearch:  &search.DefaultAddressSearch{Elastic: esClient},
	}

	lis, err := net.Listen(os.Getenv("APP_NETWORK"), fmt.Sprintf(":%s", os.Getenv("APP_PORT")))
	if err != nil {
		log.Fatalln(err)
		return
	}

	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			interceptors.LoggingInterceptor(),
		),
	)

	searchpb.RegisterSearchServiceServer(grpcServer, searchService)
	log.Printf("Search-service started on :%s\n", os.Getenv("APP_PORT"))
	grpcServer.Serve(lis)
}
