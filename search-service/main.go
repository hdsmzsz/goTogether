package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/elastic/go-elasticsearch/v8"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/spike/goTogether/pkg/discovery"
	"github.com/spike/goTogether/pkg/tracing"
	searchpb "github.com/spike/goTogether/proto/search"
	"github.com/spike/goTogether/search-service/consumer"
	"github.com/spike/goTogether/search-service/service"
	"google.golang.org/grpc"
)

func main() {
	shutdown, err := tracing.InitTracer("search-service")
	if err != nil {
		log.Printf("tracing init failed: %v", err)
	} else {
		defer shutdown(context.Background())
	}

	esAddrs := os.Getenv("ES_ADDRESSES")
	if esAddrs == "" {
		esAddrs = "http://localhost:9200"
	}
	es, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: strings.Split(esAddrs, ","),
	})
	if err != nil {
		log.Fatalf("es connect: %v", err)
	}

	rabbitURL := os.Getenv("RABBITMQ_URL")
	if rabbitURL == "" {
		rabbitURL = "amqp://guest:guest@localhost:5672/"
	}
	rabbitConn, err := amqp.Dial(rabbitURL)
	if err != nil {
		log.Fatalf("rabbitmq connect: %v", err)
	}
	defer rabbitConn.Close()

	indexer := service.NewSearchService(es)

	cons := consumer.NewConsumer(rabbitConn, indexer)
	go cons.Start(context.Background())

	port := os.Getenv("SEARCH_SERVICE_PORT")
	if port == "" {
		port = "9004"
	}
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	registry, _ := discovery.NewRegistry()
	if registry != nil {
		registry.Register(context.Background(), "search-service", "search-service:"+port)
	}

	srv := grpc.NewServer()
	searchpb.RegisterSearchServiceServer(srv, indexer)

	go func() {
		log.Printf("search-service listening on :%s", port)
		if err := srv.Serve(lis); err != nil {
			log.Fatalf("serve: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	srv.GracefulStop()
	if registry != nil {
		registry.Close()
	}
}
