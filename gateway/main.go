package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/spike/goTogether/gateway/handler"
	"github.com/spike/goTogether/gateway/router"
	"github.com/spike/goTogether/pkg/discovery"
	"github.com/spike/goTogether/pkg/tracing"
)

func main() {
	shutdown, err := tracing.InitTracer("gateway")
	if err != nil {
		log.Printf("tracing init failed: %v", err)
	} else {
		defer shutdown(context.Background())
	}

	registry, err := discovery.NewRegistry()
	if err != nil {
		log.Printf("etcd init failed (will use direct addresses): %v", err)
	}

	h := handler.New(registry)
	r := router.Setup(h)

	port := os.Getenv("GATEWAY_PORT")
	if port == "" {
		port = "8080"
	}

	srv := &http.Server{Addr: ":" + port, Handler: r}
	go func() {
		log.Printf("gateway listening on :%s", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
	if registry != nil {
		registry.Close()
	}
	log.Println("gateway stopped")
}

var _ = gin.New // ensure import
