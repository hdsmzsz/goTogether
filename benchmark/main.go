package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

type Message struct {
	Type    string `json:"type"`
	Payload string `json:"payload"`
}

type Stats struct {
	TotalSent     atomic.Int64
	TotalReceived atomic.Int64
	Latencies     sync.Map // stores []time.Duration per user
	Errors        atomic.Int64
	ConnectTime   sync.Map // userID -> duration
}

func main() {
	wsHost := envOr("WS_HOST", "localhost:8082")
	docID := envOr("DOC_ID", "test-bench-doc")
	userCount, _ := strconv.Atoi(envOr("USERS", "10"))
	duration, _ := strconv.Atoi(envOr("DURATION", "30"))
	editInterval, _ := strconv.Atoi(envOr("INTERVAL_MS", "200"))

	fmt.Println("========================================")
	fmt.Println("  goTogether Collaboration Benchmark")
	fmt.Println("========================================")
	fmt.Printf("  WebSocket Host : %s\n", wsHost)
	fmt.Printf("  Document ID    : %s\n", docID)
	fmt.Printf("  Users          : %d\n", userCount)
	fmt.Printf("  Duration       : %ds\n", duration)
	fmt.Printf("  Edit Interval  : %dms\n", editInterval)
	fmt.Println("========================================")
	fmt.Println()

	stats := &Stats{}
	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Connect all users
	fmt.Printf("[Phase 1] Connecting %d users...\n", userCount)
	conns := make([]*websocket.Conn, userCount)
	for i := 0; i < userCount; i++ {
		wg.Add(1)
		go func(userID int) {
			defer wg.Done()
			start := time.Now()
			u := url.URL{Scheme: "ws", Host: wsHost, Path: "/ws", RawQuery: "doc_id=" + docID}
			conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
			if err != nil {
				log.Printf("  user-%d connect failed: %v", userID, err)
				stats.Errors.Add(1)
				return
			}
			elapsed := time.Since(start)
			stats.ConnectTime.Store(userID, elapsed)
			conns[userID] = conn
		}(i)
	}
	wg.Wait()

	connected := 0
	var totalConnTime time.Duration
	for i := 0; i < userCount; i++ {
		if conns[i] != nil {
			connected++
			if v, ok := stats.ConnectTime.Load(i); ok {
				totalConnTime += v.(time.Duration)
			}
		}
	}
	fmt.Printf("  Connected: %d/%d (avg connect time: %v)\n\n", connected, userCount, totalConnTime/time.Duration(max(connected, 1)))

	if connected == 0 {
		fmt.Println("No connections established. Exiting.")
		return
	}

	// Start readers on all connections
	fmt.Println("[Phase 2] Starting benchmark...")
	for i := 0; i < userCount; i++ {
		if conns[i] == nil {
			continue
		}
		go func(userID int, conn *websocket.Conn) {
			for {
				select {
				case <-stop:
					return
				default:
				}
				conn.SetReadDeadline(time.Now().Add(5 * time.Second))
				_, data, err := conn.ReadMessage()
				if err != nil {
					if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
						return
					}
					select {
					case <-stop:
						return
					default:
					}
					if websocket.IsUnexpectedCloseError(err) {
						return
					}
					continue
				}
				stats.TotalReceived.Add(1)

				// Try to extract timestamp for latency calculation
				var msg map[string]interface{}
				if json.Unmarshal(data, &msg) == nil {
					if tsStr, ok := msg["ts"].(string); ok {
						if ts, err := time.Parse(time.RFC3339Nano, tsStr); err == nil {
							latency := time.Since(ts)
							if v, ok := stats.Latencies.Load(userID); ok {
								lats := v.([]time.Duration)
								stats.Latencies.Store(userID, append(lats, latency))
							} else {
								stats.Latencies.Store(userID, []time.Duration{latency})
							}
						}
					}
				}
			}
		}(i, conns[i])
	}

	// Start writers - each user sends edits at the configured interval
	for i := 0; i < userCount; i++ {
		if conns[i] == nil {
			continue
		}
		wg.Add(1)
		go func(userID int, conn *websocket.Conn) {
			defer wg.Done()
			ticker := time.NewTicker(time.Duration(editInterval) * time.Millisecond)
			defer ticker.Stop()
			seq := 0
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					seq++
					msg := map[string]interface{}{
						"type":    "update",
						"payload": fmt.Sprintf("user-%d-edit-%d-%s", userID, seq, randomText(20)),
						"ts":      time.Now().Format(time.RFC3339Nano),
						"user_id": userID,
					}
					data, _ := json.Marshal(msg)
					conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
					if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
						stats.Errors.Add(1)
						continue
					}
					stats.TotalSent.Add(1)
				}
			}
		}(i, conns[i])
	}

	// Progress reporter
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		elapsed := 0
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				elapsed += 5
				fmt.Printf("  [%ds] sent=%d received=%d errors=%d\n",
					elapsed, stats.TotalSent.Load(), stats.TotalReceived.Load(), stats.Errors.Load())
			}
		}
	}()

	time.Sleep(time.Duration(duration) * time.Second)
	close(stop)
	wg.Wait()

	// Close all connections
	for i := 0; i < userCount; i++ {
		if conns[i] != nil {
			conns[i].Close()
		}
	}

	// Report
	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("  Results")
	fmt.Println("========================================")
	totalSent := stats.TotalSent.Load()
	totalRecv := stats.TotalReceived.Load()
	totalErr := stats.Errors.Load()
	fmt.Printf("  Total Sent      : %d messages\n", totalSent)
	fmt.Printf("  Total Received  : %d messages\n", totalRecv)
	fmt.Printf("  Errors          : %d\n", totalErr)
	fmt.Printf("  Send Throughput : %.1f msg/s\n", float64(totalSent)/float64(duration))
	fmt.Printf("  Recv Throughput : %.1f msg/s\n", float64(totalRecv)/float64(duration))

	// Expected: each message sent by 1 user should be received by (connected-1) users
	expectedRecv := totalSent * int64(connected-1)
	if expectedRecv > 0 {
		deliveryRate := float64(totalRecv) / float64(expectedRecv) * 100
		fmt.Printf("  Delivery Rate   : %.1f%% (expected %d)\n", deliveryRate, expectedRecv)
	}

	// Latency stats
	var allLatencies []time.Duration
	stats.Latencies.Range(func(_, v interface{}) bool {
		allLatencies = append(allLatencies, v.([]time.Duration)...)
		return true
	})
	if len(allLatencies) > 0 {
		var sum time.Duration
		minLat := allLatencies[0]
		maxLat := allLatencies[0]
		for _, l := range allLatencies {
			sum += l
			if l < minLat {
				minLat = l
			}
			if l > maxLat {
				maxLat = l
			}
		}
		avg := sum / time.Duration(len(allLatencies))
		fmt.Printf("  Latency (avg)   : %v\n", avg)
		fmt.Printf("  Latency (min)   : %v\n", minLat)
		fmt.Printf("  Latency (max)   : %v\n", maxLat)
		fmt.Printf("  Latency samples : %d\n", len(allLatencies))
	}
	fmt.Println("========================================")
}

func randomText(n int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyz")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
