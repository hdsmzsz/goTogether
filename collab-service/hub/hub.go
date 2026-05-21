package hub

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

var (
	wsConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ws_active_connections",
		Help: "Number of active WebSocket connections",
	})
	wsMsgSent = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ws_messages_broadcast_total",
		Help: "Total WebSocket messages broadcast",
	})
	wsRooms = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ws_active_rooms",
		Help: "Number of active document rooms",
	})
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type Client struct {
	DocID    string
	UserID   int64
	Username string
	Conn     *websocket.Conn
	Send     chan []byte
}

type Hub struct {
	mu      sync.RWMutex
	rooms   map[string]map[*Client]bool
	rdb     *redis.Client
	mongoDB *mongo.Database

	register   chan *Client
	unregister chan *Client
	broadcast  chan *Message
	streamCh   chan *Message
}

type Message struct {
	DocID   string  `json:"doc_id"`
	UserID  int64   `json:"user_id"`
	Sender  *Client `json:"-"`
	Type    string  `json:"type"` // "update", "awareness"
	Payload []byte  `json:"payload"`
}

const streamWorkers = 4

func NewHub(rdb *redis.Client, mongoDB *mongo.Database) *Hub {
	return &Hub{
		rooms:      make(map[string]map[*Client]bool),
		rdb:        rdb,
		mongoDB:    mongoDB,
		register:   make(chan *Client),
		unregister: make(chan *Client),
		broadcast:  make(chan *Message, 256),
		streamCh:   make(chan *Message, 4096),
	}
}

func (h *Hub) Run() {
	for i := 0; i < streamWorkers; i++ {
		go h.streamWriter()
	}

	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			if h.rooms[client.DocID] == nil {
				h.rooms[client.DocID] = make(map[*Client]bool)
			}
			h.rooms[client.DocID][client] = true
			wsConnections.Inc()
			wsRooms.Set(float64(len(h.rooms)))
			h.mu.Unlock()
			log.Printf("client joined doc=%s user=%d", client.DocID, client.UserID)
			go h.sendSync(client)

		case client := <-h.unregister:
			h.mu.Lock()
			if clients, ok := h.rooms[client.DocID]; ok {
				delete(clients, client)
				if len(clients) == 0 {
					delete(h.rooms, client.DocID)
				}
			}
			close(client.Send)
			wsConnections.Dec()
			wsRooms.Set(float64(len(h.rooms)))
			h.mu.Unlock()
			log.Printf("client left doc=%s user=%d", client.DocID, client.UserID)

		case msg := <-h.broadcast:
			wsMsgSent.Inc()
			if msg.Type == "update" {
				select {
				case h.streamCh <- msg:
				default:
				}
			}
			outbound, err := json.Marshal(map[string]interface{}{
				"type":    msg.Type,
				"user_id": msg.UserID,
				"payload": msg.Payload,
			})
			if err != nil {
				log.Printf("marshal broadcast: %v", err)
				continue
			}

			h.mu.RLock()
			targets := make([]*Client, 0, len(h.rooms[msg.DocID]))
			for client := range h.rooms[msg.DocID] {
				if client != msg.Sender {
					targets = append(targets, client)
				}
			}
			h.mu.RUnlock()

			for _, c := range targets {
				go func(c *Client) {
					select {
					case c.Send <- outbound:
					default:
						h.unregister <- c
					}
				}(c)
			}
		}
	}
}

func (h *Hub) streamWriter() {
	for msg := range h.streamCh {
		h.appendToStream(msg)
	}
}

func (h *Hub) appendToStream(msg *Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	streamKey := "doc:updates:" + msg.DocID
	h.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		Values: map[string]interface{}{
			"user_id": msg.UserID,
			"payload": msg.Payload,
		},
	})
}

func (h *Hub) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("upgrade: %v", err)
		return
	}

	docID := r.URL.Query().Get("doc_id")
	if docID == "" {
		conn.Close()
		return
	}

	client := &Client{
		DocID: docID,
		Conn:  conn,
		Send:  make(chan []byte, 256),
	}
	h.register <- client

	go h.writePump(client)
	go h.readPump(client)
}

func (h *Hub) readPump(c *Client) {
	defer func() {
		h.unregister <- c
		c.Conn.Close()
	}()
	c.Conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.Conn.SetPongHandler(func(string) error {
		c.Conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})
	for {
		_, data, err := c.Conn.ReadMessage()
		if err != nil {
			break
		}
		var msg Message
		if err := json.Unmarshal(data, &msg); err != nil {
			msg = Message{Type: "update", Payload: data}
		}
		msg.DocID = c.DocID
		msg.UserID = c.UserID
		msg.Sender = c
		h.broadcast <- &msg
	}
}

func (h *Hub) writePump(c *Client) {
	ticker := time.NewTicker(30 * time.Second)
	defer func() {
		ticker.Stop()
		c.Conn.Close()
	}()
	for {
		select {
		case msg, ok := <-c.Send:
			if !ok {
				c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			c.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			c.Conn.WriteMessage(websocket.TextMessage, msg)
		case <-ticker.C:
			c.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (h *Hub) sendSync(c *Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var updates [][]byte

	oid, err := primitive.ObjectIDFromHex(c.DocID)
	if err == nil {
		var doc struct {
			YjsUpdates [][]byte `bson:"yjs_updates"`
		}
		if err := h.mongoDB.Collection("documents").FindOne(ctx, bson.M{"_id": oid}).Decode(&doc); err == nil {
			updates = doc.YjsUpdates
		}
	}

	streamKey := "doc:updates:" + c.DocID
	msgs, err := h.rdb.XRange(ctx, streamKey, "-", "+").Result()
	if err == nil {
		for _, m := range msgs {
			if payload, ok := m.Values["payload"].(string); ok {
				updates = append(updates, []byte(payload))
			}
		}
	}

	if len(updates) == 0 {
		return
	}

	msg, _ := json.Marshal(map[string]interface{}{
		"type":    "sync",
		"updates": updates,
	})
	select {
	case c.Send <- msg:
	default:
	}
}

func (h *Hub) GetCollaborators(docID string) []struct {
	UserID   int64
	Username string
} {
	h.mu.RLock()
	defer h.mu.RUnlock()
	var result []struct {
		UserID   int64
		Username string
	}
	for client := range h.rooms[docID] {
		result = append(result, struct {
			UserID   int64
			Username string
		}{client.UserID, client.Username})
	}
	return result
}
