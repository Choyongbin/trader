package uiapi

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestConcurrentBroadcastUsesSingleClientWriter(t *testing.T) {
	server := testServer(t)
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	url := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/ws"
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	deadline := time.Now().Add(time.Second)
	for {
		server.mu.RLock()
		registered := len(server.clients) == 1
		server.mu.RUnlock()
		if registered {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("websocket client registration timeout")
		}
		time.Sleep(time.Millisecond)
	}

	const producers = 20
	const messagesPerProducer = 5
	var wg sync.WaitGroup
	for producer := 0; producer < producers; producer++ {
		wg.Add(1)
		go func(producer int) {
			defer wg.Done()
			for message := 0; message < messagesPerProducer; message++ {
				server.broadcast(map[string]any{"producer": producer, "message": message})
			}
		}(producer)
	}
	wg.Wait()

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	for received := 0; received < producers*messagesPerProducer; received++ {
		if _, _, err = conn.ReadMessage(); err != nil {
			t.Fatalf("received=%d: %v", received, err)
		}
	}

	var disconnectWG sync.WaitGroup
	for producer := 0; producer < producers; producer++ {
		disconnectWG.Add(1)
		go func(producer int) {
			defer disconnectWG.Done()
			for message := 0; message < messagesPerProducer; message++ {
				server.broadcast(fmt.Sprintf("disconnect-%d-%d", producer, message))
			}
		}(producer)
	}
	_ = conn.Close()
	disconnectWG.Wait()
}
