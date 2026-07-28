package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestBlockingQuery_LeaderTransition(t *testing.T) {
	store := NewStateStore()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/agent/service/register" {
			store.handleRegisterService(w, r)
		} else if r.URL.Path == "/v1/operator/raft/configuration" {
			store.handleRaftConfiguration(w, r)
		} else {
			store.handleHealthService(w, r)
		}
	}))
	defer server.Close()

	// 1. Register a service
	srv := Service{
		ID:      "test-service-1",
		Service: "test-service",
		Address: "127.0.0.1",
		Port:    8080,
	}
	srvData, _ := json.Marshal(srv)
	resp, err := http.Post(server.URL+"/v1/agent/service/register", "application/json", bytes.NewBuffer(srvData))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("Failed to register service: %v", err)
	}

	// Get current index
	resp, err = http.Get(server.URL + "/v1/health/service/test-service")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("Failed to get service: %v", err)
	}
	indexHeader := resp.Header.Get("X-Consul-Index")
	currentIndex, _ := strconv.ParseUint(indexHeader, 10, 64)

	// 2. Start a blocking query in a goroutine
	var wg sync.WaitGroup
	wg.Add(1)
	var blockingResp *http.Response
	var blockingErr error

	go func() {
		defer wg.Done()
		client := &http.Client{Timeout: 5 * time.Second}
		url := server.URL + "/v1/health/service/test-service?index=" + indexHeader + "&wait=4s"
		blockingResp, blockingErr = client.Get(url)
	}()

	// Wait a bit for the blocking query to be active
	time.Sleep(200 * time.Millisecond)

	// 3. Trigger leader transition
	resp, err = http.Post(server.URL+"/v1/operator/raft/configuration", "application/json", nil)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("Failed to trigger step-down: %v", err)
	}

	// Wait for the blocking query to finish
	wg.Wait()

	if blockingErr != nil {
		t.Fatalf("Blocking query failed with error: %v", blockingErr)
	}

	// The blocking query should return 503 Service Unavailable due to leader transition
	if blockingResp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("Expected status 503 Service Unavailable, got %d", blockingResp.StatusCode)
	}
}

func TestBlockingQuery_DataChange(t *testing.T) {
	store := NewStateStore()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/agent/service/register" {
			store.handleRegisterService(w, r)
		} else {
			store.handleHealthService(w, r)
		}
	}))
	defer server.Close()

	// Register initial service
	srv := Service{
		ID:      "test-service-1",
		Service: "test-service",
		Address: "127.0.0.1",
		Port:    8080,
	}
	srvData, _ := json.Marshal(srv)
	http.Post(server.URL+"/v1/agent/service/register", "application/json", bytes.NewBuffer(srvData))

	// Get current index
	resp, _ := http.Get(server.URL + "/v1/health/service/test-service")
	indexHeader := resp.Header.Get("X-Consul-Index")

	// Start blocking query
	var wg sync.WaitGroup
	wg.Add(1)
	var blockingResp *http.Response
	var blockingErr error

	go func() {
		defer wg.Done()
		client := &http.Client{Timeout: 5 * time.Second}
		url := server.URL + "/v1/health/service/test-service?index=" + indexHeader + "&wait=4s"
		blockingResp, blockingErr = client.Get(url)
	}()

	time.Sleep(200 * time.Millisecond)

	// Register another service instance to trigger update
	srv2 := Service{
		ID:      "test-service-2",
		Service: "test-service",
		Address: "127.0.0.1",
		Port:    8081,
	}
	srvData2, _ := json.Marshal(srv2)
	http.Post(server.URL+"/v1/agent/service/register", "application/json", bytes.NewBuffer(srvData2))

	wg.Wait()

	if blockingErr != nil {
		t.Fatalf("Blocking query failed: %v", blockingErr)
	}

	if blockingResp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200 OK, got %d", blockingResp.StatusCode)
	}

	newIndexHeader := blockingResp.Header.Get("X-Consul-Index")
	if newIndexHeader == indexHeader {
		t.Errorf("Expected index to change, got unchanged index %s", newIndexHeader)
	}

	var services []Service
	json.NewDecoder(blockingResp.Body).Decode(&services)
	if len(services) != 2 {
		t.Errorf("Expected 2 services, got %d", len(services))
	}
}