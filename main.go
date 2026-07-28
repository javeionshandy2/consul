package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// Service represents a service instance.
type Service struct {
	ID      string   `json:"ID"`
	Service string   `json:"Service"`
	Tags    []string `json:"Tags"`
	Address string   `json:"Address"`
	Port    int      `json:"Port"`
}

// StateStore manages the state of services and the Raft index.
type StateStore struct {
	mu           sync.RWMutex
	index        uint64
	services     map[string][]Service
	leader       bool
	watchers     map[string][]chan struct{}
	leaderChange chan struct{}
}

func NewStateStore() *StateStore {
	return &StateStore{
		index:        1,
		services:     make(map[string][]Service),
		leader:       true,
		watchers:     make(map[string][]chan struct{}),
		leaderChange: make(chan struct{}),
	}
}

func (s *StateStore) GetIndex() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index
}

func (s *StateStore) IsLeader() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.leader
}

func (s *StateStore) GetServices(name string) ([]Service, uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.services[name], s.index
}

func (s *StateStore) RegisterService(srv Service) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.services[srv.Service] = append(s.services[srv.Service], srv)
	s.index++
	s.notifyWatchers(srv.Service)
}

func (s *StateStore) StepDown() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.leader {
		s.leader = false
		close(s.leaderChange)
		s.leaderChange = make(chan struct{})
		
		// Simulate election of a new leader after 1 second
		go func() {
			time.Sleep(1 * time.Second)
			s.mu.Lock()
			s.leader = true
			s.mu.Unlock()
			log.Println("New leader elected")
		}()
	}
}

func (s *StateStore) Watch(name string, minIndex uint64, timeout time.Duration) (chan struct{}, chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.index > minIndex {
		ch := make(chan struct{})
		close(ch)
		return ch, s.leaderChange
	}

	ch := make(chan struct{}, 1)
	s.watchers[name] = append(s.watchers[name], ch)
	return ch, s.leaderChange
}

func (s *StateStore) notifyWatchers(name string) {
	watchers := s.watchers[name]
	for _, ch := range watchers {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	delete(s.watchers, name)
}

func (s *StateStore) handleHealthService(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	const prefix = "/v1/health/service/"
	if len(path) <= len(prefix) {
		http.Error(w, "Missing service name", http.StatusBadRequest)
		return
	}
	serviceName := path[len(prefix):]

	q := r.URL.Query()
	indexStr := q.Get("index")
	waitStr := q.Get("wait")

	var minIndex uint64
	if indexStr != "" {
		var err error
		minIndex, err = strconv.ParseUint(indexStr, 10, 64)
		if err != nil {
			http.Error(w, "Invalid index parameter", http.StatusBadRequest)
			return
		}
	}

	var waitDuration time.Duration
	if waitStr != "" {
		var err error
		waitDuration, err = time.ParseDuration(waitStr)
		if err != nil {
			val, err2 := strconv.Atoi(waitStr)
			if err2 == nil {
				waitDuration = time.Duration(val) * time.Second
			} else {
				http.Error(w, "Invalid wait parameter", http.StatusBadRequest)
				return
			}
		}
	} else {
		waitDuration = 5 * time.Minute
	}

	if !s.IsLeader() {
		http.Error(w, "No cluster leader", http.StatusServiceUnavailable)
		return
	}

	services, currentIndex := s.GetServices(serviceName)

	if minIndex == 0 || currentIndex > minIndex {
		w.Header().Set("X-Consul-Index", strconv.FormatUint(currentIndex, 10))
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(services)
		return
	}

	watchCh, leaderChangeCh := s.Watch(serviceName, minIndex, waitDuration)

	timer := time.NewTimer(waitDuration)
	defer timer.Stop()

	select {
	case <-watchCh:
		services, currentIndex = s.GetServices(serviceName)
		w.Header().Set("X-Consul-Index", strconv.FormatUint(currentIndex, 10))
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(services)
	case <-leaderChangeCh:
		http.Error(w, "Leadership lost during query", http.StatusServiceUnavailable)
	case <-timer.C:
		w.Header().Set("X-Consul-Index", strconv.FormatUint(currentIndex, 10))
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(services)
	}
}

func (s *StateStore) handleRegisterService(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var srv Service
	if err := json.NewDecoder(r.Body).Decode(&srv); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if srv.Service == "" {
		http.Error(w, "Missing Service name", http.StatusBadRequest)
		return
	}
	s.RegisterService(srv)
	w.WriteHeader(http.StatusOK)
}

func (s *StateStore) handleRaftConfiguration(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost || r.Method == http.MethodDelete {
		s.StepDown()
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Leader stepped down"))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"Servers": [{"ID": "node1", "Address": "127.0.0.1:8300", "Leader": true}]}`))
}

func main() {
	store := NewStateStore()

	http.HandleFunc("/v1/health/service/", store.handleHealthService)
	http.HandleFunc("/v1/agent/service/register", store.handleRegisterService)
	http.HandleFunc("/v1/operator/raft/configuration", store.handleRaftConfiguration)

	log.Println("Starting Consul mock server on :8500...")
	if err := http.ListenAndServe(":8500", nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}