package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"sync"
)

const (
	port = ":8080"
)

type Record struct {
	Value  string `json:"value"`
	Offset int64  `json:"offset"`
}

type ProduceRequest struct {
	Record Record `json:"record"`
}

type ProduceResponse struct {
	Offset int64 `json:"offset"`
}

type ConsumeRequest struct {
	Offset int64 `json:"offset"`
}

type ConsumeResponse struct {
	Record Record `json:"record"`
}

type Server struct {
	mu      sync.Mutex
	records []Record
}

func (s *Server) decodeProduceRequest(r *http.Request) (*ProduceRequest, error) {
	var req ProduceRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	return &req, err
}

func (s *Server) saveRecord(record Record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record.Offset = int64(len(s.records))
	s.records = append(s.records, record)
}

func (s *Server) encodeProduceResponse(w http.ResponseWriter, offset int64) error {
	res := ProduceResponse{Offset: offset}
	return json.NewEncoder(w).Encode(res)
}

func (s *Server) Produce(w http.ResponseWriter, r *http.Request) {
	req, err := s.decodeProduceRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	record := req.Record
	s.saveRecord(record)

	err = s.encodeProduceResponse(w, record.Offset)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *Server) decodeConsumeRequest(r *http.Request) (*ConsumeRequest, error) {
	var req ConsumeRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	return &req, err
}

func (s *Server) getRecordByOffset(offset int64) (*Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if offset >= int64(len(s.records)) {
		return nil, errors.New("record not found")
	}
	return &s.records[offset], nil
}

func (s *Server) encodeConsumeResponse(w http.ResponseWriter, record *Record) error {
	res := ConsumeResponse{Record: *record}
	return json.NewEncoder(w).Encode(res)
}

func (s *Server) Consume(w http.ResponseWriter, r *http.Request) {
	req, err := s.decodeConsumeRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	record, err := s.getRecordByOffset(req.Offset)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	err = s.encodeConsumeResponse(w, record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func main() {
	server := &Server{}

	http.HandleFunc("/produce", server.Produce)
	http.HandleFunc("/consume", server.Consume)

	log.Fatal(http.ListenAndServe(port, nil))
}
