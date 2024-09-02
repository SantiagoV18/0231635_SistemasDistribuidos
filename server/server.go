package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	/*
		"github.com/your/project/api" // api para Record
		"github.com/your/project/log" *///preguntar sobre esto
)

type Server struct {
	log *log.Log
}

func NewServer(dir string, c log.Config) (*Server, error) {
	l, err := log.NewLog(dir, c)
	if err != nil {
		return nil, err
	}
	return &Server{log: l}, nil
}

func (s *Server) Start() error {
	http.HandleFunc("/append", s.handleAppend)
	http.HandleFunc("/read", s.handleRead)
	http.HandleFunc("/truncate", s.handleTruncate)
	return http.ListenAndServe(":8080", nil)
}

func (s *Server) handleAppend(w http.ResponseWriter, r *http.Request) {
	record := &api.Record{}
	err := json.NewDecoder(r.Body).Decode(record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	off, err := s.log.Append(record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write([]byte(fmt.Sprintf("Appended record at offset %d", off)))
}

func (s *Server) handleRead(w http.ResponseWriter, r *http.Request) {
	off, err := strconv.ParseUint(r.URL.Query().Get("offset"), 10, 64)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	record, err := s.log.Read(off)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(record)
}

func (s *Server) handleTruncate(w http.ResponseWriter, r *http.Request) {
	lowest, err := strconv.ParseUint(r.URL.Query().Get("lowest"), 10, 64)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	err = s.log.Truncate(lowest)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write([]byte("Truncated log successfully"))
}
