package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

type Entry struct {
	Value  string `json:"value"`
	Offset uint64 `json:"offset"`
}

type AppendRequest struct {
	Entry Entry `json:"entry"`
}

type AppendResponse struct {
	Offset uint64 `json:"offset"`
}

type FetchRequest struct {
	Offset uint64 `json:"offset"`
}

type FetchResponse struct {
	Entry Entry `json:"entry"`
}

type DataLog struct {
	records []Entry
}

func NewLog() *DataLog {
	return &DataLog{}
}

func (log *DataLog) Append(record Entry) (uint64, error) {
	record.Offset = uint64(len(log.records))
	log.records = append(log.records, record)
	return record.Offset, nil
}

func (log *DataLog) Read(offset uint64) (Entry, error) {
	if offset >= uint64(len(log.records)) {
		return Entry{}, ErrOffsetNotFound
	}
	return log.records[offset], nil
}

var ErrOffsetNotFound = &ErrNotFound{"offset not found"}

type ErrNotFound struct {
	Msg string
}

func (e *ErrNotFound) Error() string {
	return e.Msg
}

type WebServer struct {
	Log *DataLog
}

func NewHTTPServer(addr string) *http.Server {
	server := newWebServer()
	router := mux.NewRouter()
	router.HandleFunc("/append", server.handleAppend).Methods("POST")
	router.HandleFunc("/fetch", server.handleFetch).Methods("GET")
	return &http.Server{
		Addr:    addr,
		Handler: router,
	}
}

func newWebServer() *WebServer {
	return &WebServer{
		Log: NewLog(),
	}
}

func (s *WebServer) handleAppend(w http.ResponseWriter, r *http.Request) {
	var req AppendRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	offset, err := s.Log.Append(req.Entry)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	res := AppendResponse{Offset: offset}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *WebServer) handleFetch(w http.ResponseWriter, r *http.Request) {
	var req FetchRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	record, err := s.Log.Read(req.Offset)
	if err == ErrOffsetNotFound {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	res := FetchResponse{Entry: record}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func main() {
	server := NewHTTPServer(":8080")
	log.Fatal(server.ListenAndServe())
}
