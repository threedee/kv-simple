package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
)

// KVStore uses a lock for map
type KVStore struct {
	mu       sync.RWMutex
	m        map[string][]byte
	fileName string
}

func (k *KVStore) save() error {
	var err error
	if file, err := json.Marshal(k.m); err == nil {
		if err = ioutil.WriteFile(k.fileName, file, 0644); err == nil {
			return nil
		}
	}
	return fmt.Errorf("problem saving file %s, %v", k.fileName, err)

}

func (k *KVStore) init() {
	k.mu.Lock()
	defer k.mu.Unlock()

	if file, err := ioutil.ReadFile(k.fileName); err == nil {
		if err = json.Unmarshal(file, &k.m); err == nil {
			return
		}
	}
	k.m = make(map[string][]byte)
}

func (k *KVStore) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	key := req.URL.Path[len("/"):]

	switch req.Method {
	case http.MethodPut:
		defer req.Body.Close()
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			log.Fatalf("error reading body, %v", err)
		}
		k.mu.Lock()
		defer k.mu.Unlock()
		k.m[key] = body
		w.WriteHeader(http.StatusNoContent)
		k.save()
	case http.MethodGet:
		k.mu.RLock()
		defer k.mu.RUnlock()
		if value, ok := k.m[key]; !ok {
			w.WriteHeader(http.StatusNotFound)
		} else {
			w.Write(value)
		}
	case http.MethodDelete:
		k.mu.Lock()
		defer k.mu.Unlock()
		delete(k.m, key)
		k.save()
	}
}

func main() {
	filename := flag.String("f", "key_value.db.json", "file name used for the store.")
	addr := flag.String("addr", ":5000", "Listen address")
	flag.Parse()
	store := &KVStore{fileName: *filename}

	store.init()

	fmt.Println("Start listening on" + *addr)
	if err := http.ListenAndServe(*addr, store); err != nil {
		log.Fatalf("could not listen on %v, %v", addr, err)
	}
}
