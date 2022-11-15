package main

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"github.com/tesrohit-developer/go-dmux/plugins"
	"log"
	"net/http"
	"os"
	"strconv"
)

var scanPlugin interface{}
var unsidelinePlugin interface{}

func getScanPlugin(meta PluginMeta) interface{} {
	s := plugins.NewManager(meta.Prefix, meta.Path, "", &plugins.ScanImplPlugin{})
	//defer s.Dispose()
	err := s.Init()
	if err != nil {
		log.Fatal(err.Error())
	}
	s.Launch()
	p, err := s.GetInterface(meta.Id)
	if err != nil {
		log.Fatal(err.Error())
	}
	return p
}

func getUnsidelinePlugin(meta PluginMeta) interface{} {
	s := plugins.NewManager(meta.Prefix, meta.Path, "", &plugins.UnsidelineImplPlugin{})
	//defer s.Dispose()
	err := s.Init()
	if err != nil {
		log.Fatal(err.Error())
	}
	s.Launch()
	p, err := s.GetInterface(meta.Id)
	if err != nil {
		log.Fatal(err.Error())
	}
	return p
}

func scan(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	var request = plugins.ScanWithStartRowEndRowRequest{
		StartKey: vars["startRow"],
		EndKey:   vars["endRow"],
	}
	rows, err := scanPlugin.(plugins.ScanImpl).ScanWithStartRowEndRow(request)
	if err != nil {
		w.WriteHeader(500)
		json.NewEncoder(w).Encode(err.Error())
		return
	}
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(rows)
}

func unsideline(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	var request = plugins.UnsidelineByKeyRequest{
		Key: vars["key"],
	}
	rows, err := unsidelinePlugin.(plugins.UnsidelineImpl).UnsidelineByKey(request)
	if err != nil {
		log.Printf("Received error while executing plugin")
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		w.Header().Set("Content-Type", "application/json")
		return
	}
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(rows)
}

func healthCheck(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
}

func main() {
	log.Println("Hi starting the API")
	args := os.Args[1:]
	sz := len(args)
	var path string
	if sz == 1 {
		path = args[0]
	} else {
		log.Fatalf("Received incorrect number of args %d expected only 1", sz)
	}
	unsidelineConfig := UnsidelineConfig{
		FilePath: path,
	}
	unsidelineContainerConfig := unsidelineConfig.getUnsidelineContainerConfig()
	scanPlugin = getScanPlugin(unsidelineContainerConfig.PluginsMeta.ScanPluginMeta)
	unsidelinePlugin = getUnsidelinePlugin(unsidelineContainerConfig.PluginsMeta.UnsidelinePluginMeta)
	r := mux.NewRouter()
	r.HandleFunc("/scan/{startRow}/{endRow}", scan)
	r.HandleFunc("/unsideline/{key}", unsideline)
	r.HandleFunc("/healthCheck", healthCheck)
	log.Fatal(http.ListenAndServe(":"+strconv.FormatInt(unsidelineContainerConfig.Port, 10), r))
}
