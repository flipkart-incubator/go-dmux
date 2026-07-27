package sideline

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"log"
	"os"
	"net/http"
	"strconv"
)

var scanImpl Scan
var unsidelineImpl Unsideline

func scan(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	var request = ScanWithStartRowEndRowRequest{
		StartKey: vars["startRow"],
		EndKey:   vars["endRow"],
	}
	rows, err := scanImpl.ScanWithStartRowEndRow(request)
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
	var request = UnsidelineByKeyRequest{
		Key: vars["key"],
	}
	rows, err := unsidelineImpl.UnsidelineByKey(request)
	if err != nil {
		log.Printf("Received error while executing plugin \n")
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

func UnsidelineStart(scanImplArg Scan, unsidelineImplArg Unsideline, configPath string) {
	log.Println("Hi starting the API")

	raw, err := os.ReadFile(configPath)
	if err != nil {
		log.Fatal(err.Error())
	}
	var conf UnsidelineContainerConfig
	confSerdeErr := json.Unmarshal(raw, &conf)
	if confSerdeErr != nil {
		log.Fatal(confSerdeErr.Error())
	}
	scanImpl = scanImplArg
	unsidelineImpl = unsidelineImplArg
	r := mux.NewRouter()
	r.HandleFunc("/scan/{startRow}/{endRow}", scan)
	r.HandleFunc("/unsideline/{key}", unsideline)
	r.HandleFunc("/healthCheck", healthCheck)
	log.Fatal(http.ListenAndServe(":"+strconv.FormatInt(conf.Port, 10), r))
}

type Scan interface {
	ScanWithStartRowEndRow(request ScanWithStartRowEndRowRequest) ([]string, error)
	ScanWithStartTimeEndTime(request ScanWithStartTimeEndTimeRequest) ([]string, error)
}

type Unsideline interface {
	UnsidelineByKey(request UnsidelineByKeyRequest) (string, error)
}

type ScanWithStartRowEndRowRequest struct {
	StartKey string
	EndKey   string
}

type ScanWithStartTimeEndTimeRequest struct {
	StartTime int64
	EndTime   int64
	StartKey  string
	EndKey    string
}

type UnsidelineByKeyRequest struct {
	Key      string
	DmuxItem string
}

type UnsidelineContainerConfig struct {
	Port int64 `json:"port"`
}
