package plugins

type ScanWithStartTimeEndTimeRequest struct {
	StartTime int64
	EndTime   int64
	StartKey  string
	EndKey    string
}
