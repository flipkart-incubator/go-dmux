package metrics

type registry struct {
	provider Provider
	SourceCh chan SourceOffset
	SinkCh chan SinkOffset
	PartitionCh chan PartitionInfo
}

type OffsetInfo struct {
	Topic string
	Partition int32
	Offset int64
}

type PartitionInfo struct {
	PartitionId int32
	ConsumerId string
	Topic string
}

type LagDetail struct{
	lag int64
	partition int32
}

type (
	SinkOffset OffsetInfo
	SourceOffset OffsetInfo
)

//Provider interface that implements metric registry types
type Provider interface {
	Init()
	Ingest(interface{})
}

var (
	Registry registry
	MetricPort int
	//ThrottleChs for sending signals to source for throttling
	ThrottleChs map[string]chan bool
	//LagTh is the upper limit for lag post which source would be throttled
	LagTh map[string]int64
)

//Init creates a registry and initializes the metrics based on the registry type and implementation and returns the created registry
func Init() registry{
	LagTh = make(map[string]int64)
	ThrottleChs = make(map[string]chan bool)

	pm := PrometheusMetrics{}

	reg := registry{provider: &pm, SourceCh: make(chan SourceOffset, 1), SinkCh: make(chan SinkOffset, 1), PartitionCh: make(chan PartitionInfo)}
	reg.provider.Init()
	return reg
}

//TrackMetrics reads from various channels and calls ingest method updating the metrics
func (reg *registry) TrackMetrics(){
	for{
		select {
		case info := <- reg.SourceCh:
			reg.ingest(info)
		case info := <- reg.SinkCh:
			reg.ingest(info)
		case info := <- reg.PartitionCh:
			reg.ingest(info)
		}
	}
}

//Ingest calls the ingest method of the provider which is implementation by a metric registry type and forwards the metric
func (reg *registry) ingest(metric interface{}){
	switch metric.(type) {
	case SourceOffset:
		reg.provider.Ingest(metric.(SourceOffset))
	case SinkOffset:
		reg.provider.Ingest(metric.(SinkOffset))
	case PartitionInfo:
		reg.provider.Ingest(metric.(PartitionInfo))
	}
}
