package metrics

import (
	"flag"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"net/http"
	"strconv"
)

//pusher is an interface where the methods are implemented by the metric type
type pusher interface {
	push(p *PrometheusMetrics)
}

//lagUpdater is an interface where the methods are implemented by source and sink offset metric for
//keeping track of the lag
type lagUpdater interface {
	updateLag(p *PrometheusMetrics)
}

type PrometheusMetrics struct {
	//keep track of last source and sink details for lag calculation
	LastSourceDetail map[string]map[int32]int64
	LastSinkDetail map[string]map[int32]int64

	//maxLag is the maximum lag across partitions for a topic
	maxLag map[string]LagDetail

	//to maintain the state of the source
	isThrottled map[string]bool

	//metric collectors
	sourceOffMetric *prometheus.GaugeVec
	sinkOffMetric *prometheus.GaugeVec
	lagOffMetric *prometheus.GaugeVec
	partitionOwned *prometheus.GaugeVec
}

func (p *PrometheusMetrics) Init(){
	go p.displayMetrics()

	p.LastSinkDetail = make(map[string]map[int32]int64)
	p.LastSourceDetail = make(map[string]map[int32]int64)
	p.maxLag = make(map[string]LagDetail)
	p.isThrottled = make(map[string]bool)

	p.createMetrics()
	p.registerMetrics()
}

//Ingest metrics as and when events are received from the channels
func (p *PrometheusMetrics) Ingest(metric interface{}){
		switch metric.(type) {
		case SourceOffset:
			m := metric.(SourceOffset)
			m.push(p)
		case SinkOffset:
			m := metric.(SinkOffset)
			m.push(p)
		case PartitionInfo:
			m := metric.(PartitionInfo)
			m.push(p)
		}
}

//The metrics can be fetched by a Get request from the http://localhost:9999/metrics end point
func  (p *PrometheusMetrics) displayMetrics() {
	addr := flag.String("listen-address", ":"+strconv.Itoa(MetricPort), "The address to listen on for HTTP requests.")
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(*addr, nil))
}

//Initialize all the metric collectors
func (p *PrometheusMetrics) createMetrics(){
	p.sourceOffMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "source_offset",
			Help: "Metric to represent the offset at source",
		}, []string{"topic","partition"})

	p.sinkOffMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "sink_offset",
			Help: "Metric to represent the offset at sink",
		}, []string{"topic","partition"})

	p.lagOffMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "lag",
			Help: "Metric to represent the lag between source and sink",
		}, []string{"topic","partition"})

	p.partitionOwned = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "partition_owned",
			Help: "Metric to represent the partitions owned",
		}, []string{"topic","consumerId", "partitionId"})
}

//Register the metric collectors
func (p *PrometheusMetrics) registerMetrics() {
	prometheus.MustRegister(p.sourceOffMetric)
	prometheus.MustRegister(p.sinkOffMetric)
	prometheus.MustRegister(p.lagOffMetric)
	prometheus.MustRegister(p.partitionOwned)
}

//updateMaxLag updates the maximum lag for a topic across its partitions
func (p *PrometheusMetrics) updateMaxLag(topic string, partition int32, lag int64){
	if lagDetail, ok := p.maxLag[topic]; ok == true{
		if lagDetail.partition == partition || lag > lagDetail.lag{
			p.maxLag[topic] = LagDetail{
				lag:       lag,
				partition: partition,
			}
		}
	} else {
		p.maxLag[topic] = LagDetail{
			lag:       lag,
			partition: partition,
		}
	}
}

//throttleSrc compares with the threshold and sends signals to stop the source it also keeps
//track of state of source
func (p *PrometheusMetrics) throttleSrc(topic string) {
	if p.maxLag[topic].lag >= LagTh[topic] && !p.isThrottled[topic]{
		ThrottleChs[topic] <- true
		p.isThrottled[topic] = true
	} else if p.maxLag[topic].lag < LagTh[topic] && p.isThrottled[topic] {
		ThrottleChs[topic] <- false
		p.isThrottled[topic] = false
	}
}

func (info *SourceOffset) push(p *PrometheusMetrics){
	//Push the latest source offset for the corresponding topic and partition
	p.sourceOffMetric.WithLabelValues(info.Topic, strconv.Itoa(int(info.Partition))).Set(float64(info.Offset))

	info.updateLag(p)
}

func (info *SinkOffset) push(p *PrometheusMetrics){
	//Push the latest sink offset for the corresponding topic and partition
	p.sinkOffMetric.WithLabelValues(info.Topic, strconv.Itoa(int(info.Partition))).Set(float64(info.Offset))
	info.updateLag(p)
}

func (info *PartitionInfo) push(p *PrometheusMetrics){
	//Push the time stamp at which the partition joined dmux
	p.partitionOwned.With(prometheus.Labels{"topic": info.Topic, "consumerId":info.ConsumerId, "partitionId":strconv.Itoa(int(info.PartitionId))}).SetToCurrentTime()
}

func (info *SourceOffset) updateLag(p *PrometheusMetrics){
	//Update the previous offset details at source
	if partitionDetail, ok := p.LastSourceDetail[info.Topic]; ok == true {
		partitionDetail[info.Partition] = info.Offset
		p.LastSourceDetail[info.Topic] = partitionDetail
	} else {
		partitionDetail = make(map[int32]int64)
		p.LastSourceDetail[info.Topic] = partitionDetail
	}

	//Push lag metric based on the stored sink offset
	if lastOffset, ok := p.LastSinkDetail[info.Topic][info.Partition]; ok == true{
		//calculate lag
		lag := info.Offset - lastOffset

		//update max lag and throttle source based on threshold
		p.updateMaxLag(info.Topic, info.Partition, lag)
		p.throttleSrc(info.Topic)

		//Push the lag which is source offset minus the last sink offset for the corresponding topic and partition
		p.lagOffMetric.WithLabelValues(info.Topic, strconv.Itoa(int(info.Partition))).Set(float64(lag))
	}
}

func (info *SinkOffset) updateLag(p *PrometheusMetrics) {
	//Update the previous offset details at sink
	if partitionDetail, ok := p.LastSinkDetail[info.Topic]; ok == true{
		partitionDetail[info.Partition] = info.Offset
		p.LastSinkDetail[info.Topic] = partitionDetail
	} else{
		partitionDetail = make(map[int32]int64)
		p.LastSinkDetail[info.Topic] = partitionDetail
	}

	//Push lag metric based on the stored source offset
	if lastOffset, ok := p.LastSourceDetail[info.Topic][info.Partition]; ok == true {
		//calculate lag
		lag := lastOffset - info.Offset

		//update max lag and throttle source based on threshold
		p.updateMaxLag(info.Topic, info.Partition, lag)
		p.throttleSrc(info.Topic)

		//Push the lag which is last source offset minus the sink offset for the corresponding topic and partition
		p.lagOffMetric.WithLabelValues(info.Topic, strconv.Itoa(int(info.Partition))).Set(float64(lag))
	}
}

