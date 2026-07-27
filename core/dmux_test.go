package core

import (
	"hash/fnv"
	"log"
)

// MockSource and MockSink used for testing
type MockData struct {
	key     string
	version int
}

func GetMockData(key string, version int) MockData {
	return MockData{key, version}
}

type MockSource struct {
	status int
}

var testData = map[string]*[2]int{
	"OD1":  {100, 0},
	"OD2":  {51, 0},
	"OD3":  {72, 0},
	"OD4":  {11, 0},
	"OD5":  {49, 0},
	"OD6":  {44, 0},
	"OD7":  {121, 0},
	"OD8":  {99, 0},
	"OD9":  {28, 0},
	"OD10": {33, 0},
}

func GetMockSource() *MockSource {
	source := &MockSource{}
	source.status = 1
	return source
}

func (m *MockSource) Generate(ch chan<- interface{}) {
	count := 0
	for count < 10 {
		for key := range testData {
			// log.Println(m.status)
			val := testData[key]
			if m.status != 1 {
				log.Println("Break From Generate")
				return
			}
			th := val[0]
			if val[1] > th {
				continue
			}
			ch <- MockData{key, val[1]}
			val[1]++
			if val[1] > th {
				count++
			}
		}
	}
	// for _, data := range m.sourceData {
	// 	if m.status != 1 {
	// 		log.Println("Breaking")
	// 		break
	// 	}
	// 	ch <- data
	// }
}

func (m *MockSource) Stop() {
	m.status = 2
	log.Println(m.status)
}

type MockSink struct {
	sinks  []*MockSink
	buffer map[string]MockData
}

func (m *MockSink) Clone() Sink {
	sink := &MockSink{
		buffer: make(map[string]MockData),
	}
	m.sinks = append(m.sinks, sink)
	return sink
}

func (m *MockSink) Consume(msg interface{}) {
	data := msg.(MockData)
	m.buffer[data.key] = data
}

func (m *MockSink) BatchConsume(msgs []interface{}, version int) {
	for _, msg := range msgs {
		m.Consume(msg)
	}
}

// Method added for testing
func (m *MockSink) MergeAndGetOutput() map[string]MockData {
	output := make(map[string]MockData)
	for _, s := range m.sinks {
		for k, val := range s.buffer {
			output[k] = val
		}
	}
	return output
}

type MockDataHasher struct{}

func (m *MockDataHasher) ComputeHash(data interface{}) int {
	val := data.(MockData)
	h := fnv.New32a()
	h.Write([]byte(val.key))
	return int(h.Sum32())
}

/*
func TestConsistenHashDmuxHappyCase(t *testing.T) {
	log.Println("running test TestConsistenHashDmuxHappyCase")
	hasher := new(MockDataHasher)
	d := GetHashDistribution(hasher)
	size := 4

	source := GetMockSource()
	sink := new(MockSink)
	dmux := GetDmux(size, d)
	dmux.Connect(source, sink)
	dmux.Await(10 * time.Second)
	// dmux.Stop()

	// output := sink.MergeAndGetOutput()
	// for key, val := range output {
	// 	expected := testData[key][0]
	// 	if val.version != expected {
	// 		t.Errorf("for %s expected version %d got %d", key, expected, val.version)
	// 	}
	// }
	// dmux.Await(1 * time.Second)

}

func TestConsistenHashDmuxWithResize(t *testing.T) {
	hasher := new(MockDataHasher)
	d := GetHashDistribution(hasher)
	size := 4

	source := GetMockSource()
	sink := new(MockSink)
	dmux := GetDmux(size, d)
	dmux.Connect(source, sink)
	dmux.Await(1 * time.Second)
	dmux.Resize(10)
	// dmux.Await(9 * time.Second)
	// dmux.Stop()

	// output := sink.MergeAndGetOutput()
	// for key, val := range output {
	// 	expected := testData[key][0]
	// 	if val.version != expected {
	// 		t.Errorf("for %s expected version %d got %d", key, expected, val.version)
	// 	}
	// }
}
*/
