package core

import (
	"hash/fnv"
	"log"
	"testing"
)

type StrData string

func (s StrData) ComputeHash(data interface{}) int {
	val := data.(StrData)
	h := fnv.New32a()
	h.Write([]byte(val))
	return int(h.Sum32())
}

func TestStringDistribution(t *testing.T) {
	log.Println("running test TestStringDistribution")
	data := [5]StrData{"First", "Second", "Third", "Fourth", "Fifth"}
	hashIndex := [5]int{7, 9, 0, 9, 4}
	size := 10
	for i, val := range data {
		hd := GetHashDistribution(val)
		expected := hashIndex[i]
		actual := hd.Distribute(val, size)
		if expected != actual {
			t.Errorf("expected %d got %d \n", expected, actual)
		}
	}
}

type MsgData struct {
	key string
}

func (m MsgData) ComputeHash(data interface{}) int {
	val := data.(MsgData)
	h := fnv.New32a()
	h.Write([]byte(val.key))
	return int(h.Sum32())
}
func GetMsg(key string) MsgData {
	return MsgData{key}
}
func TestMsgDistribution(t *testing.T) {
	log.Println("running test TestMsgDistribution")
	data := [5]MsgData{
		GetMsg("First"),
		GetMsg("Second"),
		GetMsg("Third"),
		GetMsg("Fourth"),
		GetMsg("Fifth"),
	}
	hashIndex := [5]int{7, 9, 0, 9, 4}
	size := 10
	for i, val := range data {
		hd := GetHashDistribution(val)
		expected := hashIndex[i]
		actual := hd.Distribute(val, size)
		if expected != actual {
			t.Errorf("expected %d got %d \n", expected, actual)
		}
	}
}
