package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"time"
)

type PluginMeta struct {
	Prefix string `json:"prefix"`
	Id     string `json:"id"`
	Path   string `json:"path"`
}

type PluginsMeta struct {
	ScanPluginMeta       PluginMeta `json:"scanPluginMeta"`
	UnsidelinePluginMeta PluginMeta `json:"unsidelinePluginMeta"`
}

type UnsidelineContainerConfig struct {
	PluginsMeta PluginsMeta `json:"pluginsMeta"`
	Port        int64       `json:"port"`
}

type UnsidelineConfig struct {
	FilePath string
}

func (unsidelineConfig UnsidelineConfig) getUnsidelineContainerConfig() UnsidelineContainerConfig {
	raw, err := ioutil.ReadFile(unsidelineConfig.FilePath)
	if err != nil {
		time.Sleep(60 * time.Second)
		log.Fatal(err.Error())
	}
	var conf UnsidelineContainerConfig
	confSerdeErr := json.Unmarshal(raw, &conf)
	if confSerdeErr != nil {
		log.Fatal(confSerdeErr.Error())
	}
	return conf
}
