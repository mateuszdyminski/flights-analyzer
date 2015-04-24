package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"
	"runtime"

	"github.com/mateuszdyminski/flights-analyzer/feeder"
)

var configPath string
var config feeder.Config

func init() {
	flag.Usage = func() {
		flag.PrintDefaults()
	}

	flag.StringVar(&configPath, "config", "config/config.json", "Path to config file")
}

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(runtime.NumCPU())

	loadConfig()

	f := feeder.NewFeeder(config)

	f.Start()
}

// loadConfig loads json config to struct.
func loadConfig() {
	r, err := os.Open(configPath)
	if err != nil {
		log.Fatalf("Can't open config file!")
	}
	defer r.Close()
	dec := json.NewDecoder(r)
	dec.Decode(&config)

	log.Printf("Config.DBAddress: %s\n", config.DBAddress)
	log.Printf("Config.WorkersNo: %d\n", config.WorkersNo)
	log.Printf("Config.ClearDb: %t\n", config.ClearDb)
	log.Printf("Config.From: %s\n", config.From)
	log.Printf("Config.To: %s\n", config.To)
	log.Printf("Config.Currency: %s\n", config.Currency)
	log.Printf("Config.SupportedCurrencies: %v\n", config.Currencies)
}
