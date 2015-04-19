package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

const (
	// AirportsURL ryanair airports rest api endpoint.
	AirportsURL = "http://www.ryanair.com/en/api/2/airports/"

	// DestinationsURL ryanair destinations api %s describes origin ot the flight.
	DestinationsURL = "http://www.ryanair.com/en/api/2/routes/%s/"
)

func main() {

	req, err := http.NewRequest("GET", AirportsURL, nil)
	if err != nil {
		log.Fatalf("Can't create request: %v", err)
	}

	req.Close = true
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("Can't get response: %v", err)
	}
	defer resp.Body.Close()

	if resp.Status != "200 OK" {
		log.Fatalf("Wrong http status: %s", resp.Status)
	}

	airports := new([]Airport)
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(airports)
	if err != nil {
		log.Fatalf("Can't decode airports: %v", err)
	}

	log.Printf("Fetched %d airports \n", len(*airports))

	var allDestinations []Route

	// for each airport get all destinations !
	for _, a := range *airports {
		req, err = http.NewRequest("GET", fmt.Sprintf(DestinationsURL, a.Iata), nil)
		if err != nil {
			log.Fatalf("Can't create request: %v", err)
		}
		req.Close = true

		resp, err := client.Do(req)
		if err != nil {
			log.Fatalf("Can't get response: %v", err)
		}
		defer resp.Body.Close()

		if resp.Status != "200 OK" {
			log.Fatalf("Wrong http status: %s", resp.Status)
		}

		destinations := new([]Route)
		dec := json.NewDecoder(resp.Body)
		err = dec.Decode(destinations)
		if err != nil {
			log.Fatalf("Can't decode destinations: %v", err)
		}

		allDestinations = append(allDestinations, *destinations...)
		log.Printf("Fetched %d destinations for airport %s-%s \n", len(*destinations), a.Iata, a.Name)
	}

	log.Printf("Total fetched %d destinations \n", len(allDestinations))
}

// Airport holds info about airport details.
type Airport struct {
	Iata    string  `json:"iataCode,omitempty"`
	Name    string  `json:"name,omitempty"`
	Lat     float64 `json:"latitude,omitempty"`
	Long    float64 `json:"longitude,omitempty"`
	Country Country `json:"country,omitempty"`
}

// Country holds info about country details.
type Country struct {
	Code     string `json:"code,omitempty"`
	Name     string `json:"name,omitempty"`
	Currency string `json:"currency,omitempty"`
}

// Route holds info about flight.
type Route struct {
	IataFrom string `json:"airportFrom,omitempty"`
	IataTo   string `json:"airportTo,omitempty"`
}
