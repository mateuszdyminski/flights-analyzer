package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
)

const (
	// AirportsURL ryanair airports rest api endpoint.
	AirportsURL = "http://www.ryanair.com/en/api/2/airports/"

	// DestinationsURL ryanair destinations api %s describes origin ot the flight.
	DestinationsURL = "http://www.ryanair.com/en/api/2/routes/%s/"

	// FlightsURL ryanair flights api URL.
	// First %s - IATA from.
	// Second %s - IATA to.
	// Third %s - Date since.
	// 4th %s - Date until.
	// 5th %d - Max price in local currency.
	// 6th %d - Limit of the results.
	// 7th %d - Offset.
	FlightsURL = "http://www.ryanair.com/pl/api/2/flights/from/%s/to/%s/%s/%s/%d/unique/?limit=%d&offset=%d"

	// WorkersNo number of parallel workers.
	WorkersNo = 10
)

// Feeder interface describes basic method available for airports feeder.
type Feeder interface {
	FetchAirports() error
	FetchRoutes() error
}

// RyanairFeeder holds info about feed.
type RyanairFeeder struct {
	mu           *sync.Mutex
	airports     map[string]Airport
	destinations []Route
	client       *http.Client
}

func main() {
	f := RyanairFeeder{client: &http.Client{}, mu: &sync.Mutex{}}

	if err := f.FetchAirports(); err != nil {
		log.Fatalf("Can't fetch airports: %v", err)
	}

	if err := f.FetchRoutes(); err != nil {
		log.Fatalf("Can't fetch routes: %v", err)
	}

}

// FetchAirports fetches all airports available on ryanair rest api.
func (f *RyanairFeeder) FetchAirports() error {

	req, err := http.NewRequest("GET", AirportsURL, nil)
	if err != nil {
		return fmt.Errorf("can't create request: %v", err)
	}

	req.Close = true
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("can't get response: %v", err)
	}
	defer resp.Body.Close()

	if resp.Status != "200 OK" {
		return fmt.Errorf("wrong http status: %s", resp.Status)
	}

	var airports []Airport
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&airports)
	if err != nil {
		return fmt.Errorf("can't decode airports: %v", err)
	}

	log.Printf("Fetched %d airports \n", len(airports))

	f.airports = make(map[string]Airport, len(airports))
	for _, a := range airports {
		f.airports[a.Iata] = a
	}

	return nil
}

// FetchRoutes fetches all routes available for each airport.
func (f *RyanairFeeder) FetchRoutes() error {
	// create airports chan
	airports := make(chan Airport)
	go func() {
		// close the airports channel after all.
		defer close(airports)
		for _, a := range f.airports {
			airports <- a
		}
	}()

	// start a fixed number of workers.
	c := make(chan string)
	errc := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(WorkersNo)
	for i := 0; i < WorkersNo; i++ {
		go func() {
			f.fetchAirportRoutes(airports, c, errc)
			wg.Done()
		}()
	}

	// close change when all workes done their job.
	go func() {
		wg.Wait()
		close(c)
		close(errc)
	}()

	// go through all results in channel.
	for result := range c {
		log.Printf(result)
	}

	var err error
	for e := range errc {
		log.Printf("Error during fetching routes. Err: %v\n", e)
		err = e
	}

	if err != nil {
		return err
	}

	log.Printf("Total fetched %d destinations \n", len(f.destinations))

	return nil
}

func (f *RyanairFeeder) fetchAirportRoutes(airports <-chan Airport, c chan<- string, errc chan<- error) {
	for a := range airports {
		req, err := http.NewRequest("GET", fmt.Sprintf(DestinationsURL, a.Iata), nil)
		if err != nil {
			errc <- fmt.Errorf("Can't create request: %v", err)
			break
		}
		req.Close = true

		resp, err := f.client.Do(req)
		if err != nil {
			errc <- fmt.Errorf("Can't get response: %v", err)
			break
		}
		defer resp.Body.Close()

		if resp.Status != "200 OK" {
			errc <- fmt.Errorf("Wrong http status: %s", resp.Status)
			break
		}

		dec := json.NewDecoder(resp.Body)
		err = dec.Decode(&a.Destinations)
		if err != nil {
			errc <- fmt.Errorf("Can't decode destinations: %v", err)
			break
		}

		f.mu.Lock()
		f.airports[a.Iata] = a
		f.destinations = append(f.destinations, a.Destinations...)
		f.mu.Unlock()

		c <- fmt.Sprintf("Fetched %d destinations for airport %s-%s \n", len(a.Destinations), a.Iata, a.Name)
	}
}

// Airport holds info about airport details.
type Airport struct {
	Iata         string  `json:"iataCode"`
	Name         string  `json:"name"`
	Lat          float64 `json:"latitude"`
	Long         float64 `json:"longitude"`
	Country      Country `json:"country"`
	Destinations []Route
}

// Country holds info about country details.
type Country struct {
	Code     string `json:"code"`
	Name     string `json:"name"`
	Currency string `json:"currency"`
}

// Route holds info about flight.
type Route struct {
	IataFrom string `json:"airportFrom"`
	IataTo   string `json:"airportTo"`
}
