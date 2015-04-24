package feeder

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
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

	// MaxPrice max price for flight query - set high.
	MaxPrice = 5000

	// FlightsPerPage number of flights per page.
	FlightsPerPage = 15

	// ExchangeRateURL yahoo finance exchange rate api URL.
	// Replace $$$$ by currency pair eg. EURPLN.
	ExchangeRateURL = "https://query.yahooapis.com/v1/public/yql?q=select+%2A+from+yahoo.finance.xchange+where+pair+in+(\"$$$$\")&format=json&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys "
)

// GraphDao data access contract.
type GraphDao interface {
	Insert(*Airport) error
	InsertRoutes(int, []Route) error
	InsertFlights(int, Flights) error
	ClearDB() error
}

// RyanairFeeder holds info about feed.
type RyanairFeeder struct {
	mu           *sync.Mutex
	airports     map[string]Airport
	destinations []Route
	client       *http.Client
	rates        map[string]float64
	dao          GraphDao
	c            Config
}

// NewFeeder returns new instance of ryanair graph db feeder.
func NewFeeder(config Config) *RyanairFeeder {
	return &RyanairFeeder{client: &http.Client{}, mu: &sync.Mutex{}, c: config, dao: NewGraphDao(config.DBAddress)}
}

// Start starts graph db feedning.
func (f *RyanairFeeder) Start() {
	if f.c.ClearDb {
		if err := f.clearDB(); err != nil {
			log.Fatalf("Can't clear db: %v", err)
		}
	}

	if err := f.fetchAirports(); err != nil {
		log.Fatalf("Can't fetch airports: %v", err)
	}

	if err := f.fetchRoutes(); err != nil {
		log.Fatalf("Can't fetch routes: %v", err)
	}

	if err := f.fetchExchangeRates(); err != nil {
		log.Fatalf("Can't fetch exchange rates: %v", err)
	}

	if err := f.fetchPrices(); err != nil {
		log.Fatalf("Can't fetch routes: %v", err)
	}
}

// fetchAirports fetches all airports available on ryanair rest api.
func (f *RyanairFeeder) fetchAirports() error {

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
		if err = f.dao.Insert(&a); err != nil {
			return fmt.Errorf("can't insert airport(%s) to db: %v", a.Iata, err)
		}

		f.airports[a.Iata] = a
	}

	return nil
}

// fetchRoutes fetches all routes available for each airport.
func (f *RyanairFeeder) fetchRoutes() error {
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
	wg.Add(f.c.WorkersNo)
	for i := 0; i < f.c.WorkersNo; i++ {
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

		if resp.Status != "200 OK" {
			resp.Body.Close()
			errc <- fmt.Errorf("Wrong http status: %s", resp.Status)
			break
		}

		dec := json.NewDecoder(resp.Body)
		err = dec.Decode(&a.Destinations)
		if err != nil {
			errc <- fmt.Errorf("Can't decode destinations: %v", err)
			break
		}
		resp.Body.Close()

		for i := 0; i < len(a.Destinations); i++ {
			a.Destinations[i].From = f.airports[a.Iata]

			_, ok := f.airports[a.Destinations[i].IataFrom]
			if ok {
				a.Destinations[i].To = f.airports[a.Destinations[i].IataTo]
			}
		}

		err = f.dao.InsertRoutes(a.ID, a.Destinations)
		if err != nil {
			errc <- fmt.Errorf("Can't insert route: %v", err)
		}

		f.mu.Lock()
		f.airports[a.Iata] = a
		f.destinations = append(f.destinations, a.Destinations...)
		f.mu.Unlock()

		c <- fmt.Sprintf("Fetched %d destinations for airport %s-%s \n", len(a.Destinations), a.Iata, a.Name)
	}
}

// fetchPrices fetches prices for all routes available.
func (f *RyanairFeeder) fetchPrices() error {
	days := genDays(f.c.From, f.c.To)
	routes := make(chan Route)
	go func() {
		defer close(routes)
		for _, r := range f.destinations {
			routes <- r
		}
	}()

	// start a fixed number of workers.
	c := make(chan FetchResult)
	errc := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(f.c.WorkersNo)
	for i := 0; i < f.c.WorkersNo; i++ {
		go func() {
			f.fetchPricesInWorker(routes, c, errc, days)
			wg.Done()
		}()
	}

	// close change when all workes done their job.
	go func() {
		wg.Wait()
		close(c)
		close(errc)
	}()

	var counter, errors int
	// go through all results in channel.
	for result := range c {
		counter += result.No
		log.Printf(result.Msg)
	}

	var err error
	for e := range errc {
		errors++
		log.Printf("Error during fetching prices. Err: %v\n", e)
		err = e
	}

	log.Printf("Total fetched %d prices, errors %d \n", counter, errors)

	if err != nil {
		return err
	}

	return nil
}

// genDays generates dates day by day from 'form' date until 'to' date.
func genDays(from, to time.Time) []time.Time {
	duration := to.Sub(from)

	var result []time.Time
	for i := 0; i <= int(duration.Hours()/24); i++ {
		result = append(result, from.Add(time.Duration(i)*time.Duration(24*time.Hour)))
	}

	return result
}

func (f *RyanairFeeder) fetchPricesInWorker(routes <-chan Route, c chan<- FetchResult, errc chan<- error, t []time.Time) {
	for r := range routes {
		for _, tm := range t {
			req, err := http.NewRequest("GET", fmt.Sprintf(FlightsURL, r.From.Iata, r.To.Iata, tm.Format("2006-01-02"), tm.Format("2006-01-02"), MaxPrice, FlightsPerPage, 0), nil)
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

			if resp.Status != "200 OK" {
				resp.Body.Close()
				errc <- fmt.Errorf("Wrong http status: %s", resp.Status)
				break
			}

			flights := new(Flights)
			dec := json.NewDecoder(resp.Body)
			err = dec.Decode(flights)
			if err != nil {
				errc <- fmt.Errorf("Can't decode destinations: %v", err)
				break
			}
			resp.Body.Close()

			for i := 0; i < len(flights.Flights); i++ {
				o := flights.Flights[i].Outbound
				rate, ok := f.rates[r.From.Country.Currency]
				if !ok {
					errc <- fmt.Errorf("Can't find currency rate for flight from %s : %v", r.From.Name, err)
					continue
				}

				o.Price.ValueExchanged = o.Price.Value * rate
				o.From.ID = r.From.ID
				o.To.ID = r.To.ID

				flights.Flights[i].Outbound = o
			}

			if err = f.dao.InsertFlights(r.From.ID, *flights); err != nil {
				errc <- fmt.Errorf("Can't insert flights from %s : %v", r.From.Name, err)
				break
			}

			c <- FetchResult{Msg: fmt.Sprintf("Fetched %d flights details from airport %s-%s \n", len(flights.Flights), r.From.Iata, r.From.Name), No: len(flights.Flights)}
		}
	}
}

// fetchExchangeRates gets all needed exchange rates.
func (f *RyanairFeeder) fetchExchangeRates() error {
	f.rates = make(map[string]float64, len(f.c.Currencies))
	for _, c := range f.c.Currencies {
		req, err := http.NewRequest("GET", strings.Replace(ExchangeRateURL, "$$$$", c.Name+f.c.Currency, 1), nil)
		if err != nil {
			return fmt.Errorf("can't create request: %v", err)
		}
		req.Close = true

		resp, err := f.client.Do(req)
		if err != nil {
			return fmt.Errorf("can't get response: %v", err)
		}

		if resp.Status != "200 OK" {
			resp.Body.Close()
			return fmt.Errorf("wrong http status: %s", resp.Status)
		}

		rate := new(ExchangeQuery)
		dec := json.NewDecoder(resp.Body)
		err = dec.Decode(rate)
		if err != nil {
			resp.Body.Close()
			return fmt.Errorf("can't decode exchange rate: %v", err)
		}
		resp.Body.Close()

		fmt.Printf("Got exchange rate %s for pair %s-%s \n", rate.Result.Result.Rate.Rate, c.Name, f.c.Currency)
		val, err := strconv.ParseFloat(rate.Result.Result.Rate.Rate, 64)
		if err != nil {
			return fmt.Errorf("can't parse exchange rate: %v", err)
		}

		f.rates[c.Name] = val
	}

	return nil
}

// clearDB clears all nodes and edges in graph db.
func (f *RyanairFeeder) clearDB() error {
	return f.dao.ClearDB()
}

// Airport holds info about airport details.
type Airport struct {
	ID           int
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

// Route holds info about route.
type Route struct {
	IataFrom string `json:"airportFrom"`
	IataTo   string `json:"airportTo"`
	From     Airport
	To       Airport
}

// Flights all flights returned from query.
type Flights struct {
	Flights []Flight `json:"flights"`
}

// Flight holds info about single flight.
type Flight struct {
	Outbound Outbound `json:"outbound"`
}

// Outbound holds outbound flight details.
type Outbound struct {
	From     SimpleAiport `json:"airportFrom"`
	To       SimpleAiport `json:"airportTo"`
	Price    Price        `json:"price"`
	DateFrom time.Time    `json:"dateFrom"`
	DateTo   time.Time    `json:"dateTo"`
}

// SimpleAiport short list of airports details.
type SimpleAiport struct {
	ID   int
	Iata string `json:"iataCode"`
	Name string `json:"name"`
}

// Price holds info about flight price details.
type Price struct {
	Value          float64 `json:"value"`
	ValueExchanged float64
	Currency       string `json:"currencySymbol"`
}

// Currency holds info about specified currency.
type Currency struct {
	Name string `json:"name"`
}

// ExchangeQuery holds info about rate of currencies pair.
type ExchangeQuery struct {
	Result ExchangeResults `json:"query"`
}

// ExchangeResults holds info about rate of currencies pair.
type ExchangeResults struct {
	Result ExchangeResult `json:"results"`
}

// ExchangeResult holds info about rate of currencies pair.
type ExchangeResult struct {
	Rate Rate `json:"rate"`
}

// Rate holds info about rate of currencies pair.
type Rate struct {
	ID   string `json:"id"`
	Name string `json:"Name"`
	Rate string `json:"Rate"`
}

// Config holds info about flights feeder.
type Config struct {
	DBAddress  string     `json:"neo4jAddress"`
	WorkersNo  int        `json:"workersNo"`
	ClearDb    bool       `json:"clearDb"`
	Currency   string     `json:"currency"`
	From       time.Time  `json:"from"`
	To         time.Time  `json:"to"`
	Currencies []Currency `json:"currencies"`
}

// FetchResult tuple describes how many flights were fetched.
type FetchResult struct {
	No  int
	Msg string
}
