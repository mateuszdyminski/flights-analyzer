package feeder

import (
	"fmt"
	"log"

	"github.com/jmcvetta/neoism"
)

// NeoDao struct for accessing neo4j db.
type neoDao struct {
	db *neoism.Database
}

// NewGraphDao creates new access struct for manipulating neo4j db.
func NewGraphDao(address string) GraphDao {
	db, err := neoism.Connect(address)
	if err != nil {
		log.Fatalf("Can't open neo4j db: %v", err)
	}

	return &neoDao{db: db}
}

// Insert inserts airport node.
func (d *neoDao) Insert(a *Airport) error {
	n, err := d.db.CreateNode(neoism.Props{"name": a.Name, "iata": a.Iata, "lat": a.Lat, "long": a.Long, "country": a.Country.Code})
	if err != nil {
		return err
	}

	a.ID = n.Id()
	return nil
}

// InsertRoutes inserts all destinations as edges.
func (d *neoDao) InsertRoutes(id int, routes []Route) error {
	n, err := d.db.Node(id)
	if err != nil {
		return fmt.Errorf("can't find node(%d), err: %v", err)
	}

	for _, f := range routes {
		if _, err := n.Relate("route", f.To.ID, neoism.Props{}); err != nil {
			return fmt.Errorf("can't create relation between(%d %d), err: %v", n.Id(), f.To.ID, err)
		}
	}

	return nil
}

// InsertRoutes inserts all flights as edges.
func (d *neoDao) InsertFlights(id int, flights Flights) error {
	n, err := d.db.Node(id)
	if err != nil {
		return fmt.Errorf("can't find node(%d), err: %v", err)
	}

	for _, f := range flights.Flights {
		if _, err := n.Relate("flight", f.Outbound.To.ID,
			neoism.Props{
				"originalPrice": f.Outbound.Price.Value,
				"price":         f.Outbound.Price.ValueExchanged,
				"currency":      f.Outbound.Price.Currency,
				"dateFrom":      f.Outbound.DateFrom,
				"dateTo":        f.Outbound.DateTo,
			}); err != nil {
			return fmt.Errorf("can't create relation between(%d %d), err: %v", n.Id(), f.Outbound.To.ID, err)
		}
	}

	return nil
}

// ClearDB deletes all flights and edges.
func (d *neoDao) ClearDB() error {
	cq1 := neoism.CypherQuery{
		Statement: `
			MATCH (n)
			OPTIONAL MATCH (n)-[r]-()
			DELETE n,r
		`,
	}

	return d.db.Cypher(&cq1)
}
