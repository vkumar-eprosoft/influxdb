package ponyExpress

import (
	"fmt"
	"log"
	"sync"

	influx "github.com/influxdata/influxdb/client/v2"
)

// NewStoreFront creates the backend for the stress test
func NewStoreFront() *StoreFront {

	packageCh := make(chan Package, 0)
	directiveCh := make(chan Directive, 0)
	responseCh := make(chan Response, 0)

	clnt, _ := influx.NewHTTPClient(influx.HTTPConfig{
		Addr: fmt.Sprintf("http://%v/", "localhost:8086"),
	})

	s := &StoreFront{
		TestDB:    "_stressTest",
		Precision: "s",
		StartDate: "2016-01-02",
		BatchSize: 5000,

		packageChan:   packageCh,
		directiveChan: directiveCh,

		ResultsClient: clnt,
		ResultsChan:   responseCh,
		communes:      make(map[string]*commune),
		TestID:        randStr(10),
	}

	// Start the client service
	startPonyExpress(packageCh, directiveCh, responseCh, s.TestID)

	// Listen for Results coming in
	s.resultsListen()

	return s
}

// NewTestStoreFront returns a StoreFront to be used for testing Statements
func NewTestStoreFront() (*StoreFront, chan Package, chan Directive) {

	packageCh := make(chan Package, 0)
	directiveCh := make(chan Directive, 0)

	s := &StoreFront{
		TestDB:    "_stressTest",
		Precision: "s",
		StartDate: "2016-01-02",
		BatchSize: 5000,

		directiveChan: directiveCh,
		packageChan:   packageCh,

		communes: make(map[string]*commune),
		TestID:   randStr(10),
	}

	return s, packageCh, directiveCh
}

// The StoreFront is the Statement facing API that consumes Statement output and coordinates the test results
type StoreFront struct {
	TestID string
	TestDB string

	Precision string
	StartDate string
	BatchSize int

	sync.WaitGroup
	sync.Mutex

	packageChan   chan<- Package
	directiveChan chan<- Directive

	ResultsChan   chan Response
	communes      map[string]*commune
	ResultsClient influx.Client
}

// SendPackage is the public facing API for to send Queries and Points
func (sf *StoreFront) SendPackage(p Package) {
	sf.packageChan <- p
}

// SendDirective is the public facing API to set state variables in the test
func (sf *StoreFront) SendDirective(d Directive) {
	sf.directiveChan <- d
}

// Starts a go routine that listens for Results
func (sf *StoreFront) resultsListen() {
	sf.createDatabase(sf.TestDB)
	go func() {
		bp := sf.newResultsPointBatch()
		for resp := range sf.ResultsChan {
			switch resp.Point.Name() {
			case "done":
				sf.ResultsClient.Write(bp)
				resp.Tracer.Done()
			default:
				// Add the StoreFront tags
				pt := resp.AddTags(sf.tags())
				// Add the point to the batch
				bp = sf.batcher(pt, bp)
				resp.Tracer.Done()
			}
		}
	}()
}

// Creates a new batch of points for the results
func (sf *StoreFront) newResultsPointBatch() influx.BatchPoints {
	bp, _ := influx.NewBatchPoints(influx.BatchPointsConfig{
		Database:  sf.TestDB,
		Precision: "ns",
	})
	return bp
}

// Batches incoming Result.Point and sends them if the batch reaches 5k in size
func (sf *StoreFront) batcher(pt *influx.Point, bp influx.BatchPoints) influx.BatchPoints {
	if len(bp.Points()) <= 5000 {
		bp.AddPoint(pt)
	} else {
		err := sf.ResultsClient.Write(bp)
		if err != nil {
			log.Fatalf("Error writing performance stats\n  error: %v\n", err)
		}
		bp = sf.newResultsPointBatch()
	}
	return bp
}

// Convinence database creation function
func (sf *StoreFront) createDatabase(db string) {
	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %v", db)
	res, err := sf.ResultsClient.Query(influx.Query{Command: query})
	if err != nil {
		log.Fatalf("error: no running influx server at localhost:8086")
		if res.Error() != nil {
			log.Fatalf("error: no running influx server at localhost:8086")
		}
	}
}

// GetStatementResults is a convinence function for fetching all results given a StatementID
func (sf *StoreFront) GetStatementResults(sID, t string) (res []influx.Result) {
	qryStr := fmt.Sprintf(`SELECT * FROM "%v" WHERE statement_id = '%v'`, t, sID)
	return sf.queryTestResults(qryStr)
}

//  Runs given qry on the test results database and returns the results or nil in case of error
func (sf *StoreFront) queryTestResults(qry string) (res []influx.Result) {
	response, err := sf.ResultsClient.Query(influx.Query{Command: qry, Database: sf.TestDB})
	if err == nil {
		if response.Error() != nil {
			log.Fatalf("Error sending results query\n  error: %v\n", response.Error())
		}
	}
	if response.Results[0].Series == nil {
		return nil
	}
	return response.Results
}
