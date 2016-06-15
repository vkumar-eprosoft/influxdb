package statement

import (
	"fmt"
	"strings"

	"github.com/influxdata/influxdb/stress/v2/stressClient"
)

// SetStatement set state variables for the test
type SetStatement struct {
	Var   string
	Value string

	StatementID string

	Tracer *stressClient.Tracer
}

// SetID statisfies the Statement Interface
func (i *SetStatement) SetID(s string) {
	i.StatementID = s
}

// Run statisfies the Statement Interface
func (i *SetStatement) Run(s *stressClient.StoreFront) {

	// Set the Tracer
	i.Tracer = stressClient.NewTracer(make(map[string]string))

	// Create a new Directive
	d := stressClient.NewDirective(strings.ToLower(i.Var), strings.ToLower(i.Value), i.Tracer)

	switch d.Property {

	// Needs to be set on both StoreFront and stressClient
	// Set the write percison for points generated
	case "precision":
		s.Precision = d.Value

		// Increment the tracer
		i.Tracer.Add(1)
		s.SendDirective(d)

	// Lives on StoreFront
	// Set the date for the first point entered into the database
	case "startdate":
		s.Lock()
		s.StartDate = d.Value
		s.Unlock()

	// Lives on StoreFront
	// Set the BatchSize for writes
	case "batchsize":
		s.Lock()
		s.BatchSize = parseInt(d.Value)
		s.Unlock()

	// All other variables live on stressClient
	default:
		// Increment the tracer
		i.Tracer.Add(1)
		s.SendDirective(d)
	}
	i.Tracer.Wait()
}

// Report statisfies the Statement Interface
func (i *SetStatement) Report(s *stressClient.StoreFront) string {
	return fmt.Sprintf("SET %v = '%v'", i.Var, i.Value)
}
