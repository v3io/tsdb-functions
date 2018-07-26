package main

import (
	"encoding/json"
	"github.com/nuclio/nuclio-sdk-go"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"sort"
	"sync"
)

// Configuration
// Note: the TSDB (path) must be first created using the CLI or API
// the user must also define the v3io data binding in the nuclio function with path, username, password and name it db0
const tsdbConfig = `
path: "pmetric"
`

// example event
const eventExample = `
{
	"Metric": "cpu",
	"Labels": {
		"dc": "7",
		"hostname": "mybesthost"
	},
	"Samples": [
		{
			"Time": "1532595945142",
			"Value": {
				"N": 95.2
			}
		},
		{
			"Time": "1532595948517",
			"Value": {
				"N": 86.8
			}
		}
	]
}
`

type Value struct {
	N float64
}

type sample struct {
	Time  string `json:"Time,omitempty"`
	Value Value  `json:"Value,omitempty"`
}

type request struct {
	Metric  string            `json:"Metric"`
	Labels  map[string]string `json:"Labels,omitempty"`
	Samples []sample          `json:"Samples"`
}

type userData struct {
	appender *tsdb.Appender
	request  request
	lset     utils.Labels
}

var adapter *tsdb.V3ioAdapter
var adapterMtx sync.RWMutex

func Handler(context *nuclio.Context, event nuclio.Event) (interface{}, error) {
	udp := context.UserData.(*userData)

	err := json.Unmarshal(event.GetBody(), &udp.request)
	if err != nil {
		return nuclio.Response{
			StatusCode:  400,
			ContentType: "application/text",
			Body:        []byte(err.Error()),
		}, nil
	}
	app := *udp.appender

	udp.lset = fromMap(udp.request.Labels, udp.lset)

	for _, s := range udp.request.Samples {
		// if time is not specified assume "now"
		if s.Time == "" {
			s.Time = "now"
		}

		// convert time string to time int, string can be: now, now-2h, int (unix milisec time), or RFC3339 date string
		t, err := utils.Str2unixTime(s.Time)
		if err != nil {
			return "", errors.Wrap(err, "Failed to parse time: "+s.Time)
		}

		// Append sample to metric
		_, err = app.Add(udp.lset, t, s.Value.N)
	}

	return "", err
}

// Populate lset from labelMap and sort it.
func fromMap(labelMap map[string]string, lset utils.Labels) utils.Labels {
	i := 0

	for k, v := range labelMap {
		if i < len(lset) {
			lset[i].Name = k
			lset[i].Value = v
		} else {
			lset = append(lset, utils.Label{Name: k, Value: v})
		}
		i++
	}
	newLset := lset[0:i]
	sort.Sort(newLset)
	return newLset
}

// InitContext runs only once when the function runtime starts
func InitContext(context *nuclio.Context) error {

	var err error
	defer adapterMtx.Unlock()
	adapterMtx.Lock()

	if adapter == nil {
		// create adapter once for all contexts
		cfg, _ := config.LoadFromData([]byte(tsdbConfig))
		data := context.DataBinding["db0"].(*v3io.Container)
		adapter, err = tsdb.NewV3ioAdapter(cfg, data, context.Logger)
		if err != nil {
			return err
		}
	}

	appender, err := adapter.Appender()
	if err != nil {
		return err
	}
	context.UserData = &userData{
		appender: &appender,
	}
	return nil
}
