package main

import (
	"github.com/nuclio/nuclio-sdk-go"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"strings"
	"sync"
	"strconv"
	"sort"
)

// Configuration
// Note: the TSDB (path) must be first created using the CLI or API
// the user must also define the v3io data binding in the nuclio function with path, username, password and name it db0
const tsdbConfig = `
path: "pmetric"
`

// example event
const eventExample = `
proc.net.bytes 1532091609 2040798464615 bond=trade0 cati_id=ICTO-29094 iface=p1p1 bondstatus=standby host=scl06a-0001 envir=prod direction=out
`

type Value struct {
	N float64
}

type userData struct {
	appender *tsdb.Appender
}

var adapter *tsdb.V3ioAdapter
var adapterMtx sync.RWMutex

func Handler(context *nuclio.Context, event nuclio.Event) (interface{}, error) {
	udp := context.UserData.(*userData)

	parts := strings.Split(strings.TrimSpace(string(event.GetBody())), " ")

	if len(parts) < 3 {
		return nuclio.Response{
			StatusCode:  400,
			ContentType: "application/text",
			Body:        []byte("Not enough columns"),
		}, nil
	}

	metric := parts[0]
	time, err := strconv.ParseInt(parts[1], 10, 64)
	time *= 1000
	if err != nil {
		return nuclio.Response{
			StatusCode:  400,
			ContentType: "application/text",
			Body:        []byte("Failed to parse int"),
		}, nil
	}
	value, err := strconv.ParseFloat(parts[2], 64)
	if err != nil {
		return nuclio.Response{
			StatusCode:  400,
			ContentType: "application/text",
			Body:        []byte("Failed to parse float"),
		}, nil
	}

	var lset utils.Labels
	lset = append(lset, utils.Label{Name: "__name__", Value: metric})

	for _, part := range parts[3:] {
		index := strings.IndexByte(part, '=')
		if index < 0 {
			return nuclio.Response{
				StatusCode:  400,
				ContentType: "application/text",
				Body:        []byte("Label column missing '='"),
			}, nil
		}
		labelName := part[:index]
		labelValue := part[index+1:]
		lset = append(lset, utils.Label{Name: labelName, Value: labelValue})
	}

	sort.Sort(lset)

	app := *udp.appender

	_, err = app.Add(lset, time, value)

	return "", err
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
