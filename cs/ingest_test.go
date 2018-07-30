package main

import (
	"github.com/nuclio/nuclio-sdk-go"
	"github.com/nuclio/nuclio-test-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"testing"
	"time"
)

type mockAppender struct {
	mock.Mock
}

func (m *mockAppender) Add(l utils.Labels, t int64, v float64) (uint64, error) {
	m.Called(l, t, v)
	return 0, nil
}

func (m *mockAppender) AddFast(l utils.Labels, ref uint64, t int64, v float64) error {
	m.Called(l, ref, t, v)
	return nil
}

func (m *mockAppender) WaitForCompletion(timeout time.Duration) (int, error) {
	m.Called(timeout)
	return 0, nil
}

func (m *mockAppender) Commit() error {
	m.Called()
	return nil
}

func (m *mockAppender) Rollback() error {
	m.Called()
	return nil
}

func TestEvent(t *testing.T) {
	var m = map[string]nuclio.DataBinding{"db0": (*v3io.Container)(nil)}
	nuclioContextP := &nuclio.Context{DataBinding: m}
	mockAppender := mockAppender{}
	var appender tsdb.Appender = &mockAppender
	nuclioContextP.UserData = &userData{
		appender: &appender,
	}
	testEvent := nutest.TestEvent{
		Body: []byte(eventExample),
	}
	expectedLabels := utils.Labels{
		utils.Label{Name: "__name__", Value: "proc.net.bytes"},
		utils.Label{Name: "bond", Value: "trade0"},
		utils.Label{Name: "bondstatus", Value: "standby"},
		utils.Label{Name: "cati_id", Value: "ICTO-29094"},
		utils.Label{Name: "direction", Value: "out"},
		utils.Label{Name: "envir", Value: "prod"},
		utils.Label{Name: "host", Value: "scl06a-0001"},
		utils.Label{Name: "iface", Value: "p1p1"},
	}
	mockAppender.On(
		"Add",
		expectedLabels,
		int64(1532091609000),
		float64(2040798464615),
	)
	result, err := Handler(nuclioContextP, &testEvent)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, "", result)
}

func TestBadRequest(t *testing.T) {
	var m = map[string]nuclio.DataBinding{"db0": (*v3io.Container)(nil)}
	nuclioContextP := &nuclio.Context{DataBinding: m}
	mockAppender := mockAppender{}
	var appender tsdb.Appender = &mockAppender
	nuclioContextP.UserData = &userData{
		appender: &appender,
	}
	testEvent := nutest.TestEvent{}
	result, err := Handler(nuclioContextP, &testEvent)
	if err != nil {
		t.Error(err)
	}
	response := result.(nuclio.Response)
	assert.Equal(t, 400, response.StatusCode)
}
