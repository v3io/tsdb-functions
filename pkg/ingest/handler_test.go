package main

import (
	"github.com/nuclio/nuclio-sdk-go"
	"github.com/nuclio/nuclio-test-go"
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

func TestHandler(t *testing.T) {
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
		utils.Label{Name: "dc", Value: "7"},
		utils.Label{Name: "hostname", Value: "mybesthost"},
	}
	mockAppender.On(
		"Add",
		expectedLabels,
		int64(1532595945142),
		float64(95.2),
	)
	mockAppender.On(
		"Add",
		expectedLabels,
		int64(1532595948517),
		float64(86.8),
	)
	_, err := Handler(nuclioContextP, &testEvent)
	if err != nil {
		t.Error(err)
	}
}
