package ftsdb

import (
	"os"
	"testing"

	"go.uber.org/zap"
)

func TestEnsureEverything(t *testing.T) {
	logger, _ := zap.NewProduction()

	logger.Info("process-id", zap.Int("id", os.Getpid()))

	// time.Sleep(time.Second * 20)

	seriesMac := map[string]interface{}{
		"host": "macbook",
	}
	seriesWin := map[string]interface{}{
		"host": "wind",
	}

	tsdb := NewFTSDB(logger)

	metric := tsdb.CreateMetric("cpu")
	metric2 := tsdb.CreateMetric("mem")

	num := 10

	var i int
	for i = 1; i <= num; i++ {
		metric.Append(seriesMac, "mac", int64(i), float64(i))
		metric.Append(seriesWin, "win", int64(i), float64(i))
		metric2.Append(seriesMac, "mac", int64(i), float64(i))
	}

	logger.Info("executing search query")

	query := Query{}

	it := tsdb.Find(query)

	tot := 0
	for it.Is() {
		it = it.Next()
		tot++
	}

	if tot != num*3 {
		t.Errorf("expected %d, got %d", num*3, tot)
	}

	query.RangeStart(int64(num / 2))

	it = tsdb.Find(query)

	tot = 0
	for it.Is() {
		it = it.Next()
		tot++
	}

	exp := ((num / 2) + 1) * 3
	if tot != exp {
		t.Errorf("expected %d, got %d", exp, tot)
	}

	query = *query.RangeStart(0)
	query = *query.RangeEnd(int64((num / 2) + 1))

	it = tsdb.Find(query)

	tot = 0
	for it.Is() {
		it = it.Next()
		tot++
	}

	if tot != exp {
		t.Errorf("expected %d, got %d", exp, tot)
	}

	query = Query{}
	query.Series("mac")

	it = tsdb.Find(query)
	tot = 0
	for it.Is() {
		if it.GetSeries() != "mac" {
			t.Errorf("expected mac, got %s", it.GetSeries())
		}
		it = it.Next()
		tot++
	}

	exp = num * 2
	if tot != exp {
		t.Errorf("expected %d, got %d", exp, tot)
	}
}
