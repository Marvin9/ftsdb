package ftsdb

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestEnsureEverything(t *testing.T) {
	logger, _ := zap.NewProduction()

	logger.Info("process-id", zap.Int("id", os.Getpid()))

	// time.Sleep(time.Second * 20)

	seriesMac := map[string]string{
		"host": "macbook",
	}
	seriesWin := map[string]string{
		"host": "wind",
	}

	dir, _ := os.Getwd()
	tsdb := NewFTSDB(logger, filepath.Join(dir, "ingestion"))

	metric := tsdb.CreateMetric("cpu")

	num := 10

	var i int
	for i = 1; i <= num; i++ {
		metric.Append(seriesMac, int64(i), float64(i))
		metric.Append(seriesWin, int64(i), float64(i))
	}

	require.NoError(t, tsdb.Commit())

	query := Query{}

	ss := tsdb.Find(query)

	tot := 0
	for ss.Next() != nil {
		dp := ss.DatapointsIterator

		// fmt.Println(ss.GetSeries())
		for dp.Next() != nil {
			// fmt.Println(dp.GetDatapoint())
			tot++
		}
	}

	fmt.Println("done")

	if tot != num*2 {
		t.Errorf("expected %d, got %d", num*2, tot)
	}

	query.RangeStart(int64(num / 2))

	ss = tsdb.Find(query)

	tot = 0
	for ss.Next() != nil {
		it := ss.DatapointsIterator

		for it.Next() != nil {
			tot++
		}
	}

	exp := ((num / 2) + 1) * 2
	if tot != exp {
		t.Errorf("expected %d, got %d", exp, tot)
	}

	query = *query.RangeStart(0)
	query = *query.RangeEnd(int64((num / 2) + 1))

	ss = tsdb.Find(query)

	tot = 0
	tot = 0
	for ss.Next() != nil {
		it := ss.DatapointsIterator

		for it.Next() != nil {
			tot++
		}
	}

	if tot != exp {
		t.Errorf("expected %d, got %d", exp, tot)
	}

	query = Query{}
	query.Series(seriesMac)

	ss = tsdb.Find(query)
	tot = 0
	tot = 0
	for ss.Next() != nil {
		it := ss.DatapointsIterator

		if !seriesMatched(ss.GetSeries().SeriesValue, seriesMac) {
			t.Errorf("expected %s, got %s", seriesMac, ss.GetSeries().SeriesValue)
		}

		for it.Next() != nil {
			tot++
		}
	}

	exp = num
	if tot != exp {
		t.Errorf("expected %d, got %d", exp, tot)
	}
}
