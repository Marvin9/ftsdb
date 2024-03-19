package main

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"testing"

	"github.com/Marvin9/ftsdb/ftsdb"
	"github.com/Marvin9/ftsdb/transformer"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"go.uber.org/zap"
)

func PrometheusTSDBFindIterateAll(selector storage.SeriesSet) {
	for selector.Next() {
		series := selector.At()

		it := series.Iterator(nil)

		for it.Next() == chunkenc.ValFloat {
			it.At()
		}
	}
}

func FTSDBIterateAll(it ftsdb.DataPointsIterator) {
	for it.Is() {
		// fmt.Println(it.GetMetric(), it.GetSeries(), it.GetTimestamp(), it.GetValue())
		it = it.Next()
	}
}

func Dummy() {
	logger, _ := zap.NewDevelopment()
	dataTransformer := transformer.NewDataTransformer(logger)

	cpuData := dataTransformer.GenCPUData()

	logger.Debug("cpu-data", zap.Int("len", len(cpuData)))

	// for n := 0; n < b.N; n++ {
	// Create a random dir to work in.  Open() doesn't require a pre-existing dir, but
	// we want to make sure not to make a mess where we shouldn't.
	dir, err := os.MkdirTemp("", "tsdb-test")
	noErr(err)

	logger.Debug("directory-at", zap.String("dir", dir))

	// Open a TSDB for reading and/or writing.
	db, err := tsdb.Open(dir, nil, nil, tsdb.DefaultOptions(), nil)
	noErr(err)

	// Open an appender for writing.
	app := db.Appender(context.Background())

	series := labels.FromStrings("host", "macbook")

	var ref storage.SeriesRef = 0

	// Iterate through the CPU data
	for i, data := range cpuData {
		if i == 0 {
			ref, err = app.Append(0, series, data.Timestamp, data.CPUUsage)
			noErr(err)
			continue
		}

		_, err = app.Append(ref, series, data.Timestamp, data.CPUUsage)
		noErr(err)
	}

	// Commit to storage.
	err = app.Commit()
	noErr(err)

	logger.Debug("done commit")

	// In case you want to do more appends after app.Commit(),
	// you need a new appender.
	app = db.Appender(context.Background())
	// ... adding more samples.

	// Open a querier for reading.
	querier, err := db.Querier(math.MinInt64, math.MaxInt64)
	noErr(err)
	ss := querier.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, "host", "macbook"))

	for ss.Next() {
		series := ss.At()
		fmt.Println("series:", series.Labels().String())

		it := series.Iterator(nil)
		for it.Next() == chunkenc.ValFloat {
			ts, v := it.At() // We ignore the timestamp here, only to have a predictable output we can test against (below)
			fmt.Println("sample", v, " ", ts)
		}

		fmt.Println("it.Err():", it.Err())
	}
	fmt.Println("ss.Err():", ss.Err())
	ws := ss.Warnings()
	if len(ws) > 0 {
		fmt.Println("warnings:", ws)
	}
	err = querier.Close()
	noErr(err)

	// Clean up any last resources when done.
	err = db.Close()
	noErr(err)
	err = os.RemoveAll(dir)
	noErr(err)
	// }
}

func Benchmark_BasicPrometheusTSDB(b *testing.B) {
	for n := 0; n < b.N; n++ {
		dir, err := os.MkdirTemp("", "tsdb-test")
		noErr(err)

		// logger.Info("directory-at", zap.String("dir", dir))

		// Open a TSDB for reading and/or writing.
		db, err := tsdb.Open(dir, nil, nil, tsdb.DefaultOptions(), nil)
		noErr(err)

		// Open an appender for writing.
		app := db.Appender(context.Background())

		seriesMac := labels.FromStrings("host", "macbook")
		seriesWin := labels.FromStrings("host", "wind")

		var ref storage.SeriesRef = 0

		var i int64
		for i = 0; i < 10000; i++ {
			if i == 0 {
				ref, err = app.Append(0, seriesMac, i, float64(i))
				app.Append(ref, seriesWin, i, float64(i))
				noErr(err)
				continue
			}

			app.Append(ref, seriesMac, i, float64(i))
			app.Append(ref, seriesWin, i, float64(i))
		}

		err = app.Commit()
		noErr(err)

		querier, err := db.Querier(math.MinInt64, math.MaxInt64)
		noErr(err)

		querier.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, "host", "macbook"))

		querier.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, "host", "wind"))

		err = db.Close()
		noErr(err)

		err = os.RemoveAll(dir)
		noErr(err)
	}
}

func Benchmark_BasicFTSDB(b *testing.B) {
	logger, _ := zap.NewProduction()

	for n := 0; n < b.N; n++ {
		seriesMac := map[string]interface{}{
			"host": "macbook",
		}
		seriesWin := map[string]interface{}{
			"host": "wind",
		}

		tsdb := ftsdb.NewFTSDB(logger)

		metric := tsdb.CreateMetric("jay")

		var i int64
		for i = 0; i < 10000; i++ {
			metric.Append(seriesMac, "mac", int64(i), float64(i))
			metric.Append(seriesWin, "win", int64(i), float64(i))
		}

		query := ftsdb.Query{}
		query.Series("mac")

		FTSDBIterateAll(tsdb.Find(query))

		query.Series("win")

		FTSDBIterateAll(tsdb.Find(query))
	}
}

func Benchmark_RangePrometheusTSDB(b *testing.B) {
	for n := 0; n < b.N; n++ {
		dir, err := os.MkdirTemp("", "tsdb-test")
		noErr(err)

		// logger.Info("directory-at", zap.String("dir", dir))

		// Open a TSDB for reading and/or writing.
		db, err := tsdb.Open(dir, nil, nil, tsdb.DefaultOptions(), nil)
		noErr(err)

		// Open an appender for writing.
		app := db.Appender(context.Background())

		seriesMac := labels.FromStrings("host", "macbook")
		seriesWin := labels.FromStrings("host", "wind")

		var ref storage.SeriesRef = 0

		var i int64
		for i = 0; i < 1000000; i++ {
			if i == 0 {
				ref, err = app.Append(0, seriesMac, i, float64(i))
				app.Append(ref, seriesWin, i, float64(i))
				noErr(err)
				continue
			}

			app.Append(ref, seriesMac, i, float64(i))
			app.Append(ref, seriesWin, i, float64(i))
		}

		err = app.Commit()
		noErr(err)

		querier, err := db.Querier(500000, math.MaxInt64)
		noErr(err)

		PrometheusTSDBFindIterateAll(querier.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, "host", "macbook")))

		PrometheusTSDBFindIterateAll(querier.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, "host", "wind")))

		err = db.Close()
		noErr(err)

		err = os.RemoveAll(dir)
		noErr(err)
	}
}

func Benchmark_RangeFTSDB(b *testing.B) {
	logger, _ := zap.NewProduction()
	for n := 0; n < b.N; n++ {
		seriesMac := map[string]interface{}{
			"host": "macbook",
		}
		seriesWin := map[string]interface{}{
			"host": "wind",
		}

		tsdb := ftsdb.NewFTSDB(logger)

		metric := tsdb.CreateMetric("jay")

		var i int64
		for i = 0; i < 1000000; i++ {
			metric.Append(seriesMac, "mac", int64(i), float64(i))
			metric.Append(seriesWin, "win", int64(i), float64(i))
		}

		query := ftsdb.Query{}
		query.RangeStart(500000)
		query.Series("mac")

		FTSDBIterateAll(tsdb.Find(query))

		query.Series("win")

		FTSDBIterateAll(tsdb.Find(query))
	}
}

func Benchmark_RangesPrometheusTSDB(b *testing.B) {
	for n := 0; n < b.N; n++ {
		dir, err := os.MkdirTemp("", "tsdb-test")
		noErr(err)

		// logger.Info("directory-at", zap.String("dir", dir))

		// Open a TSDB for reading and/or writing.
		db, err := tsdb.Open(dir, nil, nil, tsdb.DefaultOptions(), nil)
		noErr(err)

		// Open an appender for writing.
		app := db.Appender(context.Background())

		seriesMac := labels.FromStrings("host", "macbook")
		seriesWin := labels.FromStrings("host", "wind")

		var ref storage.SeriesRef = 0

		var i int64
		for i = 0; i < 1000000; i++ {
			if i == 0 {
				ref, err = app.Append(0, seriesMac, i, float64(i))
				app.Append(ref, seriesWin, i, float64(i))
				noErr(err)
				continue
			}

			app.Append(ref, seriesMac, i, float64(i))
			app.Append(ref, seriesWin, i, float64(i))
		}

		err = app.Commit()
		noErr(err)

		querier, err := db.Querier(500000, 510000)
		noErr(err)

		PrometheusTSDBFindIterateAll(querier.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, "host", "macbook")))

		PrometheusTSDBFindIterateAll(querier.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, "host", "wind")))

		err = db.Close()
		noErr(err)

		err = os.RemoveAll(dir)
		noErr(err)
	}
}

func Benchmark_RangesFTSDB(b *testing.B) {
	logger, _ := zap.NewProduction()
	for n := 0; n < b.N; n++ {
		seriesMac := map[string]interface{}{
			"host": "macbook",
		}
		seriesWin := map[string]interface{}{
			"host": "wind",
		}

		tsdb := ftsdb.NewFTSDB(logger)

		metric := tsdb.CreateMetric("jay")

		var i int64
		for i = 0; i < 1000000; i++ {
			metric.Append(seriesMac, "mac", int64(i), float64(i))
			metric.Append(seriesWin, "win", int64(i), float64(i))
		}

		query := ftsdb.Query{}
		query.RangeStart(500000)
		query.RangeEnd(510000)
		query.Series("mac")

		FTSDBIterateAll(tsdb.Find(query))

		query.Series("win")

		FTSDBIterateAll(tsdb.Find(query))
	}
}

func genHeavyMetricsList(_i int) []string {
	metricsList := []string{}

	for i := 0; i < _i; i++ {
		metricsList = append(metricsList, fmt.Sprintf("metric-%d", i))
	}

	return metricsList
}

func getHeavySeriesList(_i int) []map[string]interface{} {
	seriesList := []map[string]interface{}{}

	for i := 0; i < _i; i++ {
		seriesList = append(seriesList, getHeavySeries(i))
	}

	return seriesList
}

func getHeavySeries(_i int) map[string]interface{} {
	series := make(map[string]interface{})

	for i := 0; i < _i; i++ {
		seriesName := fmt.Sprintf("series-%d", i)
		series[seriesName] = rand.Intn(100)
	}

	return series
}

func Benchmark_HeavyAppendPrometheusTSDB(b *testing.B) {
	// logger, _ := zap.NewProduction()
	seriesList := getHeavySeriesList(10)

	for n := 0; n < b.N; n++ {
		dir, err := os.MkdirTemp("", "tsdb-test")
		noErr(err)

		// logger.Info("directory-at", zap.String("dir", dir))

		// Open a TSDB for reading and/or writing.
		db, err := tsdb.Open(dir, nil, nil, tsdb.DefaultOptions(), nil)
		noErr(err)

		// Open an appender for writing.
		app := db.Appender(context.Background())

		var ref storage.SeriesRef = 0

		firstTime := true
		for _, seriesIn := range seriesList {
			for key, val := range seriesIn {
				__series := labels.FromStrings(key, fmt.Sprint(val))

				var i int64
				for i = 0; i < 100; i++ {
					if firstTime {
						ref, _ = app.Append(0, __series, i, float64(i))
						// noErr(err)
						firstTime = false
						continue
					}

					app.Append(ref, __series, i, float64(i))
				}
			}
		}

		err = app.Commit()
		noErr(err)

		querier, err := db.Querier(math.MinInt64, math.MaxInt64)
		noErr(err)

		for _, seriesIn := range seriesList {
			for key, val := range seriesIn {
				PrometheusTSDBFindIterateAll(querier.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, key, fmt.Sprint(val))))
			}
		}
		err = db.Close()
		noErr(err)

		err = os.RemoveAll(dir)
		noErr(err)
	}
}

func Benchmark_HeavyAppendFTSDB(b *testing.B) {
	logger, _ := zap.NewProduction()

	seriesList := getHeavySeriesList(100)

	for n := 0; n < b.N; n++ {
		tsdb := ftsdb.NewFTSDB(logger)

		metric := tsdb.CreateMetric("jay")

		for _, seriesIn := range seriesList {
			for key, val := range seriesIn {
				__series := map[string]interface{}{}

				__series[key] = fmt.Sprint(val)
				for i := 0; i < 100; i++ {
					metric.Append(__series, fmt.Sprintf("%s-%s", key, val), int64(i), float64(i))
				}
			}
		}

		fmt.Println("done")
		query := ftsdb.Query{}

		for _, seriesIn := range seriesList {
			for key, val := range seriesIn {
				query.Series(fmt.Sprintf("%s-%s", key, val))

				FTSDBIterateAll(tsdb.Find(query))
			}
		}
	}
}
