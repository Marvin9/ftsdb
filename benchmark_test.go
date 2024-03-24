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
	tot := 0
	var __series labels.Labels
	for selector.Next() {
		series := selector.At()
		if __series.Len() == 0 {
			__series = series.Labels()
		}

		it := series.Iterator(nil)

		for it.Next() == chunkenc.ValFloat {
			tot++
			it.At()
		}
	}
	if tot > 0 {
		// fmt.Println(__series, tot)
	}
}

func FTSDBIterateAll(ss *ftsdb.SeriesIterator) {
	tot := 0
	for ss.Next() != nil {
		it := ss.DatapointsIterator

		// fmt.Println(ss.GetSeries())
		for it.Next() != nil {
			// fmt.Println(it.GetDatapoint())
			tot++
		}
	}
	// fmt.Println(tot)
}

func Dummy() {
	logger, _ := zap.NewDevelopment()
	dataTransformer := transformer.NewDataTransformer(logger)

	cpuData := dataTransformer.GenCPUData(-1)

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
		b.Run("core", func(b *testing.B) {
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
				app.Append(0, seriesMac, i, float64(i))
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
		})
	}
}

func Benchmark_BasicFTSDB(b *testing.B) {
	logger, _ := zap.NewProduction()

	for n := 0; n < b.N; n++ {
		b.Run("core", func(b *testing.B) {
			seriesMac := map[string]string{
				"host": "macbook",
			}
			seriesWin := map[string]string{
				"host": "wind",
			}

			tsdb := ftsdb.NewFTSDB(logger)

			metric := tsdb.CreateMetric("jay")

			var i int64
			for i = 0; i < 10000; i++ {
				metric.Append(seriesMac, int64(i), float64(i))
				metric.Append(seriesWin, int64(i), float64(i))
			}

			query := ftsdb.Query{}
			query.Series(seriesMac)

			FTSDBIterateAll(tsdb.Find(query))

			query.Series(seriesWin)

			FTSDBIterateAll(tsdb.Find(query))
		})
	}
}

func Benchmark_RangePrometheusTSDB(b *testing.B) {
	for n := 0; n < b.N; n++ {
		b.Run("core", func(b *testing.B) {
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

			var i int64
			for i = 0; i < 1000000; i++ {
				app.Append(0, seriesMac, i, float64(i))
				app.Append(0, seriesWin, i, float64(i))

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
		})
	}
}

func Benchmark_RangeFTSDB(b *testing.B) {
	logger, _ := zap.NewProduction()
	for n := 0; n < b.N; n++ {
		b.Run("core", func(b *testing.B) {
			seriesMac := map[string]string{
				"host": "macbook",
			}
			seriesWin := map[string]string{
				"host": "wind",
			}

			tsdb := ftsdb.NewFTSDB(logger)

			metric := tsdb.CreateMetric("jay")

			var i int64
			for i = 0; i < 1000000; i++ {
				metric.Append(seriesMac, int64(i), float64(i))
				metric.Append(seriesWin, int64(i), float64(i))
			}

			query := ftsdb.Query{}
			query.RangeStart(500000)
			query.Series(seriesMac)

			FTSDBIterateAll(tsdb.Find(query))

			query.Series(seriesWin)

			FTSDBIterateAll(tsdb.Find(query))
		})
	}
}

func Benchmark_RangesPrometheusTSDB(b *testing.B) {
	for n := 0; n < b.N; n++ {
		b.Run("core", func(b *testing.B) {
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
				app.Append(0, seriesMac, i, float64(i))
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
		})
	}
}

func Benchmark_RangesFTSDB(b *testing.B) {
	logger, _ := zap.NewProduction()
	for n := 0; n < b.N; n++ {
		b.Run("core", func(b *testing.B) {
			seriesMac := map[string]string{
				"host": "macbook",
			}
			seriesWin := map[string]string{
				"host": "wind",
			}

			tsdb := ftsdb.NewFTSDB(logger)

			metric := tsdb.CreateMetric("jay")

			var i int64
			for i = 0; i < 1000000; i++ {
				metric.Append(seriesMac, int64(i), float64(i))
				metric.Append(seriesWin, int64(i), float64(i))
			}

			query := ftsdb.Query{}
			query.RangeStart(500000)
			query.RangeEnd(510000)
			query.Series(seriesMac)

			FTSDBIterateAll(tsdb.Find(query))

			query.Series(seriesWin)

			FTSDBIterateAll(tsdb.Find(query))
		})
	}
}

func genHeavyMetricsList(_i int) []string {
	metricsList := []string{}

	for i := 0; i < _i; i++ {
		metricsList = append(metricsList, fmt.Sprintf("metric-%d", i))
	}

	return metricsList
}

func getHeavySeriesList(_i int) []map[string]int {
	seriesList := []map[string]int{}

	for i := 0; i < _i; i++ {
		seriesList = append(seriesList, getHeavySeries(i))
	}

	return seriesList
}

func getHeavySeries(_i int) map[string]int {
	series := make(map[string]int)

	for i := _i; i < _i+1; i++ {
		seriesName := fmt.Sprintf("series-%d", i)
		series[seriesName] = rand.Intn(100)
	}

	return series
}

func Benchmark_HeavyAppendPrometheusTSDB(b *testing.B) {
	// logger, _ := zap.NewProduction()
	seriesList := getHeavySeriesList(100)

	for n := 0; n < b.N; n++ {
		b.Run("core", func(b *testing.B) {
			dir, err := os.MkdirTemp("", "tsdb-test")
			noErr(err)

			// logger.Info("directory-at", zap.String("dir", dir))

			// Open a TSDB for reading and/or writing.
			db, err := tsdb.Open(dir, nil, nil, tsdb.DefaultOptions(), nil)
			noErr(err)

			// Open an appender for writing.
			app := db.Appender(context.Background())

			// var ref storage.SeriesRef = 0

			for _, seriesIn := range seriesList {
				for key, val := range seriesIn {
					// firstTime := true
					__series := labels.FromStrings(key, fmt.Sprintf("%d", val))

					i := int64(0)
					for i = 0; i < 10000; i++ {
						app.Append(0, __series, i, float64(i))
					}
				}
			}

			err = app.Commit()
			noErr(err)

			querier, err := db.Querier(math.MinInt64, math.MaxInt64)
			noErr(err)

			for _, seriesIn := range seriesList {
				for key, val := range seriesIn {
					PrometheusTSDBFindIterateAll(querier.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, key, fmt.Sprintf("%d", val))))
				}
			}
			err = db.Close()
			noErr(err)

			err = os.RemoveAll(dir)
			noErr(err)
		})
	}
}

func Benchmark_HeavyAppendFTSDB(b *testing.B) {
	logger, _ := zap.NewProduction()

	seriesList := getHeavySeriesList(100)

	for n := 0; n < b.N; n++ {
		b.Run("core", func(b *testing.B) {
			tsdb := ftsdb.NewFTSDB(logger)

			metric := tsdb.CreateMetric("jay")

			for _, seriesIn := range seriesList {
				for key, val := range seriesIn {
					__series := map[string]string{}

					__series[key] = fmt.Sprint(val)
					for i := 0; i < 10000; i++ {
						metric.Append(__series, int64(i), float64(i))
					}
				}
			}

			query := ftsdb.Query{}

			for _, seriesIn := range seriesList {
				for key, val := range seriesIn {
					__series := map[string]string{}

					__series[key] = fmt.Sprintf("%d", val)
					query.Series(__series)

					FTSDBIterateAll(tsdb.Find(query))
				}
			}
		})
	}
}

func BenchmarkRealCPUUsageDataPrometheusTSDB(b *testing.B) {
	logger, _ := zap.NewProduction()
	dataTransformer := transformer.NewDataTransformer(logger)

	cpuData := dataTransformer.GenCPUData(100000)
	series := labels.FromStrings("host", "macbook")

	for n := 0; n < b.N; n++ {

		b.Run("main", func(b *testing.B) {
			dir, err := os.MkdirTemp("", "tsdb-test")
			// logger.Info(dir)
			noErr(err)

			// logger.Info("directory-at", zap.String("dir", dir))

			// Open a TSDB for reading and/or writing.
			db, err := tsdb.Open(dir, nil, nil, tsdb.DefaultOptions(), nil)
			noErr(err)

			// Open an appender for writing.
			app := db.Appender(context.Background())

			for _, data := range cpuData {
				app.Append(0, series, data.Timestamp, data.CPUUsage)
			}

			err = app.Commit()
			noErr(err)

			querier, err := db.Querier(math.MinInt64, math.MaxInt64)
			noErr(err)

			PrometheusTSDBFindIterateAll(querier.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, "host", "macbook")))

			err = db.Close()
			noErr(err)

			err = os.RemoveAll(dir)
			noErr(err)
		})

	}
}

func BenchmarkRealCPUUsageDataFTSDB(b *testing.B) {
	logger, _ := zap.NewProduction()
	dataTransformer := transformer.NewDataTransformer(logger)

	cpuData := dataTransformer.GenCPUData(100000)
	series := labels.FromStrings("host", "macbook")

	for n := 0; n < b.N; n++ {
		b.Run("core", func(b *testing.B) {
			tsdb := ftsdb.NewFTSDB(logger)

			metric := tsdb.CreateMetric("mayur")
			for _, data := range cpuData {
				metric.Append(series.Map(), data.Timestamp, data.CPUUsage)
			}

			query := ftsdb.Query{}
			query.Series(series.Map())
			FTSDBIterateAll(tsdb.Find(query))
		})
	}
}

func BenchmarkRealCPUUsageDataConsequentAppendWritePrometheusTSDB(b *testing.B) {
	logger, _ := zap.NewProduction()
	dataTransformer := transformer.NewDataTransformer(logger)

	cpuData := dataTransformer.GenCPUData(10000)

	series := labels.FromStrings("host", "macbook")
	seriesMatcher := labels.MustNewMatcher(labels.MatchEqual, "host", "macbook")

	for n := 0; n < b.N; n++ {
		b.Run("main", func(b *testing.B) {
			dir, err := os.MkdirTemp("", "tsdb-test")
			noErr(err)

			db, err := tsdb.Open(dir, nil, nil, tsdb.DefaultOptions(), nil)
			noErr(err)

			for _, data := range cpuData {
				app := db.Appender(context.Background())
				app.Append(0, series, data.Timestamp, data.CPUUsage)

				noErr(app.Commit())

				queries, _ := db.Querier(math.MinInt64, math.MaxInt64)
				// noErr(err)

				PrometheusTSDBFindIterateAll(queries.Select(context.Background(), false, nil, seriesMatcher))
			}

			err = db.Close()
			noErr(err)

			err = os.RemoveAll(dir)
			noErr(err)
		})
	}
}

func BenchmarkRealCPUUsageDataConsequentAppendWriteFTSDB(b *testing.B) {
	logger, _ := zap.NewProduction()
	dataTransformer := transformer.NewDataTransformer(logger)

	cpuData := dataTransformer.GenCPUData(10000)
	series := labels.FromStrings("host", "macbook")

	for n := 0; n < b.N; n++ {
		b.Run("core", func(b *testing.B) {
			tsdb := ftsdb.NewFTSDB(logger)

			query := ftsdb.Query{}
			query.Series(series.Map())

			metric := tsdb.CreateMetric("mayur")
			for _, data := range cpuData {
				metric.Append(series.Map(), data.Timestamp, data.CPUUsage)
				FTSDBIterateAll(tsdb.Find(query))
			}
		})
	}
}

func BenchmarkRealCPUUsageRangeDataPrometheusTSDB(b *testing.B) {
	logger, _ := zap.NewProduction()
	dataTransformer := transformer.NewDataTransformer(logger)

	cpuData := dataTransformer.GenCPUData(100000)

	series := labels.FromStrings("host", "macbook")
	seriesMatcher := labels.MustNewMatcher(labels.MatchEqual, "host", "macbook")

	for n := 0; n < b.N; n++ {
		b.Run("main", func(b *testing.B) {
			dir, err := os.MkdirTemp("", "tsdb-test")
			noErr(err)

			db, err := tsdb.Open(dir, nil, nil, tsdb.DefaultOptions(), nil)
			noErr(err)

			app := db.Appender(context.Background())

			for _, data := range cpuData {
				app.Append(0, series, data.Timestamp, data.CPUUsage)
			}

			err = app.Commit()
			noErr(err)

			queries, err := db.Querier(cpuData[5000].Timestamp, math.MaxInt64)
			noErr(err)

			PrometheusTSDBFindIterateAll(queries.Select(context.Background(), false, nil, seriesMatcher))

			queries, err = db.Querier(math.MinInt64, cpuData[5000].Timestamp)
			noErr(err)

			PrometheusTSDBFindIterateAll(queries.Select(context.Background(), false, nil, seriesMatcher))

			err = db.Close()
			noErr(err)

			err = os.RemoveAll(dir)
			noErr(err)
		})
	}
}

func BenchmarkRealCPUUsageRangeDataFTSDB(b *testing.B) {
	logger, _ := zap.NewProduction()
	dataTransformer := transformer.NewDataTransformer(logger)

	cpuData := dataTransformer.GenCPUData(100000)
	series := labels.FromStrings("host", "macbook")

	for n := 0; n < b.N; n++ {
		b.Run("core", func(b *testing.B) {
			tsdb := ftsdb.NewFTSDB(logger)

			metric := tsdb.CreateMetric("mayur")
			for _, data := range cpuData {
				metric.Append(series.Map(), data.Timestamp, data.CPUUsage)
			}

			query := ftsdb.Query{}
			query.Series(series.Map())
			query.RangeStart(cpuData[5000].Timestamp)

			FTSDBIterateAll(tsdb.Find(query))

			query.RangeStart(math.MinInt64)
			query.RangeEnd(cpuData[5000].Timestamp)

			FTSDBIterateAll(tsdb.Find(query))
		})
	}
}
