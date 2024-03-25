package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"testing"

	"github.com/Marvin9/ftsdb/transformer"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"go.uber.org/zap"
)

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
			BasicPrometheus()
		})
	}
}

func Benchmark_BasicFTSDB(b *testing.B) {
	logger, _ := zap.NewProduction()

	for n := 0; n < b.N; n++ {
		b.Run("core", func(b *testing.B) {
			BasicFTSDB(logger)
		})
	}
}

func Benchmark_RangePrometheusTSDB(b *testing.B) {
	for n := 0; n < b.N; n++ {
		b.Run("core", func(b *testing.B) {
			RangePrometheusTSDB()
		})
	}
}

func Benchmark_RangeFTSDB(b *testing.B) {
	logger, _ := zap.NewProduction()
	for n := 0; n < b.N; n++ {
		b.Run("core", func(b *testing.B) {
			RangeFTSDB(logger)
		})
	}
}

func Benchmark_RangesPrometheusTSDB(b *testing.B) {
	for n := 0; n < b.N; n++ {
		b.Run("core", func(b *testing.B) {
			RangesPrometheusTSDB()
		})
	}
}

func Benchmark_RangesFTSDB(b *testing.B) {
	logger, _ := zap.NewProduction()
	for n := 0; n < b.N; n++ {
		b.Run("core", func(b *testing.B) {
			RangesFTSDB(logger)
		})
	}
}

func Benchmark_HeavyAppendPrometheusTSDB(b *testing.B) {
	// logger, _ := zap.NewProduction()
	seriesList := getHeavySeriesList(20)

	for n := 0; n < b.N; n++ {
		b.Run("core", func(b *testing.B) {
			HeavyAppendPrometheusTSDB(seriesList, 100000)
		})
	}
}

func Benchmark_HeavyAppendWriteDiskPrometheusTSDB(b *testing.B) {
	// logger, _ := zap.NewProduction()
	seriesList := getHeavySeriesList(20)

	for n := 0; n < b.N; n++ {
		b.Run("core", func(b *testing.B) {
			HeavyAppendWriteDiskPrometheusTSDB(seriesList, 100000)
		})
	}
}

func Benchmark_HeavyAppendFTSDB(b *testing.B) {
	logger, _ := zap.NewProduction()

	seriesList := getHeavySeriesList(20)

	for n := 0; n < b.N; n++ {
		b.Run("core", func(b *testing.B) {
			HeavyAppendFTSDB(logger, seriesList, 100000)
		})
	}
}

func Benchmark_HeavyAppendWriteDiskFTSDB(b *testing.B) {
	logger, _ := zap.NewProduction()

	seriesList := getHeavySeriesList(20)

	for n := 0; n < b.N; n++ {
		b.Run("core", func(b *testing.B) {
			HeavyAppendWriteDiskFTSDB(logger, seriesList, 100000)
		})
	}
}

func BenchmarkRealCPUUsageDataPrometheusTSDB(b *testing.B) {
	logger, _ := zap.NewProduction()
	dataTransformer := transformer.NewDataTransformer(logger)

	cpuData := dataTransformer.GenCPUData(100000)
	for n := 0; n < b.N; n++ {

		b.Run("main", func(b *testing.B) {
			RealCPUUsageDataPrometheusTSDB(cpuData, logger)
		})

	}
}

func BenchmarkRealCPUUsageDataFTSDB(b *testing.B) {
	logger, _ := zap.NewProduction()
	dataTransformer := transformer.NewDataTransformer(logger)

	cpuData := dataTransformer.GenCPUData(100000)

	for n := 0; n < b.N; n++ {
		b.Run("core", func(b *testing.B) {
			RealCPUUsageDataFTSDB(logger, cpuData)
		})
	}
}

func BenchmarkRealCPUUsageDataConsequentAppendWritePrometheusTSDB(b *testing.B) {
	logger, _ := zap.NewProduction()
	dataTransformer := transformer.NewDataTransformer(logger)

	cpuData := dataTransformer.GenCPUData(10000)

	for n := 0; n < b.N; n++ {
		b.Run("main", func(b *testing.B) {
			RealCPUUsageDataConsequentAppendWritePrometheusTSDB(logger, cpuData)
		})
	}
}

func BenchmarkRealCPUUsageDataConsequentAppendWriteFTSDB(b *testing.B) {
	logger, _ := zap.NewProduction()
	dataTransformer := transformer.NewDataTransformer(logger)

	cpuData := dataTransformer.GenCPUData(10000)

	for n := 0; n < b.N; n++ {
		b.Run("core", func(b *testing.B) {
			RealCPUUsageDataConsequentAppendWriteFTSDB(logger, cpuData)
		})
	}
}

func BenchmarkRealCPUUsageRangeDataPrometheusTSDB(b *testing.B) {
	logger, _ := zap.NewProduction()
	dataTransformer := transformer.NewDataTransformer(logger)

	cpuData := dataTransformer.GenCPUData(100000)

	for n := 0; n < b.N; n++ {
		b.Run("main", func(b *testing.B) {
			RealCPUUsageRangeDataPrometheusTSDB(logger, cpuData)
		})
	}
}

func BenchmarkRealCPUUsageRangeDataFTSDB(b *testing.B) {
	logger, _ := zap.NewProduction()
	dataTransformer := transformer.NewDataTransformer(logger)

	cpuData := dataTransformer.GenCPUData(100000)

	for n := 0; n < b.N; n++ {
		b.Run("core", func(b *testing.B) {
			RealCPUUsageRangeDataFTSDB(logger, cpuData)
		})
	}
}

func BenchmarkAppendMillionPointsPrometheusTSDB(b *testing.B) {
	for n := 0; n < b.N; n++ {
		b.Run("main", func(b *testing.B) {
			AppendMillionPointsPrometheusTSDB()
		})
	}
}

func BenchmarkAppendMillionPointsFTSDB(b *testing.B) {
	logger, _ := zap.NewProduction()

	for n := 0; n < b.N; n++ {
		b.Run("core", func(b *testing.B) {
			AppendMillionPointsFTSDB(logger)
		})
	}
}

func BenchmarkAppendHundredPointsWithLabelsPrometheusTSDB(b *testing.B) {
	for n := 0; n < b.N; n++ {
		b.Run("main", func(b *testing.B) {
			AppendPointsWithLabelsPrometheusTSDB(10000)
		})
	}
}

func BenchmarkAppendHundredPointsWithLabelsFTSDB(b *testing.B) {
	logger, _ := zap.NewProduction()

	for n := 0; n < b.N; n++ {
		b.Run("core", func(b *testing.B) {
			AppendPointsWithLabelsFTSDB(logger, 10000)
		})
	}
}
