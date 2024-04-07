package main

import (
	"context"
	"math"
	"os"
	"testing"

	"github.com/Marvin9/ftsdb/experiments"
	"github.com/Marvin9/ftsdb/ftsdb"
	"github.com/Marvin9/ftsdb/shared"
	"github.com/Marvin9/ftsdb/transformer"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func noErr(err error) {
	shared.NoErr(err)
}

func TestSmoke(t *testing.T) {
	os.RemoveAll(experiments.GetIngestionDir())

	logger, _ := zap.NewProduction()

	seriesMac := map[string]string{
		"host": "macbook",
	}
	seriesWin := map[string]string{
		"host": "wind",
	}

	fftsdb := ftsdb.NewFTSDB(logger, experiments.GetIngestionDir())

	defer fftsdb.Close()

	metric := fftsdb.CreateMetric("jay")

	var i int64
	for i = 0; i < 10000; i++ {
		metric.Append(seriesMac, int64(i), float64(i))
		metric.Append(seriesWin, int64(i), float64(i))
	}

	fftsdb.Commit()

	query := ftsdb.Query{}
	query.Series(seriesMac)

	firstFtsdb := experiments.FTSDBIterateAll(fftsdb.Find(query))

	query.Series(seriesWin)

	secFtsdb := experiments.FTSDBIterateAll(fftsdb.Find(query))

	dir, err := os.MkdirTemp("", "tsdb-test")
	shared.NoErr(err)

	// logger.Info("directory-at", zap.String("dir", dir))

	// Open a TSDB for reading and/or writing.
	db, err := tsdb.Open(dir, nil, nil, tsdb.DefaultOptions(), nil)
	shared.NoErr(err)

	// Open an appender for writing.
	app := db.Appender(context.Background())

	pseriesMac := labels.FromStrings("host", "macbook")
	pseriesWin := labels.FromStrings("host", "wind")

	var ref storage.SeriesRef = 0

	for i = 0; i < 10000; i++ {
		app.Append(0, pseriesMac, i, float64(i))
		app.Append(ref, pseriesWin, i, float64(i))
	}

	err = app.Commit()
	shared.NoErr(err)

	querier, err := db.Querier(math.MinInt64, math.MaxInt64)
	shared.NoErr(err)

	firstProm := experiments.PrometheusTSDBFindIterateAll(querier.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, "host", "macbook")))

	secondProm := experiments.PrometheusTSDBFindIterateAll(querier.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, "host", "wind")))

	err = db.Close()
	shared.NoErr(err)

	err = os.RemoveAll(dir)
	shared.NoErr(err)

	require.Equal(t, firstProm, firstFtsdb)
	require.Equal(t, secondProm, secFtsdb)

	os.RemoveAll(experiments.GetIngestionDir())

	// ------------------------------------------

	// Open a TSDB for reading and/or writing.
	db, err = tsdb.Open(dir, nil, nil, tsdb.DefaultOptions(), nil)
	shared.NoErr(err)

	// Open an appender for writing.
	app = db.Appender(context.Background())

	for i = 0; i < 10000; i++ {
		app.Append(0, pseriesMac, i, float64(i))
		app.Append(0, pseriesWin, i, float64(i))

	}

	err = app.Commit()
	noErr(err)

	querier, err = db.Querier(5000, math.MaxInt64)
	noErr(err)

	firstProm = experiments.PrometheusTSDBFindIterateAll(querier.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, "host", "macbook")))

	secondProm = experiments.PrometheusTSDBFindIterateAll(querier.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, "host", "wind")))

	querier, err = db.Querier(5000, 5100)
	noErr(err)

	thirdProm := experiments.PrometheusTSDBFindIterateAll(querier.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, "host", "wind")))
	fourthProm := experiments.PrometheusTSDBFindIterateAll(querier.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, "host", "macbook")))

	err = db.Close()
	noErr(err)

	err = os.RemoveAll(dir)
	noErr(err)

	fftsdb = ftsdb.NewFTSDB(logger, experiments.GetIngestionDir())

	metric = fftsdb.CreateMetric("jay")

	for i = 0; i < 10000; i++ {
		metric.Append(seriesMac, int64(i), float64(i))
		metric.Append(seriesWin, int64(i), float64(i))
	}

	fftsdb.Commit()

	query = ftsdb.Query{}
	query.RangeStart(5000)
	query.Series(seriesMac)

	firstFtsdb = experiments.FTSDBIterateAll(fftsdb.Find(query))

	query.RangeEnd(5100)
	fourthFtsdb := experiments.FTSDBIterateAll(fftsdb.Find(query))

	query.Series(seriesWin)
	query.RangeEnd(math.MaxInt64)

	secFtsdb = experiments.FTSDBIterateAll(fftsdb.Find(query))

	query.RangeEnd(5100)
	thirdFtsdb := experiments.FTSDBIterateAll(fftsdb.Find(query))

	require.Equal(t, firstProm, firstFtsdb)
	require.Equal(t, secondProm, secFtsdb)
	require.Equal(t, thirdProm, thirdFtsdb)
	require.Equal(t, fourthProm, fourthFtsdb)

	os.RemoveAll(experiments.GetIngestionDir())

	// ----------------------------------

	dataTransformer := transformer.NewDataTransformer(logger)

	cpuData := dataTransformer.GenCPUData("./data/cpu_usage.json", 100000)

	// Open a TSDB for reading and/or writing.
	db, err = tsdb.Open(dir, nil, nil, tsdb.DefaultOptions(), nil)
	noErr(err)

	// Open an appender for writing.
	app = db.Appender(context.Background())

	for _, data := range cpuData {
		app.Append(0, pseriesMac, data.Timestamp, data.CPUUsage)
	}

	err = app.Commit()
	noErr(err)

	querier, err = db.Querier(math.MinInt64, math.MaxInt64)
	noErr(err)

	firstProm = experiments.PrometheusTSDBFindIterateAll(querier.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, "host", "macbook")))

	err = db.Close()
	noErr(err)

	err = os.RemoveAll(dir)
	noErr(err)

	fftsdb = ftsdb.NewFTSDB(logger, shared.GetIngestionDir())

	metric = fftsdb.CreateMetric("mayur")

	for _, data := range cpuData {
		metric.Append(pseriesMac.Map(), data.Timestamp, data.CPUUsage)
	}

	fftsdb.Commit()
	query = ftsdb.Query{}
	query.Series(pseriesMac.Map())
	firstFtsdb = experiments.FTSDBIterateAll(fftsdb.Find(query))

	require.Equal(t, firstProm, firstFtsdb)
	os.RemoveAll(experiments.GetIngestionDir())
}
