package experiments

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"

	"github.com/Marvin9/ftsdb/ftsdb"
	"github.com/Marvin9/ftsdb/transformer"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"go.uber.org/zap"
)

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

func BasicPrometheus() {
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

	// err = app.Commit()
	// noErr(err)

	querier, err := db.Querier(math.MinInt64, math.MaxInt64)
	noErr(err)

	PrometheusTSDBFindIterateAll(querier.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, "host", "macbook")))

	PrometheusTSDBFindIterateAll(querier.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, "host", "wind")))

	err = db.Close()
	noErr(err)

	err = os.RemoveAll(dir)
	noErr(err)
}

func BasicFTSDB(logger *zap.Logger) {
	seriesMac := map[string]string{
		"host": "macbook",
	}
	seriesWin := map[string]string{
		"host": "wind",
	}

	tsdb := ftsdb.NewFTSDB(logger, GetIngestionDir())

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
}

func RangePrometheusTSDB() {
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

	// err = app.Commit()
	// noErr(err)

	querier, err := db.Querier(500000, math.MaxInt64)
	noErr(err)

	PrometheusTSDBFindIterateAll(querier.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, "host", "macbook")))

	PrometheusTSDBFindIterateAll(querier.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, "host", "wind")))

	err = db.Close()
	noErr(err)

	err = os.RemoveAll(dir)
	noErr(err)
}

func RangeFTSDB(logger *zap.Logger) {
	seriesMac := map[string]string{
		"host": "macbook",
	}
	seriesWin := map[string]string{
		"host": "wind",
	}

	tsdb := ftsdb.NewFTSDB(logger, GetIngestionDir())

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
}

func RangesPrometheusTSDB() {
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

	// err = app.Commit()
	// noErr(err)

	querier, err := db.Querier(500000, 510000)
	noErr(err)

	PrometheusTSDBFindIterateAll(querier.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, "host", "macbook")))

	PrometheusTSDBFindIterateAll(querier.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, "host", "wind")))

	err = db.Close()
	noErr(err)

	err = os.RemoveAll(dir)
	noErr(err)
}

func RangesFTSDB(logger *zap.Logger) {
	seriesMac := map[string]string{
		"host": "macbook",
	}
	seriesWin := map[string]string{
		"host": "wind",
	}

	tsdb := ftsdb.NewFTSDB(logger, GetIngestionDir())

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
}

func HeavyAppendPrometheusTSDB(seriesList []map[string]int, points int) {
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
			for i = 0; i < int64(points); i++ {
				app.Append(0, __series, i, float64(i))
			}
		}
	}

	// err = app.Commit()
	// noErr(err)

	// querier, err := db.Querier(math.MinInt64, math.MaxInt64)
	// noErr(err)

	// for _, seriesIn := range seriesList {
	// 	for key, val := range seriesIn {
	// 		PrometheusTSDBFindIterateAll(querier.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, key, fmt.Sprintf("%d", val))))
	// 	}
	// }
	err = db.Close()
	noErr(err)

	err = os.RemoveAll(dir)
	noErr(err)
}

func HeavyAppendFTSDB(logger *zap.Logger, seriesList []map[string]int, points int) {
	tsdb := ftsdb.NewFTSDB(logger, GetIngestionDir())

	metric := tsdb.CreateMetric("jay")

	for _, seriesIn := range seriesList {
		for key, val := range seriesIn {
			__series := map[string]string{}

			__series[key] = fmt.Sprint(val)
			for i := 0; i < points; i++ {
				metric.Append(__series, int64(i), float64(i))
			}
		}
	}

	// query := ftsdb.Query{}

	// for _, seriesIn := range seriesList {
	// 	for key, val := range seriesIn {
	// 		__series := map[string]string{}

	// 		__series[key] = fmt.Sprintf("%d", val)
	// 		query.Series(__series)

	// 		FTSDBIterateAll(tsdb.Find(query))
	// 	}
	// }
}

func RealCPUUsageDataPrometheusTSDB(cpuData []transformer.CPUData, logger *zap.Logger) {
	series := labels.FromStrings("host", "macbook")

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

	// err = app.Commit()
	// noErr(err)

	querier, err := db.Querier(math.MinInt64, math.MaxInt64)
	noErr(err)

	PrometheusTSDBFindIterateAll(querier.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, "host", "macbook")))

	err = db.Close()
	noErr(err)

	err = os.RemoveAll(dir)
	noErr(err)
}

func RealCPUUsageDataFTSDB(logger *zap.Logger, cpuData []transformer.CPUData) {
	series := labels.FromStrings("host", "macbook")

	tsdb := ftsdb.NewFTSDB(logger, GetIngestionDir())

	metric := tsdb.CreateMetric("mayur")
	for _, data := range cpuData {
		metric.Append(series.Map(), data.Timestamp, data.CPUUsage)
	}

	query := ftsdb.Query{}
	query.Series(series.Map())
	FTSDBIterateAll(tsdb.Find(query))
}

func RealCPUUsageDataConsequentAppendWritePrometheusTSDB(logger *zap.Logger, cpuData []transformer.CPUData) {
	series := labels.FromStrings("host", "macbook")
	seriesMatcher := labels.MustNewMatcher(labels.MatchEqual, "host", "macbook")

	dir, err := os.MkdirTemp("", "tsdb-test")
	noErr(err)

	db, err := tsdb.Open(dir, nil, nil, tsdb.DefaultOptions(), nil)
	noErr(err)

	for _, data := range cpuData {
		app := db.Appender(context.Background())
		app.Append(0, series, data.Timestamp, data.CPUUsage)

		// noErr(app.Commit())

		queries, _ := db.Querier(math.MinInt64, math.MaxInt64)
		// noErr(err)

		PrometheusTSDBFindIterateAll(queries.Select(context.Background(), false, nil, seriesMatcher))
	}

	err = db.Close()
	noErr(err)

	err = os.RemoveAll(dir)
	noErr(err)
}

func RealCPUUsageDataConsequentAppendWriteFTSDB(logger *zap.Logger, cpuData []transformer.CPUData) {
	series := labels.FromStrings("host", "macbook")

	tsdb := ftsdb.NewFTSDB(logger, GetIngestionDir())

	query := ftsdb.Query{}
	query.Series(series.Map())

	metric := tsdb.CreateMetric("mayur")
	for _, data := range cpuData {
		metric.Append(series.Map(), data.Timestamp, data.CPUUsage)
		FTSDBIterateAll(tsdb.Find(query))
	}
}

func RealCPUUsageRangeDataPrometheusTSDB(logger *zap.Logger, cpuData []transformer.CPUData) {
	series := labels.FromStrings("host", "macbook")
	seriesMatcher := labels.MustNewMatcher(labels.MatchEqual, "host", "macbook")

	dir, err := os.MkdirTemp("", "tsdb-test")
	noErr(err)

	db, err := tsdb.Open(dir, nil, nil, tsdb.DefaultOptions(), nil)
	noErr(err)

	app := db.Appender(context.Background())

	for _, data := range cpuData {
		app.Append(0, series, data.Timestamp, data.CPUUsage)
	}

	// err = app.Commit()
	// noErr(err)

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
}

func RealCPUUsageRangeDataFTSDB(logger *zap.Logger, cpuData []transformer.CPUData) {
	series := labels.FromStrings("host", "macbook")

	tsdb := ftsdb.NewFTSDB(logger, GetIngestionDir())

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
}

func AppendMillionPointsPrometheusTSDB() {
	dir, err := os.MkdirTemp("", "tsdb-test")
	noErr(err)

	db, err := tsdb.Open(dir, nil, nil, tsdb.DefaultOptions(), nil)
	noErr(err)

	app := db.Appender(context.Background())

	series := labels.FromStrings("foo", "bar")

	for i := 0; i <= 1000000; i++ {
		app.Append(0, series, int64(i), 0.1)
	}

	// err = app.Commit()
	// noErr(err)

	err = db.Close()
	noErr(err)

	err = os.RemoveAll(dir)
	noErr(err)
}

func AppendMillionPointsFTSDB(logger *zap.Logger) {
	tsdb := ftsdb.NewFTSDB(logger, GetIngestionDir())
	m := tsdb.CreateMetric("met")
	for i := 1; i <= 1000000; i++ {
		m.Append(map[string]string{"foo": "bar"}, int64(i), 0.1)
	}
}

func AppendPointsWithLabelsFTSDB(logger *zap.Logger, points int) {
	tsdb := ftsdb.NewFTSDB(logger, GetIngestionDir())
	m := tsdb.CreateMetric("met")
	for i := 1; i <= points; i++ {
		series := map[string]string{}
		for j := 1; j <= i; j++ {
			key := fmt.Sprintf("series-%d", j)
			value := fmt.Sprintf("%d", rand.Intn(100))

			series[key] = value
		}
		m.Append(series, int64(i), 0.1)
	}
}

func AppendPointsWithLabelsPrometheusTSDB(points int) {
	dir, err := os.MkdirTemp("", "tsdb-test")
	noErr(err)

	db, err := tsdb.Open(dir, nil, nil, tsdb.DefaultOptions(), nil)
	noErr(err)

	app := db.Appender(context.Background())

	for i := 1; i <= points; i++ {
		series := map[string]string{}
		for j := 1; j <= i; j++ {
			key := fmt.Sprintf("series-%d", j)
			value := fmt.Sprintf("%d", rand.Intn(100))

			series[key] = value
		}
		app.Append(0, labels.FromMap(series), int64(i), 0.1)
	}

	// err = app.Commit()
	// noErr(err)

	err = db.Close()
	noErr(err)

	err = os.RemoveAll(dir)
	noErr(err)
}

func HeavyAppendWriteDiskPrometheusTSDB(seriesList []map[string]int, points int) {
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
			for i = 0; i < int64(points); i++ {
				app.Append(0, __series, i, float64(i))
			}
		}
	}

	err = app.Commit()
	noErr(err)

	// querier, err := db.Querier(math.MinInt64, math.MaxInt64)
	// noErr(err)

	// for _, seriesIn := range seriesList {
	// 	for key, val := range seriesIn {
	// 		PrometheusTSDBFindIterateAll(querier.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, key, fmt.Sprintf("%d", val))))
	// 	}
	// }
	err = db.Close()
	noErr(err)

	err = os.RemoveAll(dir)
	noErr(err)
}

func HeavyAppendWriteDiskFTSDB(logger *zap.Logger, seriesList []map[string]int, points int) {
	tsdb := ftsdb.NewFTSDB(logger, GetIngestionDir())

	metric := tsdb.CreateMetric("jay")

	for _, seriesIn := range seriesList {
		for key, val := range seriesIn {
			__series := map[string]string{}

			__series[key] = fmt.Sprint(val)
			for i := 0; i < points; i++ {
				metric.Append(__series, int64(i), float64(i))
			}
		}
	}

	noErr(tsdb.Commit())

	err := os.RemoveAll(GetIngestionDir())
	noErr(err)
}
