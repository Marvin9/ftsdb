package experiments

import (
	"context"
	"fmt"
	"math"
	"math/rand"

	"github.com/Marvin9/ftsdb/ftsdb"
	"github.com/Marvin9/ftsdb/shared"
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

func GetHeavySeriesList(_i int) []map[string]int {
	return getHeavySeriesList(_i)
}

func getHeavySeries(_i int) map[string]int {
	series := make(map[string]int)

	for i := _i; i < _i+1; i++ {
		seriesName := fmt.Sprintf("series-%d", i)
		series[seriesName] = rand.Intn(100)
	}

	return series
}

func PrometheusTSDBFindIterateAll(selector storage.SeriesSet) int {
	tot := 0
	var __series labels.Labels
	for selector.Next() {
		series := selector.At()
		if __series.Len() == 0 {
			__series = series.Labels()
		}

		it := series.Iterator(nil)

		// fmt.Println(series.Labels())

		for it.Next() == chunkenc.ValFloat {
			tot++
			// fmt.Println(it.At())
		}
	}

	// fmt.Println("prom", tot)
	return tot
}

func FTSDBIterateAll(ss *ftsdb.SeriesIterator) int {
	tot := 0
	for ss.Next() != nil {
		it := ss.DatapointsIterator

		// fmt.Println(ss.GetSeries())
		for it.Next() != nil {
			// fmt.Println(it.GetDatapoint())
			tot++
		}
	}
	// fmt.Println("ftsdb", tot)
	return tot
}

func BasicPrometheus() string {
	dir := shared.GetPromIngestionDir()

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

	querier, err := db.Querier(math.MinInt64, math.MaxInt64)
	noErr(err)

	PrometheusTSDBFindIterateAll(querier.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, "host", "macbook")))

	PrometheusTSDBFindIterateAll(querier.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, "host", "wind")))

	err = db.Close()
	noErr(err)

	return dir
}

func BasicFTSDB(logger *zap.Logger) {
	seriesMac := map[string]string{
		"host": "macbook",
	}
	seriesWin := map[string]string{
		"host": "wind",
	}

	tsdb := ftsdb.NewFTSDB(logger, GetIngestionDir())

	defer tsdb.Close()

	metric := tsdb.CreateMetric("jay")

	var i int64
	for i = 0; i < 1000000; i++ {
		metric.Append(seriesMac, int64(i), float64(i))
		metric.Append(seriesWin, int64(i), float64(i))
	}

	tsdb.Commit()

	query := ftsdb.Query{}
	query.Series(seriesMac)

	FTSDBIterateAll(tsdb.Find(query))

	query.Series(seriesWin)

	FTSDBIterateAll(tsdb.Find(query))
}

func RangePrometheusTSDB() string {
	dir := shared.GetPromIngestionDir()

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

	return dir
}

func RangeFTSDB(logger *zap.Logger) {
	seriesMac := map[string]string{
		"host": "macbook",
	}
	seriesWin := map[string]string{
		"host": "wind",
	}

	tsdb := ftsdb.NewFTSDB(logger, GetIngestionDir())
	defer tsdb.Close()

	metric := tsdb.CreateMetric("jay")

	var i int64
	for i = 0; i < 1000000; i++ {
		metric.Append(seriesMac, int64(i), float64(i))
		metric.Append(seriesWin, int64(i), float64(i))
	}

	tsdb.Commit()

	query := ftsdb.Query{}
	query.RangeStart(500000)
	query.Series(seriesMac)

	FTSDBIterateAll(tsdb.Find(query))

	query.Series(seriesWin)

	FTSDBIterateAll(tsdb.Find(query))
}

func RangesPrometheusTSDB() string {
	dir := shared.GetPromIngestionDir()

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

	return dir
}

func RangesFTSDB(logger *zap.Logger) {
	seriesMac := map[string]string{
		"host": "macbook",
	}
	seriesWin := map[string]string{
		"host": "wind",
	}

	tsdb := ftsdb.NewFTSDB(logger, GetIngestionDir())
	defer tsdb.Close()

	metric := tsdb.CreateMetric("jay")

	var i int64
	for i = 0; i < 1000000; i++ {
		metric.Append(seriesMac, int64(i), float64(i))
		metric.Append(seriesWin, int64(i), float64(i))
	}

	tsdb.Commit()

	query := ftsdb.Query{}
	query.RangeStart(500000)
	query.RangeEnd(510000)
	query.Series(seriesMac)

	FTSDBIterateAll(tsdb.Find(query))

	query.Series(seriesWin)

	FTSDBIterateAll(tsdb.Find(query))
}

func HeavyAppendPrometheusTSDB(seriesList []map[string]int, points int) string {
	dir := shared.GetPromIngestionDir()

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

	querier, err := db.Querier(math.MinInt64, math.MaxInt64)
	noErr(err)

	for _, seriesIn := range seriesList {
		for key, val := range seriesIn {
			PrometheusTSDBFindIterateAll(querier.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, key, fmt.Sprintf("%d", val))))
		}
	}

	err = db.Close()
	noErr(err)

	return dir
}

func HeavyAppendFTSDB(logger *zap.Logger, seriesList []map[string]int, points int) {
	tsdb := ftsdb.NewFTSDB(logger, GetIngestionDir())
	defer tsdb.Close()

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

	tsdb.Commit()

	query := ftsdb.Query{}

	for _, seriesIn := range seriesList {
		for key, val := range seriesIn {
			__series := map[string]string{}

			__series[key] = fmt.Sprintf("%d", val)
			query.Series(__series)

			FTSDBIterateAll(tsdb.Find(query))
		}
	}
}

func RealCPUUsageDataPrometheusTSDB(cpuData []transformer.CPUData, logger *zap.Logger) string {
	series := labels.FromStrings("host", "macbook")

	dir := shared.GetPromIngestionDir()

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

	return dir
}

func RealCPUUsageDataFTSDB(logger *zap.Logger, cpuData []transformer.CPUData) {
	series := labels.FromStrings("host", "macbook")

	tsdb := ftsdb.NewFTSDB(logger, GetIngestionDir())
	defer tsdb.Close()

	metric := tsdb.CreateMetric("mayur")
	for _, data := range cpuData {
		metric.Append(series.Map(), data.Timestamp, data.CPUUsage)
	}

	noErr(tsdb.Commit())
	query := ftsdb.Query{}
	query.Series(series.Map())
	FTSDBIterateAll(tsdb.Find(query))
}

func RealCPUUsageDataConsequentAppendWritePrometheusTSDB(logger *zap.Logger, cpuData []transformer.CPUData) string {
	series := labels.FromStrings("host", "macbook")
	seriesMatcher := labels.MustNewMatcher(labels.MatchEqual, "host", "macbook")

	dir := shared.GetPromIngestionDir()

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

	return dir
}

func RealCPUUsageDataConsequentAppendWriteFTSDB(logger *zap.Logger, cpuData []transformer.CPUData) {
	series := labels.FromStrings("host", "macbook")

	tsdb := ftsdb.NewFTSDB(logger, GetIngestionDir())
	defer tsdb.Close()

	query := ftsdb.Query{}
	query.Series(series.Map())

	for _, data := range cpuData {
		metric := tsdb.CreateMetric("mayur")
		metric.Append(series.Map(), data.Timestamp, data.CPUUsage)
		tsdb.Commit()
		FTSDBIterateAll(tsdb.Find(query))
	}
}

func RealCPUUsageRangeDataPrometheusTSDB(logger *zap.Logger, cpuData []transformer.CPUData) string {
	series := labels.FromStrings("host", "macbook")
	seriesMatcher := labels.MustNewMatcher(labels.MatchEqual, "host", "macbook")

	dir := shared.GetPromIngestionDir()

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

	return dir
}

func RealCPUUsageRangeDataFTSDB(logger *zap.Logger, cpuData []transformer.CPUData) {
	series := labels.FromStrings("host", "macbook")

	tsdb := ftsdb.NewFTSDB(logger, GetIngestionDir())
	defer tsdb.Close()

	metric := tsdb.CreateMetric("mayur")
	for _, data := range cpuData {
		metric.Append(series.Map(), data.Timestamp, data.CPUUsage)
	}

	tsdb.Commit()

	query := ftsdb.Query{}
	query.Series(series.Map())
	query.RangeStart(cpuData[5000].Timestamp)

	FTSDBIterateAll(tsdb.Find(query))

	query.RangeStart(math.MinInt64)
	query.RangeEnd(cpuData[5000].Timestamp)

	FTSDBIterateAll(tsdb.Find(query))
}

func AppendMillionPointsPrometheusTSDB() string {
	dir := shared.GetPromIngestionDir()

	db, err := tsdb.Open(dir, nil, nil, tsdb.DefaultOptions(), nil)
	noErr(err)

	app := db.Appender(context.Background())

	series := labels.FromStrings("foo", "bar")

	for i := 0; i <= 1000000; i++ {
		app.Append(0, series, int64(i), 0.1)
	}

	err = app.Commit()
	noErr(err)

	err = db.Close()
	noErr(err)

	return dir
}

func AppendMillionPointsFTSDB(logger *zap.Logger) {
	tsdb := ftsdb.NewFTSDB(logger, GetIngestionDir())
	defer tsdb.Close()
	m := tsdb.CreateMetric("met")
	for i := 1; i <= 1000000; i++ {
		m.Append(map[string]string{"foo": "bar"}, int64(i), 0.1)
	}

	noErr(tsdb.Commit())
}

func AppendPointsWithLabelsFTSDB(logger *zap.Logger, points int) {
	tsdb := ftsdb.NewFTSDB(logger, GetIngestionDir())
	defer tsdb.Close()
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

	noErr(tsdb.Commit())
}

func AppendPointsWithLabelsPrometheusTSDB(points int) string {
	dir := shared.GetPromIngestionDir()

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

	err = app.Commit()
	noErr(err)

	err = db.Close()
	noErr(err)

	return dir
}

func HeavyAppendWriteDiskPrometheusTSDB(seriesList []map[string]int, points int) string {
	dir := shared.GetPromIngestionDir()

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

	err = db.Close()
	noErr(err)

	return dir
}

func HeavyAppendWriteDiskFTSDB(logger *zap.Logger, seriesList []map[string]int, points int) {
	tsdb := ftsdb.NewFTSDB(logger, GetIngestionDir())
	defer tsdb.Close()

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
}

func RealRAMUsageDataPrometheusTSDB(cpuData, ramData []transformer.CPUData, seriesList []labels.Labels) string {
	dir := shared.GetPromIngestionDir()

	db, err := tsdb.Open(dir, nil, nil, tsdb.DefaultOptions(), nil)
	noErr(err)

	app := db.Appender(context.Background())

	for _, series := range seriesList {
		sseries := series.Map()
		sseries["metric"] = "cpu"
		for _, data := range cpuData {
			app.Append(0, series, data.Timestamp, data.CPUUsage)
		}
	}

	err = app.Commit()
	noErr(err)

	app = db.Appender(context.Background())

	for _, series := range seriesList {
		sseries := series.Map()
		sseries["metric"] = "ram"
		for _, data := range ramData {
			app.Append(0, labels.FromMap(sseries), data.Timestamp, data.RAMUsage)
		}
	}

	err = app.Commit()
	noErr(err)

	querier, err := db.Querier(math.MinInt64, math.MaxInt64)
	noErr(err)

	for _, series := range seriesList {
		all := series.Map()
		for k, v := range all {
			PrometheusTSDBFindIterateAll(querier.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, k, v)))
		}
	}

	return dir
}

func RealRAMUsageDataFTSDB(logger *zap.Logger, cpuData, ramData []transformer.CPUData, seriesList []map[string]string) {
	tsdb := ftsdb.NewFTSDB(logger, GetIngestionDir())
	defer tsdb.Close()

	metric := tsdb.CreateMetric("mayur")
	for _, series := range seriesList {
		series["metric"] = "cpu"
		for _, data := range cpuData {
			metric.Append(series, data.Timestamp, data.CPUUsage)
		}
		delete(series, "metric")
	}

	noErr(tsdb.Commit())

	metric = tsdb.CreateMetric("mayur")

	for _, series := range seriesList {
		series["metric"] = "ram"
		for _, data := range ramData {
			metric.Append(series, data.Timestamp, data.RAMUsage)
		}
		delete(series, "metric")
	}

	noErr(tsdb.Commit())

	query := ftsdb.Query{}

	for _, series := range seriesList {
		all := series
		for k, v := range all {
			query.Series(map[string]string{k: v})
			FTSDBIterateAll(tsdb.Find(query))
		}
	}
}
