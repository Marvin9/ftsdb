package main

import (
	"fmt"
	"os"

	"github.com/Marvin9/ftsdb/ftsdb"
	"go.uber.org/zap"
)

/*
TSDB

timestamp - metric - field1=value1, field2=value2 field3=value3 - value

metric-1
timestamp - field1=value1 - value
timestamp - field2=value2 - value
timestamp - field1=value3 field3=value4 - value
....

CPU
1 - node=host-1 - 0.06
2 - node=host-1 - 0.1
3 - node=host-1 - 0.5
4 - node=host-2 - 0.1
....

RAM
1 - node=host-1 - 0.06
2 - node=host-1 - 0.1
3 - node=host-1 - 0.5
4 - node=host-2 - 0.1
...

http_requests
1 - path=/api - 1
1 - path=/api - 1
1 - path=/api - 1
1 - path=/api - 1
1 - path=/api/foo - 1
....

metric := CreateMetric(metric string)

series := map[string]interface{}

metric.Append(series, timestamp, value)

db.Find(metric string)
db.Find(rangeStart int64, rangeEnd int64, metric string)
db.Find(metric string, series map[string]interface{})
db.Find(rangeStart int64, rangeEnd int64, metric string, series map[string]interface{})

db.Delete(metric string)
db.Delete(rangeStart int64, rangeEnd int64, metric string)
db.Delete(metric string, series map[string]interface{})
db.Delete(rangeStart int64, rangeEnd int64, metric string, series map[string]interface{})
*/

func main() {
	// pprofFTSDB()
	// pprofPrometheusTSDB()

	// logger, _ := zap.NewDevelopment()

	// seriesMac := map[string]string{
	// 	"host": "macbook",
	// }
	// seriesWin := map[string]string{
	// 	"host": "wind",
	// }

	// tsdb := ftsdb.NewFTSDB(logger, GetIngestionDir())

	// metric := tsdb.CreateMetric("jay")

	// var i int64
	// for i = 0; i < 1000000; i++ {
	// 	metric.Append(seriesMac, int64(i), float64(i))
	// 	metric.Append(seriesWin, int64(i), float64(i))
	// }

	// fmt.Println(tsdb.Commit())

	// db, err := tsdb.Open(fmt.Sprintf("%s/tmp", GetIngestionDir()), nil, nil, tsdb.DefaultOptions(), nil)
	// noErr(err)

	// app := db.Appender(context.Background())

	// seriesMac := labels.FromStrings("host", "macbook")
	// seriesWin := labels.FromStrings("host", "wind")

	// for i := int64(0); i < 1000000; i++ {
	// 	app.Append(0, seriesMac, i, float64(i))
	// 	app.Append(0, seriesWin, i, float64(i))

	// }

	// noErr(app.Commit())

	// noErr(db.Close())
}

func _main() {
	logger, _ := zap.NewProduction()

	logger.Info("process-id", zap.Int("id", os.Getpid()))

	// time.Sleep(time.Second * 20)

	seriesMac := map[string]string{
		"host": "macbook",
	}
	seriesWin := map[string]string{
		"host": "wind",
	}

	tsdb := ftsdb.NewFTSDB(logger, GetIngestionDir())

	metric := tsdb.CreateMetric("jay")

	var i int64
	for i = 0; i < 1000; i++ {
		metric.Append(seriesMac, int64(i), float64(i))
		metric.Append(seriesWin, int64(i), float64(i))
		// metric2.Append(seriesMac, int64(i), float64(i))
	}

	logger.Info("executing search query")

	query := &ftsdb.Query{}
	// // query = query.Metric("jay")
	query.Series(seriesMac)
	for i := 0; i < 100000; i++ {
		query.RangeStart(int64(i))
		tsdb.Find(*query)
		fmt.Println("query done ", i)
	}

	tsdb.DisplayMetrics()
}

func noErr(err error) {
	if err != nil {
		panic(err)
	}
}
