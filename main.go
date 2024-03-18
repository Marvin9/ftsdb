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
	logger, _ := zap.NewProduction()

	logger.Info("process-id", zap.Int("id", os.Getpid()))

	// time.Sleep(time.Second * 20)

	seriesMac := map[string]interface{}{
		"host": "macbook",
	}
	seriesWin := map[string]interface{}{
		"host": "wind",
	}

	tsdb := ftsdb.NewFTSDB(logger)

	metric := tsdb.CreateMetric("jay")

	var i int64
	for i = 0; i < 1000; i++ {
		metric.Append(seriesMac, "mac", int64(i), float64(i))
		metric.Append(seriesWin, "win", int64(i), float64(i))
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
