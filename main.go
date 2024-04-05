package main

import (
	"github.com/Marvin9/ftsdb/experiments"
	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewProduction()

	experiments.Experiment(
		"Append in 10000 points in 2 series",
		"./results/append-100-points-in-2-series.html",
		100,
		func() {
			for i := 0; i < 1000; i++ {
				experiments.BasicFTSDB(logger)
			}
		},
		func() {
			for i := 0; i < 1000; i++ {
				experiments.BasicPrometheus()
			}
		},
	)

	experiments.Experiment(
		"Append in 1000000 points in 2 series and making range start call",
		"./results/append-100-points-in-2-series-and-range-start-query.html",
		10,
		func() {
			for i := 0; i < 10; i++ {
				experiments.RangeFTSDB(logger)
			}
		},
		func() {
			for i := 0; i < 10; i++ {
				experiments.RangePrometheusTSDB()
			}
		},
	)

	experiments.Experiment(
		"Append in 1000000 points in 2 series and making range start and end call",
		"./results/append-100-points-in-2-series-and-range-start-and-end-query.html",
		10,
		func() {
			for i := 0; i < 10; i++ {
				experiments.RangesFTSDB(logger)
			}
		},
		func() {
			for i := 0; i < 10; i++ {
				experiments.RangesPrometheusTSDB()
			}
		},
	)
}
