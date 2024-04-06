package main

import (
	"github.com/Marvin9/ftsdb/experiments"
	"github.com/Marvin9/ftsdb/transformer"
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
		50,
		func() {
			experiments.RangeFTSDB(logger)
		},
		func() {
			experiments.RangePrometheusTSDB()
		},
	)

	experiments.Experiment(
		"Append in 1000000 points in 2 series and making range start and end call",
		"./results/append-100-points-in-2-series-and-range-start-and-end-query.html",
		50,
		func() {
			experiments.RangesFTSDB(logger)
		},
		func() {
			experiments.RangesPrometheusTSDB()
		},
	)

	seriesList := experiments.GetHeavySeriesList(20)

	experiments.Experiment(
		"Append 100K data points in 20 series",
		"./results/append-100k-dp-in-20-series.html",
		50,
		func() {
			experiments.HeavyAppendFTSDB(logger, seriesList, 100000)

		},
		func() {
			experiments.HeavyAppendPrometheusTSDB(seriesList, 100000)

		},
	)

	seriesList = experiments.GetHeavySeriesList(20)

	experiments.Experiment(
		"Append 100K data points in 20 series and write to disk",
		"./results/append-100k-dp-in-20-series-with-disk.html",
		100,
		func() {
			experiments.HeavyAppendWriteDiskFTSDB(logger, seriesList, 100000)

		},
		func() {
			experiments.HeavyAppendWriteDiskPrometheusTSDB(seriesList, 100000)
		},
	)

	dataTransformer := transformer.NewDataTransformer(logger)

	cpuData := dataTransformer.GenCPUData(100000)

	experiments.Experiment(
		"100K CPU Usage",
		"./results/100k-cpu-usage.html",
		100,
		func() {
			experiments.RealCPUUsageDataFTSDB(logger, cpuData)
		},
		func() {
			experiments.RealCPUUsageDataPrometheusTSDB(cpuData, logger)
		},
	)

	experiments.Experiment(
		"Consequent writes",
		"./results/cpu-usage-consequent-writes.html",
		100,
		func() {
			experiments.RealCPUUsageDataConsequentAppendWriteFTSDB(logger, cpuData[:10000])
		},
		func() {
			experiments.RealCPUUsageDataConsequentAppendWritePrometheusTSDB(logger, cpuData[:10000])
		},
	)

	experiments.Experiment(
		"Range data",
		"./results/cpu-usage-range-data.html",
		100,
		func() {
			experiments.RealCPUUsageRangeDataFTSDB(logger, cpuData)
		},
		func() {
			experiments.RealCPUUsageRangeDataPrometheusTSDB(logger, cpuData)
		},
	)

	experiments.Experiment(
		"Append million points",
		"./results/append-million-points.html",
		100,
		func() {
			experiments.AppendMillionPointsFTSDB(logger)
		},
		func() {
			experiments.AppendMillionPointsPrometheusTSDB()
		},
	)

	experiments.Experiment(
		"Append 10k points with label",
		"./results/append-million-points-with-label.html",
		100,
		func() {
			experiments.AppendPointsWithLabelsFTSDB(logger, 10000)
		},
		func() {
			experiments.AppendPointsWithLabelsPrometheusTSDB(10000)
		},
	)
}
