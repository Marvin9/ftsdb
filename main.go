package main

import (
	"strconv"

	"github.com/Marvin9/ftsdb/experiments"
	"github.com/Marvin9/ftsdb/transformer"
	"github.com/prometheus/prometheus/model/labels"
	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewProduction()

	experiments.Experiment(
		"Append 1M points in 2 series",
		"./results/append-100-points-in-2-series.html",
		100,
		func() string {
			for i := 0; i < 10; i++ {
				experiments.BasicFTSDB(logger)
			}
			return ""
		},
		func() string {
			var file string
			for i := 0; i < 10; i++ {
				file = experiments.BasicPrometheus()
			}
			return file
		},
	)

	experiments.Experiment(
		"Append in 1M points in 2 series and making range start query",
		"./results/append-100-points-in-2-series-and-range-start-query.html",
		50,
		func() string {
			experiments.RangeFTSDB(logger)
			return ""
		},
		func() string {
			return experiments.RangePrometheusTSDB()
		},
	)

	experiments.Experiment(
		"Append in 1M points in 2 series and making range start and end query",
		"./results/append-100-points-in-2-series-and-range-start-and-end-query.html",
		50,
		func() string {
			experiments.RangesFTSDB(logger)
			return ""
		},
		func() string {
			return experiments.RangesPrometheusTSDB()
		},
	)

	seriesList := experiments.GetHeavySeriesList(20)

	experiments.Experiment(
		"Append 100K data points in 20 series",
		"./results/append-100k-dp-in-20-series.html",
		50,
		func() string {
			experiments.HeavyAppendFTSDB(logger, seriesList, 100000)
			return ""

		},
		func() string {
			return experiments.HeavyAppendPrometheusTSDB(seriesList, 100000)
		},
	)

	seriesList = experiments.GetHeavySeriesList(20)

	experiments.Experiment(
		"Append 100K data points in 20 series and write to disk",
		"./results/append-100k-dp-in-20-series-with-disk.html",
		50,
		func() string {
			experiments.HeavyAppendWriteDiskFTSDB(logger, seriesList, 100000)
			return ""
		},
		func() string {
			return experiments.HeavyAppendWriteDiskPrometheusTSDB(seriesList, 100000)
		},
	)

	dataTransformer := transformer.NewDataTransformer(logger)

	cpuData := dataTransformer.GenCPUData("./data/cpu_usage.json", 100000)

	experiments.Experiment(
		"Append 100K CPU Usage data in disk",
		"./results/100k-cpu-usage.html",
		50,
		func() string {
			experiments.RealCPUUsageDataFTSDB(logger, cpuData)
			return ""
		},
		func() string {
			return experiments.RealCPUUsageDataPrometheusTSDB(cpuData, logger)
		},
	)

	experiments.Experiment(
		"Append 10k CPU Usage data consequently in disk",
		"./results/cpu-usage-consequent-writes.html",
		50,
		func() string {
			experiments.RealCPUUsageDataConsequentAppendWriteFTSDB(logger, cpuData[:10000])
			return ""
		},
		func() string {
			return experiments.RealCPUUsageDataConsequentAppendWritePrometheusTSDB(logger, cpuData[:10000])
		},
	)

	experiments.Experiment(
		"Append 10k cpu usage data and make range query",
		"./results/cpu-usage-range-data.html",
		50,
		func() string {
			experiments.RealCPUUsageRangeDataFTSDB(logger, cpuData[:10000])
			return ""
		},
		func() string {
			return experiments.RealCPUUsageRangeDataPrometheusTSDB(logger, cpuData[:10000])
		},
	)

	experiments.Experiment(
		"Append million points",
		"./results/append-million-points.html",
		50,
		func() string {
			experiments.AppendMillionPointsFTSDB(logger)
			return ""
		},
		func() string {
			return experiments.AppendMillionPointsPrometheusTSDB()
		},
	)

	experiments.Experiment(
		"Append 10k points with label",
		"./results/append-million-points-with-label.html",
		100,
		func() string {
			experiments.AppendPointsWithLabelsFTSDB(logger, 10000)
			return ""
		},
		func() string {
			return experiments.AppendPointsWithLabelsPrometheusTSDB(10000)
		},
	)

	var fSeriesList []map[string]string
	for _, m := range seriesList {
		convertedMap := make(map[string]string)
		for k, v := range m {
			convertedMap[k] = strconv.Itoa(v)
		}
		fSeriesList = append(fSeriesList, convertedMap)
	}

	var pSeriesList []labels.Labels
	for _, m := range fSeriesList {
		pSeriesList = append(pSeriesList, labels.FromMap(m))
	}
	ramData := dataTransformer.GenCPUData("./data/ram_usage.json", 100000)

	experiments.Experiment(
		"Append 100K CPU and RAM usage data in disk",
		"./results/100k-cpu-ram-usage.html",
		50,
		func() string {
			experiments.RealRAMUsageDataFTSDB(logger, cpuData, ramData, fSeriesList)
			return ""
		},
		func() string {
			return experiments.RealRAMUsageDataPrometheusTSDB(cpuData, ramData, pSeriesList)
		},
	)
}
