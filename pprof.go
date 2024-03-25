package main

import (
	"github.com/Marvin9/ftsdb/transformer"
	"go.uber.org/zap"
)

func pprofFTSDB() {
	logger, _ := zap.NewProduction()
	seriesList := getHeavySeriesList(100)
	dataTransformer := transformer.NewDataTransformer(logger)

	cpuData := dataTransformer.GenCPUData(100000)

	BasicFTSDB(logger)
	RangeFTSDB(logger)
	RangesFTSDB(logger)
	HeavyAppendFTSDB(logger, seriesList, 10000)
	RealCPUUsageDataFTSDB(logger, cpuData)
	RealCPUUsageDataConsequentAppendWriteFTSDB(logger, cpuData[:1000])
	RealCPUUsageRangeDataFTSDB(logger, cpuData[:10000])
}

func pprofPrometheusTSDB() {
	logger, _ := zap.NewProduction()
	seriesList := getHeavySeriesList(100)
	dataTransformer := transformer.NewDataTransformer(logger)

	cpuData := dataTransformer.GenCPUData(100000)

	BasicPrometheus()
	RangePrometheusTSDB()
	RangesPrometheusTSDB()
	HeavyAppendFTSDB(logger, seriesList, 10000)
	RealCPUUsageDataPrometheusTSDB(cpuData, logger)
	RealCPUUsageDataConsequentAppendWritePrometheusTSDB(logger, cpuData[:1000])
	RealCPUUsageRangeDataPrometheusTSDB(logger, cpuData[:10000])
}
