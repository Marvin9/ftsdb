package experiments

import (
	"os"
	"path/filepath"
	"runtime"
)

func GetIngestionDir() string {
	dir, _ := os.Getwd()
	dir = filepath.Join(dir, "ingestion")
	return dir
}

func noErr(err error) {
	if err != nil {
		panic(err)
	}
}

func Experiment(
	subtitle string,
	filepath string,
	statsInterval int,
	ftsdbExecutor,
	prometheusExecutor func(),
) {
	ftsdbStats := NewStats()
	stop := ftsdbStats.StartMonitoring(statsInterval)
	ftsdbExecutor()
	stop()
	runtime.GC()

	prometheusStats := NewStats()
	stop = prometheusStats.StartMonitoring(statsInterval)
	prometheusExecutor()
	stop()
	runtime.GC()

	Plot(PlotOpts{
		Subtitle:        subtitle,
		FtsdbStats:      ftsdbStats,
		PrometheusStats: prometheusStats,
		Filepath:        filepath,
	})

	runtime.GC()
}
