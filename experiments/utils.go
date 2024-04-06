package experiments

import (
	"os"
	"runtime"
	"time"

	"github.com/Marvin9/ftsdb/shared"
)

func GetIngestionDir() string {
	return shared.GetIngestionDir()
}

func noErr(err error) {
	shared.NoErr(err)
}

func Experiment(
	subtitle string,
	filepath string,
	statsInterval int,
	ftsdbExecutor,
	prometheusExecutor func(),
) {
	runtime.GC()
	prometheusStats := NewStats()
	stop := prometheusStats.StartMonitoring(statsInterval)
	prometheusExecutor()
	time.Sleep(time.Second)
	stop()
	runtime.GC()

	ftsdbStats := NewStats()
	stop = ftsdbStats.StartMonitoring(statsInterval)
	ftsdbExecutor()
	time.Sleep(time.Second)
	stop()
	runtime.GC()

	Plot(PlotOpts{
		Subtitle:        subtitle,
		FtsdbStats:      ftsdbStats,
		PrometheusStats: prometheusStats,
		Filepath:        filepath,
	})

	os.RemoveAll(GetIngestionDir())
}
