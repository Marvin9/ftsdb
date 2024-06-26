package experiments

import (
	"os"
	"path/filepath"
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
	prometheusExecutor func() string,
) {
	noErr(os.RemoveAll(shared.GetPromIngestionDir()))
	noErr(os.RemoveAll(GetIngestionDir()))

	runtime.GC()
	prometheusStats := NewStats()
	stop := prometheusStats.StartMonitoring(statsInterval)
	dir := prometheusExecutor()
	time.Sleep(time.Second)
	stop()
	prometheusStats.RunningTime -= 1000
	size, _ := getFolderSize(dir)
	prometheusStats.DiskSize = int(size)
	runtime.GC()

	ftsdbStats := NewStats()
	stop = ftsdbStats.StartMonitoring(statsInterval)
	ftsdbExecutor()
	time.Sleep(time.Second)
	stop()
	ftsdbStats.RunningTime -= 1000
	size, _ = getFolderSize(shared.GetIngestionDir())
	ftsdbStats.DiskSize = int(size)
	runtime.GC()

	// fmt.Println(prometheusStats.DiskSize, ftsdbStats.DiskSize)

	Plot(PlotOpts{
		Subtitle:        subtitle,
		FtsdbStats:      ftsdbStats,
		PrometheusStats: prometheusStats,
		Filepath:        filepath,
	})

	noErr(os.RemoveAll(shared.GetPromIngestionDir()))
	noErr(os.RemoveAll(GetIngestionDir()))
}

func getFolderSize(folderPath string) (int64, error) {
	var folderSize int64

	// Walk through the folder and its subfolders
	err := filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// Add file size to folder size
		if !info.IsDir() {
			folderSize += info.Size()
		}
		return nil
	})
	if err != nil {
		return 0, err
	}

	return folderSize, nil
}

func toMegaBytes(n int) float64 {
	return float64(n) * 0.000001
}

func toSeconds(n int) float64 {
	return float64(n) * 0.001
}

func CleanupDir() {
	noErr(os.RemoveAll(shared.GetPromIngestionDir()))
	noErr(os.RemoveAll(GetIngestionDir()))
}
