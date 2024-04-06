package experiments

import (
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/components"
	"github.com/go-echarts/go-echarts/v2/opts"
)

type PlotOpts struct {
	Title           string
	Subtitle        string
	FtsdbStats      *stats
	PrometheusStats *stats
	Filepath        string
	Category        string
}

func Plot(plotOpts PlotOpts) {
	title := plotOpts.Title
	subtitle := plotOpts.Subtitle
	ftsdbStats := plotOpts.FtsdbStats
	prometheusStats := plotOpts.PrometheusStats

	page := components.NewPage()

	CPULine := charts.NewLine()

	CPULine.SetGlobalOptions(
		charts.WithTitleOpts(opts.Title{
			Title:    title,
			Subtitle: subtitle,
		}),
	)

	minXAxis := int(math.Min(float64(len(ftsdbStats.Data)), float64(len(prometheusStats.Data))))

	xAxis := []string{}
	ftsdbSeriesCPU := make([]opts.LineData, 0)
	promSeriesCPU := make([]opts.LineData, 0)

	for i := 0; i < minXAxis; i++ {
		xAxis = append(xAxis, fmt.Sprintf("%d", ftsdbStats.Data[i].Elapsed))

		ftsdbSeriesCPU = append(ftsdbSeriesCPU, opts.LineData{
			Value: ftsdbStats.Data[i].CPU,
		})

		promSeriesCPU = append(promSeriesCPU, opts.LineData{
			Value: prometheusStats.Data[i].CPU,
		})
	}

	CPULine.SetXAxis(xAxis).
		AddSeries("ftsdb-cpu", ftsdbSeriesCPU).
		AddSeries("prometheus-cpu", promSeriesCPU)

	// ------------------------

	MemoryLine := charts.NewLine()

	MemoryLine.SetGlobalOptions(
		charts.WithTitleOpts(opts.Title{
			Title:    title,
			Subtitle: subtitle,
		}),
	)

	ftsdbSeriesMemory := make([]opts.LineData, 0)
	promSeriesMemory := make([]opts.LineData, 0)

	for i := 0; i < minXAxis; i++ {
		ftsdbSeriesMemory = append(ftsdbSeriesMemory, opts.LineData{
			Value: ftsdbStats.Data[i].Mem.Alloc,
			Name:  fmt.Sprintf("%.2fMB", float64(ftsdbStats.Data[i].Mem.Alloc)*0.000001),
		})

		promSeriesMemory = append(promSeriesMemory, opts.LineData{
			Value: prometheusStats.Data[i].Mem.Alloc,
			Name:  fmt.Sprintf("%.2fMB", float64(prometheusStats.Data[i].Mem.Alloc)*0.000001),
		})
	}

	MemoryLine.SetXAxis(xAxis).
		AddSeries("ftsdb-memoery", ftsdbSeriesMemory).
		AddSeries("prometheus-memory", promSeriesMemory)

	// ----------------------------

	HeapLine := charts.NewLine()

	HeapLine.SetGlobalOptions(
		charts.WithTitleOpts(opts.Title{
			Title:    title,
			Subtitle: subtitle,
		}),
	)

	ftsdbHeap := make([]opts.LineData, 0)
	promHeap := make([]opts.LineData, 0)

	for i := 0; i < minXAxis; i++ {
		ftsdbHeap = append(ftsdbHeap, opts.LineData{
			Value: ftsdbStats.Data[i].Mem.Mallocs,
		})

		promHeap = append(promHeap, opts.LineData{
			Value: prometheusStats.Data[i].Mem.Mallocs,
		})
	}

	HeapLine.SetXAxis(xAxis).
		AddSeries("ftsdb-heap", ftsdbHeap).
		AddSeries("prometheus-heap", promHeap)

	f, err := os.Create(plotOpts.Filepath)

	noErr(err)

	page.AddCharts(CPULine, MemoryLine, HeapLine)

	page.Render(f)

	currentDir, _ := os.Getwd()
	resultFile := filepath.Join(currentDir, plotOpts.Filepath)
	exec.Command("open", fmt.Sprintf("file://%s", resultFile)).Start()
}
