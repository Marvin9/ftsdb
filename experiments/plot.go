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
		charts.WithYAxisOpts(opts.YAxis{
			AxisLabel: &opts.AxisLabel{
				Formatter: "{value}%",
				Show:      true,
			},
		}),
	)

	minXAxis := int(math.Min(float64(len(ftsdbStats.Data)), float64(len(prometheusStats.Data))))

	xAxis := []string{}
	ftsdbSeriesCPU := make([]opts.LineData, 0)
	promSeriesCPU := make([]opts.LineData, 0)

	for i := 0; i < minXAxis; i++ {
		xAxis = append(xAxis, fmt.Sprintf("%d ms", ftsdbStats.Data[i].Elapsed))

		ftsdbSeriesCPU = append(ftsdbSeriesCPU, opts.LineData{
			Value: ftsdbStats.Data[i].CPU,
		})

		promSeriesCPU = append(promSeriesCPU, opts.LineData{
			Value: prometheusStats.Data[i].CPU,
		})
	}

	CPULine.SetXAxis(xAxis).
		AddSeries("ftsdb-cpu", ftsdbSeriesCPU).
		AddSeries("prometheus-cpu", promSeriesCPU).
		SetSeriesOptions(charts.WithLineChartOpts(opts.LineChart{
			Smooth: true,
		}))

	// ------------------------

	MemoryLine := charts.NewLine()

	MemoryLine.SetGlobalOptions(
		charts.WithTitleOpts(opts.Title{
			Title:    title,
			Subtitle: subtitle,
		}),
		charts.WithYAxisOpts(opts.YAxis{
			AxisLabel: &opts.AxisLabel{
				Formatter: "{value}MB",
				Show:      true,
			},
		}),
	)

	ftsdbSeriesMemory := make([]opts.LineData, 0)
	promSeriesMemory := make([]opts.LineData, 0)

	for i := 0; i < minXAxis; i++ {
		ftsdbSeriesMemory = append(ftsdbSeriesMemory, opts.LineData{
			Value: toMegaBytes(int(ftsdbStats.Data[i].Mem.Alloc)),
		})

		promSeriesMemory = append(promSeriesMemory, opts.LineData{
			Value: toMegaBytes(int(prometheusStats.Data[i].Mem.Alloc)),
		})
	}

	MemoryLine.SetXAxis(xAxis).
		AddSeries("ftsdb-memory", ftsdbSeriesMemory).
		AddSeries("prometheus-memory", promSeriesMemory).
		SetSeriesOptions(charts.WithLineChartOpts(opts.LineChart{
			Smooth: true,
		}))

	// ----------------------------

	HeapLine := charts.NewLine()

	HeapLine.SetGlobalOptions(
		charts.WithTitleOpts(opts.Title{
			Title:    title,
			Subtitle: subtitle,
		}),
		charts.WithYAxisOpts(opts.YAxis{
			AxisLabel: &opts.AxisLabel{
				Formatter: "{value}MB",
				Show:      true,
			},
		}),
	)

	ftsdbHeap := make([]opts.LineData, 0)
	promHeap := make([]opts.LineData, 0)

	for i := 0; i < minXAxis; i++ {
		ftsdbHeap = append(ftsdbHeap, opts.LineData{
			Value: toMegaBytes(int(ftsdbStats.Data[i].Mem.Mallocs)),
		})

		promHeap = append(promHeap, opts.LineData{
			Value: toMegaBytes(int(prometheusStats.Data[i].Mem.Mallocs)),
		})
	}

	HeapLine.SetXAxis(xAxis).
		AddSeries("ftsdb-heap", ftsdbHeap).
		AddSeries("prometheus-heap", promHeap).
		SetSeriesOptions(charts.WithLineChartOpts(opts.LineChart{
			Smooth: true,
		}))

	DiskSize := charts.NewBar()

	DiskSize.SetGlobalOptions(
		charts.WithTitleOpts(opts.Title{
			Title:    title,
			Subtitle: subtitle,
		}),
		charts.WithYAxisOpts(opts.YAxis{
			AxisLabel: &opts.AxisLabel{
				Formatter: "{value}MB",
				Show:      true,
			},
		}),
	)

	DiskSize.SetXAxis([]string{"ftsdb-disk", "prometheus-disk"}).
		AddSeries("disk-size", []opts.BarData{
			{
				Value: toMegaBytes(ftsdbStats.DiskSize),
			},
			{
				Value: toMegaBytes(prometheusStats.DiskSize),
			},
		})

	// running time
	RunningTime := charts.NewBar()

	RunningTime.SetGlobalOptions(
		charts.WithTitleOpts(opts.Title{
			Title:    title,
			Subtitle: subtitle,
		}),
		charts.WithYAxisOpts(opts.YAxis{
			AxisLabel: &opts.AxisLabel{
				Formatter: "{value}s",
				Show:      true,
			},
		}),
	)

	RunningTime.SetXAxis([]string{"ftsdb-latency", "prometheus-latency"}).
		AddSeries("latency", []opts.BarData{
			{
				Value: toSeconds(ftsdbStats.RunningTime),
			},
			{
				Value: toSeconds(prometheusStats.RunningTime),
			},
		})

	f, err := os.Create(plotOpts.Filepath)

	noErr(err)

	charts := []components.Charter{
		CPULine,
		MemoryLine,
		RunningTime,
		HeapLine,
	}

	if prometheusStats.DiskSize > 0 && ftsdbStats.DiskSize > 0 {
		charts = append(charts, DiskSize)
	}

	page.AddCharts(charts...)

	page.Render(f)

	currentDir, _ := os.Getwd()
	resultFile := filepath.Join(currentDir, plotOpts.Filepath)
	exec.Command("open", fmt.Sprintf("file://%s", resultFile)).Start()
}
