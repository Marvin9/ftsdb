package experiments

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/shirou/gopsutil/cpu"
)

type Data struct {
	// time elapsed since start, in milliseconds
	Elapsed int
	CPU     float64
	Mem     runtime.MemStats
}

type stats struct {
	mut  sync.Mutex
	Data []Data
	// time took to run in milliseconds
	RunningTime int
	DiskSize    int
}

func NewStats() *stats {
	return &stats{}
}

// https://www.inanzzz.com/index.php/post/7qfh/monitoring-cpu-memory-and-goroutine-allocation-in-golang
func (s *stats) StartMonitoring(
	// in milliseconds
	monitoringInterval int,
) func() {
	stopMonitoring := make(chan bool)

	start := time.Now()
	go func() {
		s.Data = make([]Data, 0)
		mem := &runtime.MemStats{}

		elapsed := 0
		for {
			select {
			case <-stopMonitoring:
				s.RunningTime = int(time.Since(start).Milliseconds())
				return
			default:
				perc, _ := cpu.Percent(0, true)

				totalPerc := float64(0)

				for _, percCpu := range perc {
					totalPerc += percCpu
				}

				runtime.ReadMemStats(mem)

				s.mut.Lock()
				s.Data = append(s.Data, Data{
					Elapsed: elapsed,
					CPU:     totalPerc,
					Mem:     *mem,
				})
				s.mut.Unlock()

				elapsed += monitoringInterval
				time.Sleep(time.Millisecond * time.Duration(monitoringInterval))
			}

		}
	}()

	stop := func() {
		stopMonitoring <- true
	}

	return stop
}

func (s *stats) Print(till int) {
	d := s.Data

	if till >= 0 {
		d = s.Data[:till]
	}

	for _, data := range d {
		fmt.Println(data.Elapsed, data.CPU, data.Mem.TotalAlloc)
	}
}
