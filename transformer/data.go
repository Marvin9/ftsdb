package transformer

import (
	"encoding/json"
	"os"
	"time"

	"go.uber.org/zap"
)

const LAYOUT = "2006-01-02T15:04:05.00000"

type CPUData struct {
	Timestamp int64   `json:"timestamp"`
	CPUUsage  float64 `json:"cpu_usage"`
}

type dataTransformer struct {
	logger *zap.Logger
}

func NewDataTransformer(logger *zap.Logger) *dataTransformer {
	return &dataTransformer{
		logger: logger,
	}
}

func (dt *dataTransformer) GenCPUData(path string, _len int) []CPUData {
	// dt.logger.Info("generating cpu data")

	file, err := os.Open(path)

	if err != nil {
		panic(err)
	}

	defer file.Close()

	type cpuDataJson struct {
		Timestamp string
		CpuUsage  float64 `json:"cpu_usage"`
	}

	var cpuData []cpuDataJson

	err = json.NewDecoder(file).Decode(&cpuData)

	if err != nil {
		panic(err)
	}

	transformCpuData := []CPUData{}

	ln := len(transformCpuData)

	if _len > 0 {
		ln = _len
	}

	for _, data := range cpuData[:ln] {
		unixTs := dt.parseTimestamp(data.Timestamp)

		if unixTs == -1 {
			continue
		}

		transformCpuData = append(transformCpuData, CPUData{
			Timestamp: unixTs,
			CPUUsage:  data.CpuUsage,
		})
	}

	// dt.logger.Info("cpu data ready")
	return transformCpuData
}

// timestamp - 2024-03-02T00:01:51.57856
// return timestamp in unix milliseconds
func (dt *dataTransformer) parseTimestamp(timestamp string) int64 {
	t, err := time.Parse(LAYOUT, timestamp)

	if err != nil {
		return -1
	}

	return t.UnixMilli()
}
