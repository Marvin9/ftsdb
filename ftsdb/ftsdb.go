package ftsdb

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"

	"go.uber.org/zap"
)

type Query struct {
	metric     *string
	rangeStart *int64
	rangeEnd   *int64
	series     map[string]string
}

func (q *Query) Metric(metric string) *Query {
	q.metric = &metric
	return q
}

func (q *Query) RangeStart(rangeStart int64) *Query {
	q.rangeStart = &rangeStart
	return q
}

func (q *Query) RangeEnd(rangeEnd int64) *Query {
	q.rangeEnd = &rangeEnd
	return q
}

func (q *Query) Series(series map[string]string) *Query {
	q.series = series
	return q
}

type Iterator interface {
	At() interface{}
	Next() Iterator
}

type DBInterface interface {
	Find(query Query) *SeriesIterator
	CreateMetric(metric string) *ftsdbMetric
	DisplayMetrics()
	Commit() error
}

type ftsdbInMemory struct {
	metric *ftsdbMetric
	logger *zap.Logger
}

func newFtsdbInMemory(logger *zap.Logger) *ftsdbInMemory {
	return &ftsdbInMemory{
		logger: logger,
	}
}

func (ftsdbim *ftsdbInMemory) createMetric(metric string) *ftsdbMetric {
	// ftsdbim.logger.Debug("creating metric", zap.String("metric", metric))
	newMetric := NewMetric(metric, ftsdbim.logger.Named("metric-"+metric))

	itr := &ftsdbim.metric
	for *itr != nil {
		if (*itr).metric == metric {
			// ftsdbim.logger.Debug("found existing metric")
			return *itr
		}
		itr = &(*itr).next
	}

	*itr = newMetric

	// ftsdbim.logger.Debug("created new metric")
	return *itr
}

type ftsdb struct {
	inMemory *ftsdbInMemory
	logger   *zap.Logger
	dir      string
}

func NewFTSDB(logger *zap.Logger, dir string) DBInterface {
	return &ftsdb{
		logger:   logger,
		inMemory: newFtsdbInMemory(logger.Named("inMemory")),
		dir:      dir,
	}
}

func (ftsdb *ftsdb) DisplayMetrics() {
	ftsdb.logger.Info("display-metrics")
	itr := ftsdb.inMemory.metric

	for itr != nil {
		ftsdb.logger.Info("--")
		ftsdb.logger.Info("metric", zap.String("name", itr.metric), zap.Bool("has-next", itr.next != nil))

		seriesItr := itr.series

		for seriesItr != nil {
			dataPointsItr := 0

			ftsdb.logger.Info("series", zap.Any("series", seriesItr.series))
			for dataPointsItr < seriesItr.dataPoints.size {
				val := seriesItr.dataPoints.At(dataPointsItr).(*ftsdbDataPoint)
				ftsdb.logger.Info("data-point", zap.Int64("timestamp", val.timestamp), zap.Float64("value", val.value))
				dataPointsItr++
			}
			seriesItr = seriesItr.next
		}

		itr = itr.next
	}
}

func (ftsdb *ftsdb) CreateMetric(metric string) *ftsdbMetric {
	return ftsdb.inMemory.createMetric(metric)
}

type matchedMetricsPresentation struct {
	matched *ftsdbMetric
	next    *matchedMetricsPresentation
}

type DataPointsIterator interface {
	Is() bool
	Next() DataPointsIterator
	GetMetric() string
	GetSeries() map[string]string
	GetTimestamp() int64
	GetValue() float64
}

type Series struct {
	SeriesValue map[string]string
}

type Datapoint struct {
	Timestamp int64
	Value     int64
}

type DatapointsIterator struct {
	Next         func() *DatapointsIterator
	GetDatapoint func() Datapoint
}

type SeriesIterator struct {
	Next               func() *SeriesIterator
	GetSeries          func() Series
	DatapointsIterator *DatapointsIterator
}

type ChunkData struct {
	Series    int64
	Datapoint Datapoint
}

type ChunkMeta struct {
	MinTimestamp int64
	MaxTimestamp int64
	Series       []map[string]string
}

type Chunk struct {
	Meta ChunkMeta
	Data []ChunkData
}

func (c *Chunk) Encode() []byte {
	encodedData := strings.Builder{}
	for _, data := range c.Data {
		encodedData.WriteString(fmt.Sprintf("%d-%d-%d,", data.Series, data.Datapoint.Timestamp, data.Datapoint.Value))
	}
	return []byte(encodedData.String())
}

func (c *Chunk) Merge(data []ChunkData) {
	m := len(c.Data)
	n := len(data)
	mergedData := make([]ChunkData, m+n)
	index := m + n - 1
	i := m - 1
	j := n - 1
	for ; i >= 0 && j >= 0; index-- {
		if c.Data[i].Datapoint.Timestamp >= data[j].Datapoint.Timestamp {
			mergedData[index] = c.Data[i]
			i--
		} else {
			mergedData[index] = data[j]
			j--
		}
	}
	for j >= 0 {
		mergedData[index] = data[j]
		index--
		j--
	}

	c.Data = mergedData
}

func NewChunk() *Chunk {
	return &Chunk{
		Meta: ChunkMeta{
			MinTimestamp: math.MaxInt64,
			MaxTimestamp: math.MinInt64,
			Series:       make([]map[string]string, 0),
		},
		Data: make([]ChunkData, 0),
	}
}

func (ftsdb *ftsdb) Commit() error {
	// ftsdb.logger.Debug("commit request")
	chunk := NewChunk()

	itr := ftsdb.inMemory.metric.series

	seriedIdxInMeta := -1
	for itr != nil {
		chunk.Meta.Series = append(chunk.Meta.Series, itr.series)
		seriedIdxInMeta++

		chunkData := make([]ChunkData, len(itr.dataPoints.arr))
		for idx, val := range itr.dataPoints.arr {
			dp := val.(*ftsdbDataPoint)
			chunkData[idx] = ChunkData{
				Series: int64(seriedIdxInMeta),
				Datapoint: Datapoint{
					Timestamp: dp.timestamp,
					Value:     int64(dp.value),
				},
			}

			if dp.timestamp < chunk.Meta.MinTimestamp {
				chunk.Meta.MinTimestamp = dp.timestamp
			} else if dp.timestamp > chunk.Meta.MaxTimestamp {
				chunk.Meta.MaxTimestamp = dp.timestamp
			}
		}

		chunk.Merge(chunkData)

		itr = itr.next
	}

	// ftsdb.logger.Debug("chunk generated")

	dir := filepath.Join(ftsdb.dir, fmt.Sprintf("%d", chunk.Meta.MinTimestamp))

	if err := os.MkdirAll(dir, 0777); err != nil {
		return err
	}

	metafilename := filepath.Join(dir, "meta.json")

	metabytes, err := json.Marshal(chunk.Meta)

	if err != nil {
		return err
	}

	// ftsdb.logger.Debug("writing meta", zap.String("filename", metafilename))

	if err = os.WriteFile(metafilename, metabytes, 0666); err != nil {
		return err
	}

	chunkfilename := filepath.Join(dir, "chunk")

	chunkbytes := chunk.Encode()

	// ftsdb.logger.Debug("writing chunk", zap.String("chunk", chunkfilename))

	if err = os.WriteFile(chunkfilename, chunkbytes, 0666); err != nil {
		return err
	}

	ftsdb.inMemory = newFtsdbInMemory(ftsdb.logger)

	return nil
}

func (ftsdb *ftsdb) Find(query Query) *SeriesIterator {
	// MATCHED METRICS
	matchedMetrics := matchedMetricsPresentation{}

	currentMatchedMetricsItr := &matchedMetrics
	currentMetricsItr := &ftsdbMetric{}
	currentMetricsItr = ftsdb.inMemory.metric

	for currentMetricsItr != nil {
		metricMatched := query.metric == nil || currentMetricsItr.metric == *query.metric
		if metricMatched {
			*currentMatchedMetricsItr = matchedMetricsPresentation{
				matched: currentMetricsItr,
				next:    &matchedMetricsPresentation{},
			}
			currentMatchedMetricsItr = currentMatchedMetricsItr.next
		}
		currentMetricsItr = currentMetricsItr.next
	}

	// MATCHED SERIES
	currentMatchedMetricsItr = &matchedMetrics

	var currentSeriesItr *ftsdbSeries = nil

	ss := &SeriesIterator{}

	Next := func() *SeriesIterator {
		if currentSeriesItr == nil {
			currentSeriesItr = currentMatchedMetricsItr.matched.series
		} else {
			currentSeriesItr = currentSeriesItr.next
		}

		for currentMatchedMetricsItr != nil {
			for currentSeriesItr != nil {
				matched := query.series == nil || seriesMatched(currentSeriesItr.series, query.series)

				if matched {
					dataPointsItr := 0
					initialised := false

					var rangeEnd int64

					dd := &DatapointsIterator{}

					Next := func() *DatapointsIterator {
						if !initialised {
							if query.rangeStart != nil {
								dataPointsItr = currentSeriesItr.LowerBoundTimestamp(*query.rangeStart)
							}

							rangeEnd = math.MaxInt64

							if query.rangeEnd != nil {
								rangeEnd = *query.rangeEnd
							}
							initialised = true
						} else {
							dataPointsItr++
						}

						if dataPointsItr < currentSeriesItr.dataPoints.size {
							dataPoint := currentSeriesItr.dataPoints.At(dataPointsItr).(*ftsdbDataPoint)

							if dataPoint.timestamp > rangeEnd {
								dataPointsItr++
								return nil
							}

							return dd
						}

						return nil
					}

					dd.Next = Next
					dd.GetDatapoint = func() Datapoint {
						val := currentSeriesItr.dataPoints.At(dataPointsItr).(*ftsdbDataPoint)
						return Datapoint{
							Timestamp: val.timestamp,
							Value:     int64(val.value),
						}
					}
					ss.DatapointsIterator = dd
					return ss
				}

				currentSeriesItr = currentSeriesItr.next
			}

			currentMatchedMetricsItr = currentMatchedMetricsItr.next
			if currentMatchedMetricsItr != nil && currentMatchedMetricsItr.matched != nil {
				currentSeriesItr = currentMatchedMetricsItr.matched.series
			}
		}

		return nil
	}

	ss.Next = Next
	ss.GetSeries = func() Series {
		return Series{
			SeriesValue: currentSeriesItr.series,
		}
	}

	return ss
}

type MetricInterface interface {
	Append(series map[string]interface{}, timestamp int64, value float64)
	Find(metrc string)
}

type ftsdbMetric struct {
	metric string
	series *ftsdbSeries
	next   *ftsdbMetric
	logger *zap.Logger
}

func NewMetric(metric string, logger *zap.Logger) *ftsdbMetric {
	return &ftsdbMetric{
		metric: metric,
		logger: logger,
		series: nil,
	}
}

func (fm *ftsdbMetric) Append(series map[string]string, timestamp int64, value float64) {
	// fm.logger.Debug("appending series", zap.Any("series", series), zap.Int64("timestamp", timestamp), zap.Float64("value", value))

	seriesItr := fm.createSeries(series)

	(*seriesItr).dataPoints.Insert(newDataPoint(timestamp, value))
}

func (fm *ftsdbMetric) createSeries(series map[string]string) *ftsdbSeries {
	seriesItr := &fm.series

	for *seriesItr != nil {
		matched := true
		totKeys := len((*seriesItr).series)
		if totKeys != len(series) {
			matched = false
			seriesItr = &(*seriesItr).next
			continue
		}
		for k, v := range series {
			vv, found := (*seriesItr).series[k]

			if !found || vv != v {
				matched = false
				break
			}
		}

		if matched {
			return (*seriesItr)
		}

		seriesItr = &(*seriesItr).next
	}

	*seriesItr = newSeries(series)

	return *seriesItr
}

type ftsdbSeries struct {
	series     map[string]string
	dataPoints *FastArray
	next       *ftsdbSeries
}

func newSeries(series map[string]string) *ftsdbSeries {
	return &ftsdbSeries{
		series:     series,
		dataPoints: NewFastArray(),
		next:       nil,
	}
}

func seriesMatched(series1 map[string]string, series2 map[string]string) bool {
	if len(series1) != len(series2) {
		return false
	}

	for k, v := range series1 {
		vv, found := series2[k]

		if !found || vv != v {
			return false
		}
	}
	return true
}

func (s *ftsdbSeries) LowerBoundTimestamp(timestamp int64) int {
	var mid int

	low := 0
	high := s.dataPoints.size

	for low < high {
		mid = low + (high-low)/2

		if timestamp <= s.dataPoints.At(mid).(*ftsdbDataPoint).timestamp {
			high = mid
		} else {
			low = mid + 1
		}
	}

	if low < s.dataPoints.size && s.dataPoints.At(low).(*ftsdbDataPoint).timestamp < timestamp {
		low++
	}

	return low
}

type ftsdbDataPoint struct {
	timestamp int64
	value     float64
}

func newDataPoint(timestamp int64, value float64) *ftsdbDataPoint {
	return &ftsdbDataPoint{
		timestamp: timestamp,
		value:     value,
	}
}
