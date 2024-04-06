package ftsdb

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/Marvin9/ftsdb/shared"
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
	Close()
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

	totalDistinctSeriesInChunk := len(c.Meta.Series)
	lines := make([]strings.Builder, totalDistinctSeriesInChunk)

	for _, data := range c.Data {
		lines[int(data.Series)].WriteString(fmt.Sprintf("%d-%d,", data.Datapoint.Timestamp, data.Datapoint.Value))
	}

	for idx, line := range lines {
		encodedData.WriteString(line.String())
		if idx < len(lines)-1 {
			encodedData.WriteString("\n")
		}
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
	if ftsdb.inMemory.metric.size < 1000 {
		return nil
	}

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
		if !os.IsExist(err) {
			return err
		}
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
	currentDirectory := shared.GetIngestionDir()

	files, _ := os.ReadDir(currentDirectory)

	minTimestamps := []int{}
	for _, file := range files {
		if file.IsDir() {
			minTimestamp, _ := strconv.Atoi(file.Name())
			minTimestamps = append(minTimestamps, minTimestamp)
		}
	}

	sort.Ints(minTimestamps)

	lowerBound := 0
	if query.rangeStart != nil {
		for idx, value := range minTimestamps {
			if value > int(*query.rangeStart) {
				lowerBound = idx - 1

				if lowerBound == -1 {
					lowerBound = 0
				}
				break
			}
		}
	}

	minTimestamps = minTimestamps[lowerBound:]

	upperBound := len(minTimestamps)
	if query.rangeEnd != nil {
		for idx, value := range minTimestamps {
			if value > int(*query.rangeEnd) {
				upperBound = idx
				break
			}
		}
	}

	minTimestamps = minTimestamps[:upperBound]

	metaCache := map[int]ChunkMeta{}

	for _, minTimestamp := range minTimestamps {
		metaCache[minTimestamp] = GetChunkMeta(minTimestamp)
	}

	seriesToIterate := make([]map[string]string, 0)

	getSeries := func(series map[string]string) int {
		for idx, existingSeries := range seriesToIterate {
			if seriesMatched(existingSeries, series) {
				return idx
			}
		}
		return -1
	}

	for _, minTimestamp := range minTimestamps {
		meta := metaCache[minTimestamp]

		for _, series := range meta.Series {
			if getSeries(series) == -1 {
				// only required series
				if query.series == nil || seriesMatched(query.series, series) {
					seriesToIterate = append(seriesToIterate, series)
				}
			}
		}
	}

	ss := &SeriesIterator{}

	seriesIterator := -1
	Next := func() *SeriesIterator {
		seriesIterator++

		if seriesIterator >= len(seriesToIterate) {
			return nil
		}

		var datapoints []ChunkData

		dd := &DatapointsIterator{}

		dataPointsIterator := -1
		chunkIterator := 0
		Next := func() *DatapointsIterator {
			dataPointsIterator++
			if dataPointsIterator == 0 {
				chunkIndex := minTimestamps[chunkIterator]
				datapoints = ReadSeries(chunkIndex, metaCache[chunkIndex], seriesToIterate[seriesIterator])
			}

			if dataPointsIterator >= len(datapoints) {
				chunkIterator++
				dataPointsIterator = 0
			}

			if chunkIterator >= len(minTimestamps) {
				return nil
			}

			if query.rangeStart != nil && dd.GetDatapoint().Timestamp < *query.rangeStart {
				for dataPointsIterator < len(datapoints) && datapoints[dataPointsIterator].Datapoint.Timestamp < *query.rangeStart {
					dataPointsIterator++
				}
			}

			if query.rangeEnd != nil && dd.GetDatapoint().Timestamp > *query.rangeEnd {
				return nil
			}

			return dd
		}
		dd.Next = Next
		dd.GetDatapoint = func() Datapoint {
			return datapoints[dataPointsIterator].Datapoint
		}
		ss.DatapointsIterator = dd

		return ss
	}

	ss.Next = Next
	ss.GetSeries = func() Series {
		return Series{
			SeriesValue: seriesToIterate[seriesIterator],
		}
	}
	return ss
}

func (ftsdb *ftsdb) Close() {
	ftsdb.logger = nil
	ftsdb.inMemory = nil
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
	size   int64
}

func NewMetric(metric string, logger *zap.Logger) *ftsdbMetric {
	return &ftsdbMetric{
		metric: metric,
		logger: logger,
		series: nil,
		size:   0,
	}
}

func (fm *ftsdbMetric) Append(series map[string]string, timestamp int64, value float64) {
	// fm.logger.Debug("appending series", zap.Any("series", series), zap.Int64("timestamp", timestamp), zap.Float64("value", value))

	seriesItr := fm.createSeries(series)

	(*seriesItr).dataPoints.Insert(newDataPoint(timestamp, value))

	fm.size++
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
