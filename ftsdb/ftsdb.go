package ftsdb

import (
	"fmt"
	"math"

	"go.uber.org/zap"
)

type Query struct {
	metric     *string
	rangeStart *int64
	rangeEnd   *int64
	series     *string
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

func (q *Query) Series(seriesHash string) *Query {
	q.series = &seriesHash
	return q
}

type Iterator interface {
	At() interface{}
	Next() Iterator
}

type DBInterface interface {
	Find(query Query) DataPointsIterator
	CreateMetric(metric string) *ftsdbMetric
	DisplayMetrics()
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
}

func NewFTSDB(logger *zap.Logger) DBInterface {
	return &ftsdb{
		logger:   logger,
		inMemory: newFtsdbInMemory(logger.Named("inMemory")),
	}
}

func (ftsdb *ftsdb) DisplayMetrics() {
	ftsdb.logger.Info("display-metrics")
	itr := ftsdb.inMemory.metric

	for itr != nil {
		ftsdb.logger.Info("--")
		ftsdb.logger.Info("metric", zap.String("name", itr.metric), zap.Bool("has-next", itr.next != nil))

		dataPointsItr := 0

		for dataPointsItr < itr.dataPoints.size {
			val := itr.dataPoints.At(dataPointsItr).(*ftsdbDataPoint)
			ftsdb.logger.Info("data-point", zap.Int64("timestamp", val.timestamp), zap.Float64("value", val.value), zap.String("series", fmt.Sprint(val.series.hash)))
			dataPointsItr++
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
	GetSeries() string
	GetTimestamp() int64
	GetValue() float64
}

func (mmp *matchedDataPointsRepresentation) GetTimestamp() int64 {
	return mmp.matched.timestamp
}

func (mmp *matchedDataPointsRepresentation) GetValue() float64 {
	return mmp.matched.value
}

func (mmp *matchedDataPointsRepresentation) GetMetric() string {
	return mmp.matchedMetric.metric
}

func (mmp *matchedDataPointsRepresentation) GetSeries() string {
	return mmp.matchedSeries.hash
}

func (mmp *matchedDataPointsRepresentation) Next() DataPointsIterator {
	return newMatchedMetricsPresentationWithIterator(mmp.next)
}

func (mmp *matchedDataPointsRepresentation) Is() bool {
	return mmp != nil && mmp.matched != nil
}

func newMatchedMetricsPresentationWithIterator(mmp *matchedDataPointsRepresentation) DataPointsIterator {
	return mmp
}

type matchedDataPointsRepresentation struct {
	matched       *ftsdbDataPoint
	next          *matchedDataPointsRepresentation
	matchedMetric *ftsdbMetric
	matchedSeries *ftsdbSeries
}

type matchedSeriesPresentation struct {
	matched       *ftsdbSeries
	next          *matchedSeriesPresentation
	matchedMetric *ftsdbMetric
}

func (ftsdb *ftsdb) Find(query Query) DataPointsIterator {
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

	// MATCHED DATA POINTS
	matchedDataPoints := matchedDataPointsRepresentation{}

	matchedDataPointsItr := &matchedDataPoints

	rangeStart := math.MinInt64
	if query.rangeStart != nil {
		rangeStart = int(*query.rangeStart)
	}

	rangeEnd := math.MaxInt64
	if query.rangeEnd != nil {
		rangeEnd = int(*query.rangeEnd)
	}

	for matchedMetrics.matched != nil {
		dataPointsItr := 0

		for dataPointsItr < matchedMetrics.matched.dataPoints.size {

			dataPoint := matchedMetrics.matched.dataPoints.At(dataPointsItr).(*ftsdbDataPoint)

			if dataPoint.timestamp > int64(rangeEnd) || dataPoint.timestamp < int64(rangeStart) {
				dataPointsItr++
				continue
			}

			matched := query.series == nil || dataPoint.series.hash == *query.series
			if matched {
				*matchedDataPointsItr = matchedDataPointsRepresentation{
					matched:       dataPoint,
					next:          &matchedDataPointsRepresentation{},
					matchedMetric: matchedMetrics.matched,
					matchedSeries: dataPoint.series,
				}

				matchedDataPointsItr = matchedDataPointsItr.next
			}
			dataPointsItr++
		}

		matchedMetrics = *matchedMetrics.next
	}

	return &matchedDataPoints
}

type MetricInterface interface {
	Append(series map[string]interface{}, timestamp int64, value float64)
	Find(metrc string)
}

type ftsdbMetric struct {
	metric     string
	dataPoints *FastArray
	next       *ftsdbMetric
	logger     *zap.Logger
}

func NewMetric(metric string, logger *zap.Logger) *ftsdbMetric {
	return &ftsdbMetric{
		metric:     metric,
		dataPoints: NewFastArray(),
		logger:     logger,
	}
}

func (fm *ftsdbMetric) Append(series map[string]interface{}, seriesHash string, timestamp int64, value float64) {
	// fm.logger.Debug("appending series", zap.Any("series", series), zap.Int64("timestamp", timestamp), zap.Float64("value", value))

	if fm.dataPoints == nil {
		fm.dataPoints = NewFastArray()
	}

	fm.dataPoints.Insert(newDataPoint(timestamp, value, newSeries(series, seriesHash)))
}

func hashSeries(series map[string]interface{}) string {
	return fmt.Sprint(series)
}

type ftsdbSeries struct {
	series map[string]interface{}
	hash   string
}

func newSeries(series map[string]interface{}, hash string) *ftsdbSeries {
	return &ftsdbSeries{
		series: series,
		hash:   hash,
	}
}

func (s *ftsdbMetric) LowerBoundTimestamp(timestamp int64) int {
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
	series    *ftsdbSeries
}

func newDataPoint(timestamp int64, value float64, series *ftsdbSeries) *ftsdbDataPoint {
	return &ftsdbDataPoint{
		timestamp: timestamp,
		value:     value,
		series:    series,
	}
}
