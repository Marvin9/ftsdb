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

func (mmp *matchedDataPointsRepresentation) GetTimestamp() int64 {
	return mmp.matched.timestamp
}

func (mmp *matchedDataPointsRepresentation) GetValue() float64 {
	return mmp.matched.value
}

func (mmp *matchedDataPointsRepresentation) GetMetric() string {
	return mmp.matchedMetric.metric
}

func (mmp *matchedDataPointsRepresentation) GetSeries() map[string]string {
	return mmp.matchedSeries.series
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

func hashSeries(series map[string]interface{}) string {
	return fmt.Sprint(series)
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
