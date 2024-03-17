package ftsdb

import (
	"fmt"
	"math"

	"github.com/huandu/skiplist"
	"go.uber.org/zap"
)

type Query struct {
	metric     *string
	rangeStart *int64
	rangeEnd   *int64
	series     *map[string]interface{}
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

func (q *Query) Series(series map[string]interface{}) *Query {
	q.series = &series
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
	ftsdbim.logger.Debug("creating metric", zap.String("metric", metric))
	newMetric := NewMetric(metric, ftsdbim.logger.Named("metric-"+metric))

	itr := &ftsdbim.metric
	for *itr != nil {
		if (*itr).metric == metric {
			ftsdbim.logger.Debug("found existing metric")
			return *itr
		}
		itr = &(*itr).next
	}

	*itr = newMetric

	ftsdbim.logger.Debug("created new metric")
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
			ftsdb.logger.Info("--")
			ftsdb.logger.Info("--")
			ftsdb.logger.Info("series", zap.String("series-hash", seriesItr.hash), zap.Any("series", seriesItr.series))

			dataPointsItr := seriesItr.dataPoints.Front()

			for dataPointsItr != nil {
				val := dataPointsItr.Value.(*ftsdbDataPoint)
				ftsdb.logger.Info("data-point", zap.Int64("timestamp", val.timestamp), zap.Float64("value", val.value))
				dataPointsItr = dataPointsItr.Next()
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

	currentMetricsItr = matchedMetrics.matched

	// MATCHED SERIES
	matchedSeries := matchedSeriesPresentation{}

	currentMatchedSeriesPtr := &matchedSeries
	for currentMetricsItr != nil {
		currentSeriesItr := currentMetricsItr.series

		for currentSeriesItr != nil {
			seriesMatched := query.series == nil || hashSeries(*query.series) == hashSeries(currentSeriesItr.series)
			if seriesMatched {
				*currentMatchedSeriesPtr = matchedSeriesPresentation{
					matched:       currentSeriesItr,
					next:          &matchedSeriesPresentation{},
					matchedMetric: currentMetricsItr,
				}
				currentMatchedSeriesPtr = currentMatchedSeriesPtr.next
			}
			currentSeriesItr = currentSeriesItr.next
		}

		currentMetricsItr = currentMetricsItr.next
	}

	// MATCHED DATA POINTS
	matchedDataPoints := matchedDataPointsRepresentation{}

	matchedDataPointsItr := &matchedDataPoints
	matchedSeriesItr := &matchedSeries

	for matchedSeriesItr.matched != nil {
		dataPointsItr := matchedSeriesItr.matched.dataPoints.Front()
		if query.rangeStart != nil {
			dataPointsItr = matchedSeriesItr.matched.dataPoints.Find(*query.rangeStart)
		}

		rangeEnd := math.MaxInt64

		if query.rangeEnd != nil {
			rangeEnd = int(*query.rangeEnd)
		}

		for dataPointsItr != nil {
			if (*dataPointsItr).Value.(*ftsdbDataPoint).timestamp > int64(rangeEnd) {
				break
			}

			*matchedDataPointsItr = matchedDataPointsRepresentation{
				matched:       (*dataPointsItr).Value.(*ftsdbDataPoint),
				next:          &matchedDataPointsRepresentation{},
				matchedMetric: matchedSeriesItr.matchedMetric,
				matchedSeries: matchedSeriesItr.matched,
			}

			matchedDataPointsItr = matchedDataPointsItr.next
			dataPointsItr = dataPointsItr.Next()
		}

		matchedSeriesItr = matchedSeriesItr.next
	}

	return &matchedDataPoints
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
		series: nil,
		logger: logger,
	}
}

func (fm *ftsdbMetric) Append(series map[string]interface{}, timestamp int64, value float64) {
	fm.logger.Debug("appending series", zap.Any("series", series), zap.Int64("timestamp", timestamp), zap.Float64("value", value))

	seriesPtr := fm.createSeries(series)

	if seriesPtr.dataPoints == nil {
		seriesPtr.dataPoints = skiplist.New(skiplist.GreaterThanFunc(func(d1, d2 interface{}) int {
			_d1 := d1.(int64)
			_d2 := d2.(int64)

			if _d1 > _d2 {
				return 1
			} else if _d1 < _d2 {
				return -1
			}

			return 0
		}))
	}

	(*seriesPtr.dataPoints).Set(timestamp, newDataPoint(timestamp, value))
}

func (fm *ftsdbMetric) createSeries(series map[string]interface{}) *ftsdbSeries {
	fm.logger.Debug("creating series", zap.Any("series", series))

	itr := &fm.series

	for *itr != nil {
		if (*itr).hash == hashSeries(series) {
			return *itr
		}
		itr = &(*itr).next
	}

	*itr = newSeries(series)

	return *itr
}

func (fm *ftsdbMetric) findSeries(series map[string]interface{}) *ftsdbSeries {
	fm.logger.Debug("finding series", zap.Any("series", series))
	itr := &fm.series
	for *itr != nil {
		if (*itr).hash == hashSeries(series) {
			fm.logger.Debug("found")
			return *itr
		}
		itr = &(*itr).next
	}

	return nil
}

func hashSeries(series map[string]interface{}) string {
	return fmt.Sprint(series)
}

type ftsdbSeries struct {
	series     map[string]interface{}
	hash       string
	dataPoints *skiplist.SkipList
	next       *ftsdbSeries
}

func newSeries(series map[string]interface{}) *ftsdbSeries {
	return &ftsdbSeries{
		series: series,
		hash:   hashSeries(series),
	}
}

type ftsdbDataPoint struct {
	timestamp int64
	value     float64
	next      *ftsdbDataPoint
}

func newDataPoint(timestamp int64, value float64) *ftsdbDataPoint {
	return &ftsdbDataPoint{
		timestamp: timestamp,
		value:     value,
	}
}
