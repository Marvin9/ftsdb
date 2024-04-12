# ftsdb

Fast time-series database.

## Motivation

This was the final project in subject Algorithm Engineering (CSCI 6105) in winter 2024 by professor [Dr. Chris Whidden](https://www.dal.ca/faculty/computerscience/faculty-staff/chris-whidden.html)

## Goal

Faster time-series database implementation than Prometheus's [tsdb](https://github.com/prometheus/prometheus/tree/main/tsdb).

## Implementation

Partially follows the series indexing techniques mentioned in SlimDB [[1]](https://www.vldb.org/pvldb/vol10/p2037-ren.pdf).

Partially follows disk compression techniques mentioned in Gorilla [[2]](https://www.vldb.org/pvldb/vol8/p1816-teller.pdf).

[Proposal documentation.](./docs/Mayursinh_Sarvaiya_B00918007_Final_Proposal.pdf)

## Experiments

1. Generate data.

Run the PostgreSQL and execute below scripts

```sh
mkdir data

psql -U postgres -h 0.0.0.0 -P format=unaligned -P tuples_only=true -c "WITH time_series AS ( 

  SELECT generate_series( 

           CURRENT_TIMESTAMP::timestamp, 

           CURRENT_TIMESTAMP::timestamp + interval '1 day', 

           interval '50 milliseconds' 

         ) AS timestamp 

) 

SELECT 

  json_agg( 

    json_build_object( 

      'timestamp', timestamp, 

      'cpu_usage', round(random() * 100)::numeric(5,2) 

    ) 

  ) 

FROM time_series;" | jq > data/cpu_usage.json

psql -U postgres -h 0.0.0.0 -P format=unaligned -P tuples_only=true -c "WITH time_series AS ( 

  SELECT generate_series( 

           CURRENT_TIMESTAMP::timestamp, 

           CURRENT_TIMESTAMP::timestamp + interval '1 day', 

           interval '50 milliseconds' 

         ) AS timestamp 

) 

SELECT 

  json_agg( 

    json_build_object( 

      'timestamp', timestamp, 

      'ram_usage', round(random() * 1000)::numeric(5,2) 

    ) 

  ) 

FROM time_series;" | jq > data/ram_usage.json
```

2. Run experiment - this will generate graphs of CPU, Memory, Heap and Disk usage by ftsdb and prometheus tsdb while executing same queries.

```sh
go run .
```

## Benchmarks

```sh
$ go test -bench=. ./experiments
goos: darwin
goarch: arm64
pkg: github.com/Marvin9/ftsdb/experiments
Benchmark_BasicPrometheusTSDB/core-8                                           1        78359559084 ns/op
Benchmark_BasicFTSDB/core-8                                             1000000000               0.6350 ns/op
Benchmark_RangePrometheusTSDB/core-8                                           1        73092514708 ns/op
Benchmark_RangeFTSDB/core-8                                             1000000000               0.6151 ns/op
Benchmark_RangesPrometheusTSDB/core-8                                          1        15577411458 ns/op
Benchmark_RangesFTSDB/core-8                                            1000000000               0.6169 ns/op
Benchmark_HeavyAppendPrometheusTSDB/core-8                                     1        7467235958 ns/op
Benchmark_HeavyAppendFTSDB/core-8                                              1        1695781417 ns/op
Benchmark_HeavyAppendWriteDiskPrometheusTSDB/core-8                     1000000000               0.3399 ns/op
Benchmark_HeavyAppendWriteDiskFTSDB/core-8                                     1        1019162334 ns/op
BenchmarkRealCPUUsageDataPrometheusTSDB/main-8                          1000000000               0.4565 ns/op
BenchmarkRealCPUUsageDataFTSDB/core-8                                   1000000000               0.04666 ns/op
BenchmarkRealCPUUsageDataConsequentAppendWritePrometheusTSDB/main-8                    1        4845681375 ns/op
BenchmarkRealCPUUsageDataConsequentAppendWriteFTSDB/core-8                             1        2216013917 ns/op
BenchmarkRealCPUUsageRangeDataPrometheusTSDB/main-8                             1000000000               0.4203 ns/op
BenchmarkRealCPUUsageRangeDataFTSDB/core-8                                      1000000000               0.05934 ns/op
BenchmarkAppendMillionPointsPrometheusTSDB/main-8                               1000000000               0.2887 ns/op
BenchmarkAppendMillionPointsFTSDB/core-8                                        1000000000               0.3083 ns/op
BenchmarkAppendHundredPointsWithLabelsPrometheusTSDB/main-8                            1        52461926083 ns/op
BenchmarkAppendHundredPointsWithLabelsFTSDB/core-8                                     1        49967540542 ns/op
PASS
ok      github.com/Marvin9/ftsdb/experiments    445.670s
```

## References

[1] K. Ren, Q. Zheng, J. Arulraj, and G. Gibson, “SlimDB: A Space-efficient Key-value Storage Engine for Semi-sorted Data,” Proceedings of the VLDB Endowment, vol. 10, pp. 2037–2048, Sept. 2017. 

[2] Tuomas Pelkonen, Scott Franklin, Justin Teller, Paul Cavallaro, Qi Huang, Justin Meza, and Kaushik Veeraraghavan. 2015. Gorilla: A Fast, Scalable, in-Memory Time Series Database. Proc. VLDB Endow. 8, 12 (Aug. 2015), 1816–1827. https://doi.org/10.14778/2824032.2824078.