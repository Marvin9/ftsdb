#!/bin/bash

FILENAME=metrics-ftsdb.json

echo "{" > $FILENAME
echo "  \"data\": [" >> $FILENAME

get_timestamp_in_ms() {
    date +%s%3
}

# Function to collect CPU and Memory usage
collect_metrics() {
    cpu_usage=$(top -l 1 | grep "CPU usage" | awk '{print $3}' | cut -d'%' -f1)
    memory_usage=$(top -l 1 | grep PhysMem | awk '{print $8}' | sed 's/M//')
    timestamp=$(get_timestamp_in_ms)
    echo "{\"timestamp\": $timestamp, \"cpu\": $cpu_usage, \"memory\": $memory_usage}," >> $FILENAME
}

# Start capturing metrics in background
capture_metrics() {
    while true; do
        collect_metrics
        sleep 0.5
    done
}

# Run your Go command here
# Example:
go run . &

# Capture the PID of the Go command
go_pid=$!

echo "Go $go_pid"

# Start capturing metrics in background
capture_metrics &

metrics_pid=$!

echo "Metric $metrics_pid"

# Wait for the Go command to finish
wait $go_pid

# Stop capturing metrics
kill $metrics_pid

# Final metrics capture before exiting
collect_metrics

sed -i '' -e '$s/,$//' $FILENAME
echo "  ]" >> $FILENAME
echo "}" >> $FILENAME

echo "Monitoring completed."
