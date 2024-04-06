package ftsdb

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/Marvin9/ftsdb/shared"
)

func GetChunkMeta(chunk int) ChunkMeta {
	metapath := filepath.Join(shared.GetIngestionDir(), strconv.Itoa(chunk), "meta.json")

	chunkMeta := ChunkMeta{}

	file, err := os.Open(metapath)
	shared.NoErr(err)

	defer file.Close()

	// Read the file contents
	data, err := io.ReadAll(file)
	shared.NoErr(err)

	shared.NoErr(json.Unmarshal(data, &chunkMeta))

	return chunkMeta
}

func ReadSeries(chunk int, chunkMeta ChunkMeta, series map[string]string) []ChunkData {
	seriesIndexInChunk := -1

	for idx, existingSeries := range chunkMeta.Series {
		if seriesMatched(series, existingSeries) {
			seriesIndexInChunk = idx
			break
		}
	}

	if seriesIndexInChunk == -1 {
		return []ChunkData{}
	}

	chunkpath := filepath.Join(shared.GetIngestionDir(), strconv.Itoa(chunk), "chunk")

	file, err := os.Open(chunkpath)
	shared.NoErr(err)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanRunes)
	chunkData := make([]ChunkData, 0)

	line := 0
	token := strings.Builder{}
	newChunkData := ChunkData{
		Series:    int64(seriesIndexInChunk),
		Datapoint: Datapoint{},
	}

	for scanner.Scan() {
		curr := scanner.Text()

		if curr == "\n" {
			line++
		}

		if line == seriesIndexInChunk {
			switch curr {
			case "-":
				timestamp, _ := strconv.ParseInt(token.String(), 10, 64)
				newChunkData.Datapoint.Timestamp = timestamp
				token.Reset()
			case ",":
				value, _ := strconv.ParseInt(token.String(), 10, 64)
				newChunkData.Datapoint.Value = value
				token.Reset()
				chunkData = append(chunkData, newChunkData)
			case "\n":
				token.Reset()
			default:
				token.WriteString(curr)
			}
		}

		if line > seriesIndexInChunk {
			break
		}
	}

	shared.NoErr(scanner.Err())

	return chunkData
}

func parseRawString(raw string, lineNumber int) ([]ChunkData, error) {
	chunks := strings.Split(strings.TrimSuffix(raw, ","), ",")
	result := make([]ChunkData, 0, len(chunks))

	for _, chunk := range chunks {
		datapointParts := strings.Split(chunk, "-")
		if len(datapointParts) != 2 {
			return nil, fmt.Errorf("invalid chunk format: %s", chunk)
		}

		timestamp, err := strconv.ParseInt(datapointParts[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse timestamp: %s", err)
		}

		value, err := strconv.ParseInt(datapointParts[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse value: %s", err)
		}

		result = append(result, ChunkData{
			Series: int64(lineNumber),
			Datapoint: Datapoint{
				Timestamp: timestamp,
				Value:     value,
			},
		})
	}

	return result, nil
}

func DeltaEncodeChunk(chunks []ChunkData) []ChunkData {
	compressed := make([]ChunkData, len(chunks))

	for idx, data := range chunks {
		compressed[idx] = data

		if idx != 0 {
			compressed[idx].Datapoint.Timestamp = compressed[idx].Datapoint.Timestamp - chunks[idx-1].Datapoint.Timestamp
		}
	}

	return compressed
}

func DeltaDecodeChunk(chunks []ChunkData) []ChunkData {
	orig := make([]ChunkData, len(chunks))

	prev := 0

	for idx, data := range chunks {
		orig[idx] = data

		prev += int(data.Datapoint.Timestamp)

		orig[idx].Datapoint.Timestamp = int64(prev)
	}

	return orig
}
