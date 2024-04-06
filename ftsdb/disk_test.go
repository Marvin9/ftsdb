package ftsdb

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/Marvin9/ftsdb/shared"
	"github.com/stretchr/testify/require"
)

func TestGetChunkMeta(t *testing.T) {
	chunkMeta := ChunkMeta{
		MinTimestamp: 0,
		MaxTimestamp: 100,
		Series: []map[string]string{
			{
				"host": "mac",
			},
			{
				"host": "win",
			},
		},
	}

	metapath := filepath.Join(shared.GetIngestionDir(), "0", "meta.json")

	b, err := json.Marshal(chunkMeta)

	require.NoError(t, err)

	err = os.MkdirAll(filepath.Join(shared.GetIngestionDir(), "0"), 0777)

	require.NoError(t, err)

	require.NoError(t, os.WriteFile(metapath, b, 0666))

	require.NoError(t, err)

	fetchedChunkMeta := GetChunkMeta(0)

	require.Equal(t, chunkMeta, fetchedChunkMeta)

	require.NoError(t, os.RemoveAll(metapath))
}

func TestDeltaEncodeChunks(t *testing.T) {
	enc := DeltaEncodeChunk([]ChunkData{
		{
			Datapoint: Datapoint{
				Timestamp: 100,
				Value:     20,
			},
		},
		{
			Datapoint: Datapoint{
				Timestamp: 200,
				Value:     10,
			},
		},
		{
			Datapoint: Datapoint{
				Timestamp: 201,
				Value:     15,
			},
		},
		{
			Datapoint: Datapoint{
				Timestamp: 202,
				Value:     100,
			},
		},
	})

	require.Equal(t, []ChunkData{
		{
			Datapoint: Datapoint{
				Timestamp: 100,
				Value:     20,
			},
		},
		{
			Datapoint: Datapoint{
				Timestamp: 100,
				Value:     -10,
			},
		},
		{
			Datapoint: Datapoint{
				Timestamp: 1,
				Value:     5,
			},
		},
		{
			Datapoint: Datapoint{
				Timestamp: 1,
				Value:     85,
			},
		},
	}, enc)
}

func TestDeltaDecodeChunks(t *testing.T) {
	orig := DeltaDecodeChunk([]ChunkData{
		{
			Datapoint: Datapoint{
				Timestamp: 100,
				Value:     20,
			},
		},
		{
			Datapoint: Datapoint{
				Timestamp: 100,
				Value:     -10,
			},
		},
		{
			Datapoint: Datapoint{
				Timestamp: 1,
				Value:     5,
			},
		},
		{
			Datapoint: Datapoint{
				Timestamp: 1,
				Value:     85,
			},
		},
	})

	require.Equal(t, []ChunkData{
		{
			Datapoint: Datapoint{
				Timestamp: 100,
				Value:     20,
			},
		},
		{
			Datapoint: Datapoint{
				Timestamp: 200,
				Value:     10,
			},
		},
		{
			Datapoint: Datapoint{
				Timestamp: 201,
				Value:     15,
			},
		},
		{
			Datapoint: Datapoint{
				Timestamp: 202,
				Value:     100,
			},
		},
	}, orig)
}
