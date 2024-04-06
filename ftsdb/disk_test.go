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
			},
		},
		{
			Datapoint: Datapoint{
				Timestamp: 200,
			},
		},
		{
			Datapoint: Datapoint{
				Timestamp: 201,
			},
		},
		{
			Datapoint: Datapoint{
				Timestamp: 202,
			},
		},
	})

	require.Equal(t, []ChunkData{
		{
			Datapoint: Datapoint{
				Timestamp: 100,
			},
		},
		{
			Datapoint: Datapoint{
				Timestamp: 100,
			},
		},
		{
			Datapoint: Datapoint{
				Timestamp: 1,
			},
		},
		{
			Datapoint: Datapoint{
				Timestamp: 1,
			},
		},
	}, enc)
}

func TestDeltaDecodeChunks(t *testing.T) {
	orig := DeltaDecodeChunk(
		[]ChunkData{
			{
				Datapoint: Datapoint{
					Timestamp: 100,
				},
			},
			{
				Datapoint: Datapoint{
					Timestamp: 100,
				},
			},
			{
				Datapoint: Datapoint{
					Timestamp: 1,
				},
			},
			{
				Datapoint: Datapoint{
					Timestamp: 1,
				},
			},
		},
	)

	require.Equal(t, []ChunkData{
		{
			Datapoint: Datapoint{
				Timestamp: 100,
			},
		},
		{
			Datapoint: Datapoint{
				Timestamp: 200,
			},
		},
		{
			Datapoint: Datapoint{
				Timestamp: 201,
			},
		},
		{
			Datapoint: Datapoint{
				Timestamp: 202,
			},
		},
	}, orig)
}
