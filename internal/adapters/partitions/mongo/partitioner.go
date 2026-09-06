package mongo

import (
	"encoding/binary"
	"time"

	"github.com/omnibeam/dataflow-compute-go/internal/ports"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// BuildObjectIDRanges splits the timestamp interval between minID and maxID into N discrete PartitionSlices.
func BuildObjectIDRanges(minID, maxID bson.ObjectID, numPartitions int) []ports.PartitionSlice {
	if numPartitions <= 1 {
		return buildSingleSlice(minID, maxID)
	}

	minSec := minID.Timestamp().Unix()
	maxSec := maxID.Timestamp().Unix()
	if maxSec <= minSec {
		return buildSingleSlice(minID, maxID)
	}

	secStep := calculateStep(minSec, maxSec, numPartitions)
	return generateSlices(minID, maxID, minSec, secStep, numPartitions)
}

func buildSingleSlice(minID, maxID bson.ObjectID) []ports.PartitionSlice {
	return []ports.PartitionSlice{
		{
			SliceIndex: 0,
			LowerBound: minID.Hex(),
			UpperBound: maxID.Hex(),
			IsFirst:    true,
			IsLast:     true,
		},
	}
}

func calculateStep(minSec, maxSec int64, numPartitions int) int64 {
	step := (maxSec - minSec) / int64(numPartitions)
	if step < 1 {
		return 1
	}
	return step
}

func generateSlices(minID, maxID bson.ObjectID, minSec, secStep int64, numPartitions int) []ports.PartitionSlice {
	var slices []ports.PartitionSlice
	currentMin := minID

	for i := 0; i < numPartitions; i++ {
		isLast := (i == numPartitions-1)
		nextBound := calculateNextBound(minSec, secStep, i, isLast, maxID)

		slices = append(slices, ports.PartitionSlice{
			SliceIndex: i,
			LowerBound: currentMin.Hex(),
			UpperBound: nextBound.Hex(),
			IsFirst:    (i == 0),
			IsLast:     isLast,
		})

		currentMin = nextBound
	}

	return slices
}

func calculateNextBound(minSec, secStep int64, idx int, isLast bool, maxID bson.ObjectID) bson.ObjectID {
	if isLast {
		return maxID
	}
	targetSec := minSec + int64(idx+1)*secStep
	targetTime := time.Unix(targetSec, 0).UTC()

	var raw [12]byte
	binary.BigEndian.PutUint32(raw[0:4], uint32(targetTime.Unix()))
	return bson.ObjectID(raw)
}
