package mongo

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/omnibeam/dataflow-compute-go/internal/adapters/secrets"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// OpenClient establishes a MongoDB connection pool using the DatabaseSourceConfig and resolves secrets.
func OpenClient(ctx context.Context, cfg *domain.DatabaseSourceConfig, resolver ports.SecretResolver) (*mongo.Client, error) {
	uri, err := secrets.ResolveConnectionURI(ctx, cfg.ConnectionURI, cfg.PasswordRef, resolver)
	if err != nil {
		return nil, fmt.Errorf("failed resolving mongodb connection uri: %w", err)
	}

	opts := options.Client().ApplyURI(uri)
	if cfg.PoolConfig.MaxOpenConns > 0 {
		opts.SetMaxPoolSize(uint64(cfg.PoolConfig.MaxOpenConns))
	}

	client, err := mongo.Connect(opts)
	if err != nil {
		return nil, fmt.Errorf("failed connecting to mongodb: %w", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("failed pinging mongodb: %w", err)
	}

	return client, nil
}

var _ ports.PartitionedReader = (*MongoSource)(nil)

// MongoSource provides partitioned document reading from a MongoDB collection.
type MongoSource struct {
	client *mongo.Client
	dbName string
}

func NewMongoSource(client *mongo.Client, dbName string) *MongoSource {
	return &MongoSource{
		client: client,
		dbName: dbName,
	}
}

// Close safely disconnects the MongoDB client pool.
func (s *MongoSource) Close() error {
	if s.client != nil {
		return s.client.Disconnect(context.Background())
	}
	return nil
}

// CalculatePartitions queries min and max _id to divide the collection into parallel slices.
func (s *MongoSource) CalculatePartitions(ctx context.Context, srcCfg any) ([]ports.PartitionSlice, error) {
	cfg, ok := srcCfg.(*domain.DatabaseSourceConfig)
	if !ok {
		return nil, fmt.Errorf("expected *domain.DatabaseSourceConfig, got %T", srcCfg)
	}
	if cfg.Table == "" {
		return nil, fmt.Errorf("mongo collection (table) cannot be empty")
	}

	dbName := s.resolveDatabaseName(cfg)
	if dbName == "" {
		return nil, fmt.Errorf("mongo database name cannot be empty")
	}

	numPartitions := cfg.PartitionConfig.NumPartitions
	if numPartitions <= 1 {
		return []ports.PartitionSlice{{SliceIndex: 0, IsFirst: true, IsLast: true}}, nil
	}

	if s.client == nil {
		return nil, fmt.Errorf("mongo client is not connected")
	}

	coll := s.client.Database(dbName).Collection(cfg.Table)
	minOID, maxOID, found := fetchMinMaxObjectIDs(ctx, coll)
	if !found {
		return []ports.PartitionSlice{{SliceIndex: 0, IsFirst: true, IsLast: true}}, nil
	}

	return BuildObjectIDRanges(minOID, maxOID, numPartitions), nil
}

func (s *MongoSource) resolveDatabaseName(cfg *domain.DatabaseSourceConfig) string {
	if s.dbName != "" {
		return s.dbName
	}
	return cfg.Database
}

func fetchMinMaxObjectIDs(ctx context.Context, coll *mongo.Collection) (bson.ObjectID, bson.ObjectID, bool) {
	var minDoc, maxDoc bson.M
	errMin := coll.FindOne(ctx, bson.M{}, options.FindOne().SetSort(bson.D{{Key: "_id", Value: 1}})).Decode(&minDoc)
	errMax := coll.FindOne(ctx, bson.M{}, options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}})).Decode(&maxDoc)
	if errMin != nil || errMax != nil {
		return bson.ObjectID{}, bson.ObjectID{}, false
	}

	minOID, ok1 := minDoc["_id"].(bson.ObjectID)
	maxOID, ok2 := maxDoc["_id"].(bson.ObjectID)
	return minOID, maxOID, ok1 && ok2
}

// ReadPartition streams records from the collection matching the slice boundary.
func (s *MongoSource) ReadPartition(
	ctx context.Context,
	srcCfg any,
	slice ports.PartitionSlice,
) (<-chan *domain.GenericRecord, <-chan error, error) {
	cfg, ok := srcCfg.(*domain.DatabaseSourceConfig)
	if !ok {
		return nil, nil, fmt.Errorf("expected *domain.DatabaseSourceConfig, got %T", srcCfg)
	}

	filter, err := buildFilter(cfg.QueryFilter, slice)
	if err != nil {
		return nil, nil, err
	}

	dbName := s.resolveDatabaseName(cfg)
	if s.client == nil {
		return nil, nil, fmt.Errorf("mongo client is not connected")
	}
	coll := s.client.Database(dbName).Collection(cfg.Table)

	findOpts := options.Find().SetBatchSize(int32(cfg.PartitionConfig.BatchSize))
	cursor, err := coll.Find(ctx, filter, findOpts)
	if err != nil {
		return nil, nil, fmt.Errorf("failed executing mongo find query: %w", err)
	}

	recChan := make(chan *domain.GenericRecord, 100)
	errChan := make(chan error, 1)
	go streamRecords(ctx, cursor, cfg, recChan, errChan)

	return recChan, errChan, nil
}

func buildFilter(queryFilter string, slice ports.PartitionSlice) (bson.M, error) {
	filter := bson.M{}
	if strings.TrimSpace(queryFilter) != "" {
		if err := json.Unmarshal([]byte(queryFilter), &filter); err != nil {
			return nil, fmt.Errorf("invalid json/bson query_filter: %w", err)
		}
	}
	applySliceBounds(filter, slice)
	return filter, nil
}

func applySliceBounds(filter bson.M, slice ports.PartitionSlice) {
	if slice.LowerBound == nil || slice.UpperBound == nil {
		return
	}
	lowOID, err1 := bson.ObjectIDFromHex(fmt.Sprintf("%v", slice.LowerBound))
	upOID, err2 := bson.ObjectIDFromHex(fmt.Sprintf("%v", slice.UpperBound))
	if err1 != nil || err2 != nil {
		return
	}

	idFilter := bson.M{}
	if slice.IsFirst {
		idFilter["$lte"] = upOID
	} else if slice.IsLast {
		idFilter["$gte"] = lowOID
	} else {
		idFilter["$gte"] = lowOID
		idFilter["$lt"] = upOID
	}
	filter["_id"] = idFilter
}

func streamRecords(
	ctx context.Context,
	cursor *mongo.Cursor,
	cfg *domain.DatabaseSourceConfig,
	recChan chan<- *domain.GenericRecord,
	errChan chan<- error,
) {
	defer close(recChan)
	defer close(errChan)
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			errChan <- fmt.Errorf("failed decoding mongo bson document: %w", err)
			return
		}

		rec, err := MapBSONToRecord(doc, &cfg.Schema, cfg.FlattenNested)
		if err != nil {
			errChan <- fmt.Errorf("failed mapping bson to generic record: %w", err)
			return
		}
		recChan <- rec
	}

	if err := cursor.Err(); err != nil {
		errChan <- fmt.Errorf("cursor error during iteration: %w", err)
	}
}
