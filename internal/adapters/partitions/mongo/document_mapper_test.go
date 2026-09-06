package mongo_test

import (
	"testing"
	"time"

	"github.com/omnibeam/dataflow-compute-go/internal/adapters/partitions/mongo"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestMapBSONToRecord_AllTypes(t *testing.T) {
	oid, _ := bson.ObjectIDFromHex("507f1f77bcf86cd799439011")
	now := time.Now().UTC().Truncate(time.Millisecond)

	schema := &domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeString, Nullable: false},
			{Name: "name", Type: domain.TypeString, Nullable: false},
			{Name: "amount", Type: domain.TypeFloat64, Nullable: false},
			{Name: "count", Type: domain.TypeInt64, Nullable: false},
			{Name: "active", Type: domain.TypeBool, Nullable: false},
			{Name: "created_at", Type: domain.TypeTimestamp, Nullable: false},
			{Name: "data_bytes", Type: domain.TypeBytes, Nullable: true},
			{Name: "address", Type: domain.TypeString, Nullable: true},
			{Name: "opt_field", Type: domain.TypeString, Nullable: true},
		},
	}

	doc := bson.M{
		"_id":        oid,
		"name":       "John Doe",
		"amount":     129.50,
		"count":      int32(42),
		"active":     true,
		"created_at": now,
		"data_bytes": []byte{0x01, 0x02, 0x03},
		"address": bson.M{
			"street": "Main St",
			"city":   "São Paulo",
		},
	}

	t.Run("Maps all standard BSON types properly", func(t *testing.T) {
		rec, err := mongo.MapBSONToRecord(doc, schema, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if rec.Get(0).StringVal() != oid.Hex() {
			t.Errorf("expected hex id, got %s", rec.Get(0).StringVal())
		}
		if rec.Get(3).Int64Val() != 42 {
			t.Errorf("expected count 42, got %d", rec.Get(3).Int64Val())
		}
		if len(rec.Get(6).BytesVal()) != 3 {
			t.Errorf("expected 3 bytes, got %d", len(rec.Get(6).BytesVal()))
		}
		if !rec.Get(8).IsNull {
			t.Errorf("expected opt_field to be null")
		}
	})

	t.Run("Fails when non-nullable field is missing", func(t *testing.T) {
		missingDoc := bson.M{"_id": oid}
		_, err := mongo.MapBSONToRecord(missingDoc, schema, false)
		if err == nil {
			t.Error("expected error for missing non-nullable field, got nil")
		}
	})

	t.Run("FlattenDocument with nested bson.D and bson.M", func(t *testing.T) {
		nestedDoc := bson.M{
			"root": bson.D{
				{Key: "leaf1", Value: "val1"},
				{Key: "leaf2", Value: int64(100)},
			},
		}
		out := make(map[string]any)
		mongo.FlattenDocument("", nestedDoc, out)

		if out["root.leaf1"] != "val1" || out["root.leaf2"] != int64(100) {
			t.Errorf("unexpected flattened map: %+v", out)
		}
	})

	t.Run("Type mapping permutations and incompatible value errors", func(t *testing.T) {
		typesSchema := &domain.Schema{
			Fields: []domain.Field{
				{Name: "i64", Type: domain.TypeInt64, Nullable: false},
				{Name: "f64", Type: domain.TypeFloat64, Nullable: false},
				{Name: "ts", Type: domain.TypeTimestamp, Nullable: false},
				{Name: "by", Type: domain.TypeBytes, Nullable: false},
				{Name: "b", Type: domain.TypeBool, Nullable: false},
			},
		}

		// Valid conversions across alternative source types
		validDoc := bson.M{
			"i64": int(10),
			"f64": int32(100),
			"ts":  bson.DateTime(now.UnixMilli()),
			"by":  bson.Binary{Data: []byte("blob")},
			"b":   true,
		}
		rec, err := mongo.MapBSONToRecord(validDoc, typesSchema, false)
		if err != nil {
			t.Fatalf("MapBSONToRecord: %v", err)
		}
		if rec.Get(0).Int64Val() != 10 || rec.Get(1).Float64Val() != 100.0 || rec.Get(3).String() != "blob" {
			t.Errorf("unexpected mapped record: %+v", rec)
		}

		// Float64 with int type
		docIntFloat := bson.M{
			"i64": int64(10),
			"f64": int(55),
			"ts":  now,
			"by":  []byte("blob"),
			"b":   true,
		}
		recIntFloat, err := mongo.MapBSONToRecord(docIntFloat, typesSchema, false)
		if err != nil || recIntFloat.Get(1).Float64Val() != 55.0 {
			t.Errorf("unexpected int to float64 mapped record: %v", err)
		}

		// String field with bson.A and complex types
		strSchema := &domain.Schema{
			Fields: []domain.Field{
				{Name: "items", Type: domain.TypeString, Nullable: false},
				{Name: "obj", Type: domain.TypeString, Nullable: false},
			},
		}
		docComplexStr := bson.M{
			"items": bson.A{"val1", "val2"},
			"obj":   bson.D{{Key: "nestedKey", Value: "nestedVal"}},
		}
		recComplexStr, err := mongo.MapBSONToRecord(docComplexStr, strSchema, false)
		if err != nil || recComplexStr.Get(0).StringVal() == "" {
			t.Errorf("failed complex string mapping: %v", err)
		}

		// String timestamp conversion
		tsStrDoc := bson.M{
			"i64": float64(20.0),
			"f64": int64(100),
			"ts":  "2026-08-20T12:00:00Z",
			"by":  []byte("raw"),
			"b":   false,
		}
		recStr, err := mongo.MapBSONToRecord(tsStrDoc, typesSchema, false)
		if err != nil || recStr.Get(0).Int64Val() != 20 {
			t.Errorf("failed string timestamp conversion: %v", err)
		}

		// Incompatible errors
		incompatibles := []struct {
			field string
			val   any
		}{
			{"i64", "not-a-number"},
			{"f64", "not-a-float"},
			{"ts", "invalid-date-format"},
			{"ts", 1234567},
			{"by", "not-bytes"},
			{"b", "not-a-bool"},
		}

		for _, tc := range incompatibles {
			badDoc := bson.M{
				"i64": int64(1),
				"f64": 1.0,
				"ts":  now,
				"by":  []byte("ok"),
				"b":   true,
			}
			badDoc[tc.field] = tc.val
			_, err := mongo.MapBSONToRecord(badDoc, typesSchema, false)
			if err == nil {
				t.Errorf("expected error for incompatible value on field %s, got nil", tc.field)
			}
		}
	})
}
