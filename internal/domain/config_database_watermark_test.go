package domain_test

import (
	"strings"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestEffectiveQuery_NoWatermarkOrFirstRun_ReturnsBaseQuery(t *testing.T) {
	// No watermark config
	cfgNoWm := &domain.DatabaseSourceConfig{
		Table: "orders",
	}
	q, err := cfgNoWm.EffectiveQuery()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if q != "SELECT * FROM orders" {
		t.Fatalf("expected base query, got %q", q)
	}

	// First run: watermark configured but value is nil
	cfgFirstRun := &domain.DatabaseSourceConfig{
		Table: "orders",
		Watermark: &domain.WatermarkConfig{
			Column: "updated_at",
			Type:   domain.WatermarkTypeTimestamp,
			Value:  nil,
		},
	}
	q2, err := cfgFirstRun.EffectiveQuery()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if q2 != "SELECT * FROM orders" {
		t.Fatalf("expected base query without WHERE for first run, got %q", q2)
	}
}

func TestEffectiveQuery_AppendsPredicateAsSubquery(t *testing.T) {
	tsVal := "2026-10-24T00:00:00+00:00"
	cfg := &domain.DatabaseSourceConfig{
		Table: "orders",
		Watermark: &domain.WatermarkConfig{
			Column: "updated_at",
			Type:   domain.WatermarkTypeTimestamp,
			Value:  &tsVal,
		},
	}
	q, err := cfg.EffectiveQuery()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := "SELECT * FROM (SELECT * FROM orders) AS _wm WHERE updated_at > '2026-10-24T00:00:00+00:00'"
	if q != expected {
		t.Fatalf("expected subquery predicate:\nwant: %s\ngot:  %s", expected, q)
	}
}

func TestEffectiveQuery_KeepsOrderByQueryValid(t *testing.T) {
	intVal := "100"
	cfg := &domain.DatabaseSourceConfig{
		Query: "SELECT id, amount FROM orders ORDER BY id DESC",
		Watermark: &domain.WatermarkConfig{
			Column: "id",
			Type:   domain.WatermarkTypeInteger,
			Value:  &intVal,
		},
	}
	q, err := cfg.EffectiveQuery()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := "SELECT * FROM (SELECT id, amount FROM orders ORDER BY id DESC) AS _wm WHERE id > 100"
	if q != expected {
		t.Fatalf("expected subquery preserving ORDER BY:\nwant: %s\ngot:  %s", expected, q)
	}
}

func TestEffectiveQuery_ReplacesPlaceholderWithTypedLiteral(t *testing.T) {
	intVal := "1500"
	cfg := &domain.DatabaseSourceConfig{
		Query: "SELECT * FROM orders WHERE id > :watermark_value AND status = 'active'",
		Watermark: &domain.WatermarkConfig{
			Column: "id",
			Type:   domain.WatermarkTypeInteger,
			Value:  &intVal,
		},
	}
	q, err := cfg.EffectiveQuery()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := "SELECT * FROM orders WHERE id > 1500 AND status = 'active'"
	if q != expected {
		t.Fatalf("expected placeholder replaced without extra subquery:\nwant: %s\ngot:  %s", expected, q)
	}
}

func TestEffectiveQuery_PlaceholderWithoutValueFails(t *testing.T) {
	// Value is nil
	cfg := &domain.DatabaseSourceConfig{
		Query: "SELECT * FROM orders WHERE id > :watermark_value",
		Watermark: &domain.WatermarkConfig{
			Column: "id",
			Type:   domain.WatermarkTypeInteger,
			Value:  nil,
		},
	}
	_, err := cfg.EffectiveQuery()
	if err == nil || !strings.Contains(err.Error(), ":watermark_value") {
		t.Fatalf("expected error mentioning :watermark_value, got %v", err)
	}

	// No watermark config at all
	cfgNoWm := &domain.DatabaseSourceConfig{
		Query: "SELECT * FROM orders WHERE id > :watermark_value",
	}
	_, err2 := cfgNoWm.EffectiveQuery()
	if err2 == nil || !strings.Contains(err2.Error(), ":watermark_value") {
		t.Fatalf("expected error mentioning :watermark_value, got %v", err2)
	}
}

func TestEffectiveQuery_EscapesStringWatermarkInjection(t *testing.T) {
	strVal := "x' OR '1'='1"
	cfg := &domain.DatabaseSourceConfig{
		Table: "users",
		Watermark: &domain.WatermarkConfig{
			Column: "username",
			Type:   domain.WatermarkTypeString,
			Value:  &strVal,
		},
	}
	q, err := cfg.EffectiveQuery()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := "SELECT * FROM (SELECT * FROM users) AS _wm WHERE username > 'x'' OR ''1''=''1'"
	if q != expected {
		t.Fatalf("expected escaped string literal:\nwant: %s\ngot:  %s", expected, q)
	}
}
