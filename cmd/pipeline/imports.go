package main

import (
	_ "github.com/omnibeam/dataflow-compute-go/internal/adapters/paged_api/sink"
	_ "github.com/omnibeam/dataflow-compute-go/internal/adapters/paged_api/source"
	_ "github.com/omnibeam/dataflow-compute-go/internal/adapters/partitions/mongo"
	_ "github.com/omnibeam/dataflow-compute-go/internal/adapters/partitions/sql"
	_ "github.com/omnibeam/dataflow-compute-go/internal/adapters/secrets"
	_ "github.com/omnibeam/dataflow-compute-go/internal/adapters/storage"
	_ "github.com/omnibeam/dataflow-compute-go/internal/adapters/streams/codecs/charsets"
	_ "github.com/omnibeam/dataflow-compute-go/internal/adapters/streams/codecs/compression"
	_ "github.com/omnibeam/dataflow-compute-go/internal/adapters/streams/formatters"
	_ "github.com/omnibeam/dataflow-compute-go/internal/adapters/streams/parsers"
	_ "github.com/omnibeam/dataflow-compute-go/internal/adapters/telemetry"
)
