# OmniBeam Pipeline Specification & Configuration Contract for Airflow & LLMs

This document defines the formal contract, schema specification, supported adapters, parameter constraints, and execution semantics of **OmniBeam-Go**. It is designed as an exhaustive, deterministic reference manual for Apache Airflow DAG authors, orchestrators, and Large Language Models (LLMs) responsible for dynamically generating or validating OmniBeam pipeline manifests.

---

## 1. Pipeline Execution Contract & CLI Interface

The compiled binary `pipeline` is a self-contained, statically linked executable (`CGO_ENABLED=0`, Linux `amd64`).

### 1.1 CLI Flags & Invocation Modes

The binary accepts configuration through flags and supports two primary execution runners:

| Flag | Type | Description | Mandatory / Precedence |
| :--- | :--- | :--- | :--- |
| `--config` | `string` | Local filesystem path to the JSON manifest file. | First priority if provided. |
| `--config_payload` | `string` | Raw JSON string containing the full pipeline manifest payload. | Second priority (ideal for Airflow inline execution/XCom). |
| `--config_payload_path` | `string` | File path containing the JSON configuration payload. | Third priority. |
| `--runner` | `string` | Execution engine: `direct` (local multi-threaded) or `dataflow` (Google Cloud Dataflow). | Overrides `runner` field in manifest if set. Default: `direct`. |

#### Airflow Operator Invocation Patterns:
- **BashOperator / Local Worker:**
  ```bash
  ./pipeline --config_payload='{{ params.manifest_json | tojson }}' --runner=direct
  ```
- **KubernetesPodOperator / DockerOperator:**
  ```bash
  /opt/dataflow/pipeline --config=/etc/omnibeam/manifest.json
  ```
- **Google Cloud Dataflow Flex Template:**
  ```bash
  gcloud dataflow flex-template run "job-name" \
    --template-file-gcs-location="gs://bucket/templates/omnibeam.json" \
    --parameters config_payload='{"pipeline_id":"...","source":{...}}'
  ```

### 1.2 Process Exit Codes & Return Semantics

- `0`: Pipeline completed successfully with zero unhandled fatal exceptions and DLQ error threshold satisfied.
- `1`: Validation error, fatal connection error, invalid configuration, or unrecoverable execution failure.

### 1.3 Data Conservation & Audit Invariant

Every record ingested by OmniBeam strictly satisfies the conservation law:
$$\text{TotalRecordsRead} = \text{RowsWritten} + \text{DeadLetterCount}$$
- No records are silently dropped.
- Failed rows with transformation/type errors are written to the DLQ quarantine path along with error reason and field-level traces.
- Metrics are exported to JSON in the configured `metrics_dir` or output directory upon completion.

---

## 2. Root Configuration Object (`PipelineConfig`)

The root JSON configuration object must adhere to the following schema:

| Key | JSON Type | Mandatory | Default | Accepted Values / Description |
| :--- | :--- | :--- | :--- | :--- |
| `pipeline_id` | `string` | **Yes** | — | Unique identifier for the pipeline definition (e.g. `order-ingestion-v1`). |
| `run_id` | `string` | **Yes** | — | Unique execution run identifier (e.g. Airflow `{{ run_id }}` or `{{ ds_nodash }}`). |
| `pipeline_type` | `string` | No | `"ingestion"` | `"ingestion"`, `"sql"`, `"api"`. |
| `runner` | `string` | No | `"direct"` | `"direct"`, `"dataflow"`. |
| `secrets_config` | `object` | No | `null` | Enterprise secrets resolution backend configuration (OpenBao/Vault & GCP Secret Manager). |
| `schema` | `object` | No | `null` | Top-level schema definition. If omitted, fallback to `source.schema`. |
| `source` | `object` | Cond. | — | File / Object Storage stream ingestion configuration. Required if `database_source` and `api_source` are null. |
| `database_source` | `object` | Cond. | — | SQL / NoSQL partitioned database ingestion configuration. |
| `api_source` | `object` | Cond. | — | Paginated REST HTTP API ingestion configuration. |
| `destination` | `object` | **Yes** | — | Output destination configuration (Files, BigQuery, REST API). |
| `dlq_config` | `object` | **Yes** | — | Dead Letter Queue and quarantine threshold settings. |
| `quality_config` | `object` | No | `null` | Data quality assertions and schema validation rules. |
| `security` | `object` | No | `null` | Sensitive fields list for PII redaction and credential masking. |

---

## 3. Universal Secrets & Credential Routing (`secrets_config`)

OmniBeam resolves credentials dynamically at runtime via HashiCorp Vault / OpenBao, Google Cloud Secret Manager, or plaintext literals.

### 3.1 `secrets_config` Specification

```json
{
  "provider": "openbao",
  "vault_url": "http://openbao:8200",
  "vault_token": "root",
  "gcp_project_id": "my-gcp-project",
  "host_override": {
    "postgres.prod.internal": "postgres.staging.internal"
  }
}
```

| Field | Type | Mandatory | Description |
| :--- | :--- | :--- | :--- |
| `provider` | `string` | No | Default provider if URI prefix is omitted. Values: `"openbao"`, `"vault"`, `"bao"`, `"gcp"`, `"gcp_sm"`, `"gcp_secret_manager"`. |
| `vault_url` | `string` | Cond. | Base URL for OpenBao / HashiCorp Vault API. |
| `vault_token` | `string` | Cond. | Authentication token for OpenBao / Vault. |
| `gcp_project_id` | `string` | Cond. | Google Cloud Project ID for Secret Manager resolution. |
| `host_override` | `map[string]string` | No | Hostname replacement dictionary for database connections across network topologies. |

### 3.2 Secret URI Resolution Syntax

Secret references anywhere in the configuration (e.g. `password_ref`, `token_ref`, `public_key_ref`) support polymorphic URI schemes:

- **OpenBao / Vault KV v2:** `openbao:<mount>/<path>#<field>` or `vault:<mount>/<path>#<field>`  
  *Example:* `openbao:secret/data/database/mysql#password`
- **Google Cloud Secret Manager:** `gcp:<secret_name>` or `gcp:projects/<project>/secrets/<name>/versions/<version>`  
  *Example:* `gcp:postgres-prod-password`
- **Plaintext Literal:** If no provider prefix is present and the string does not contain path separators `/`, it is treated as a literal plaintext string.
- **Connection URI Password Interpolation:** Database `connection_uri` templates can include `{{password}}` or `{{PASSWORD}}` which will be automatically replaced with the resolved `password_ref`.

---

## 4. Source Connectors Contract

OmniBeam supports three distinct source families. Exactly one source configuration must be populated.

### 4.1 ByteStream / Storage Source (`source`)

Ingests flat, columnar, compressed, and encrypted files from local filesystem, Google Cloud Storage (`gs://`), or AWS S3 (`s3://`).

| Field | Type | Mandatory | Default | Accepted Values / Description |
| :--- | :--- | :--- | :--- | :--- |
| `type` | `string` | No | `"file"` | `"file"`, `"storage"`, `"gcs"`, `"local_storage"`. |
| `path` | `string` | Cond. | `""` | Single source file path or glob pattern (e.g. `gs://bucket/data/*.csv.gz`). |
| `paths` | `[]string` | Cond. | `[]` | Explicit array of file paths. Has precedence over `path`. |
| `format` | `string` | No | `"csv"` | `"csv"`, `"txt"`, `"tsv"`, `"json"`, `"jsonl"`, `"jsonlines"`. |
| `delimiter` | `string` | No | `","` | `","`, `";"`, `"|"`, `"\t"`. |
| `quote_char` | `string` | No | `"\""` | `"\""`, `"'"` or custom single character. |
| `multiline` | `bool` | No | `false` | `true` if CSV records contain unescaped line breaks inside quoted fields. |
| `charset` | `string` | No | `"utf-8"` | `"utf-8"`, `"utf8"`, `"iso-8859-1"`, `"latin1"`, `"latin-1"`, `"windows-1252"`, `"cp1252"`, `"utf-16le"`, `"utf-16be"`. |
| `compression` | `string` | No | `"none"` | `"none"`, `""`, `"gzip"`, `"gz"`, `"zstd"`, `"snappy"`, `"sz"`, `"bzip2"`, `"bz2"`. |
| `chunk_size_bytes` | `int64` | No | `67108864` (64MB) | Sub-split chunk size in bytes for non-compressed ByteStream Splittable DoFns. |
| `schema` | `object` | **Yes** | — | Typed schema declaration (see Section 6). |

### 4.2 Database PartitionQuery Source (`database_source`)

Executes parallel bounded queries against relational and document databases by calculating non-overlapping partition slices.

| Field | Type | Mandatory | Default | Accepted Values / Description |
| :--- | :--- | :--- | :--- | :--- |
| `driver` | `string` | **Yes** | — | `"postgres"`, `"pgx"`, `"cockroach"`, `"mysql"`, `"mariadb"`, `"mongo"`, `"mongodb"`. |
| `connection_uri` | `string` | Cond. | `""` | Full DSN connection string (e.g. `postgres://user:{{password}}@host:5432/db?sslmode=disable`). |
| `host` | `string` | Cond. | `""` | Hostname (used if `connection_uri` is omitted). |
| `port` | `int` | No | `0` | Port number. |
| `database` | `string` | No | `""` | Target database name. |
| `username` | `string` | No | `""` | Database username. |
| `password_ref` | `string` | No | `""` | Secret reference for database password. |
| `table` | `string` | Cond. | `""` | Target table or MongoDB collection name. |
| `query` | `string` | Cond. | `""` | Full custom SQL query (overrides `table`). |
| `query_filter` | `string` | No | `""` | SQL WHERE condition filter appended when querying `table` (e.g. `status = 'ACTIVE'`). |
| `columns` | `[]string` | No | `[]` | Explicit column selection (defaults to `*`). |
| `flatten_nested` | `bool` | No | `false` | For MongoDB: flattens nested BSON/JSON objects to top-level schema attributes with dot notation. |
| `partition_config` | `object` | No | `{}` | Partitioning parameters (see below). |
| `pool_config` | `object` | No | `{}` | Connection pool tuning (see below). |
| `resilience` | `object` | No | `{}` | Retry, timeout, and circuit breaker settings (see Section 4.4). |
| `schema` | `object` | **Yes** | — | Expected schema for columns (see Section 6). |

#### `database_source.partition_config`:
- `partition_column` (`string`): Column used for numeric or timestamp interval splitting (e.g. `id`, `created_at`).
- `batch_size` (`int`, default: `2500`): Maximum rows per partition query slice.
- `num_partitions` (`int`, default: `0`): Target number of concurrent partitions (0 = dynamic calculation from table min/max).

#### `database_source.pool_config`:
- `max_open_conns` (`int`, default: `4`): Maximum active connections per worker node.
- `max_idle_conns` (`int`, default: `2`): Maximum idle connections per worker node.
- `conn_max_lifetime_s` (`int`, default: `300`): Max connection lifetime in seconds.

### 4.3 Paged REST API Source (`api_source`)

Ingests data from paginated HTTP/REST web services with built-in token-bucket rate limiting, circuit breaker, and automatic auth renewal.

| Field | Type | Mandatory | Default | Accepted Values / Description |
| :--- | :--- | :--- | :--- | :--- |
| `base_url` | `string` | **Yes** | — | Base HTTP URL (e.g. `https://api.example.com`). |
| `endpoint` | `string` | **Yes** | — | Relative API endpoint path (e.g. `/v1/events`). |
| `http_method` | `string` | No | `"GET"` | `"GET"`, `"POST"`. |
| `headers` | `map[string]string` | No | `{}` | Static HTTP headers sent with every request. |
| `query_params` | `map[string]string` | No | `{}` | Static query parameters. |
| `auth` | `object` | No | `null` | Authentication configuration (see below). |
| `pagination` | `object` | **Yes** | — | Pagination strategy configuration (see below). |
| `retry` | `object` | No | `{}` | Resilience & backoff configuration (see Section 4.4). |
| `skip_tls_verify` | `bool` | No | `false` | If `true`, disables TLS certificate verification (development/testing only). |
| `records_path` | `string` | No | `""` | JSONPath to the nested array of record objects (e.g. `"data.items"`, `"results"`). |
| `field_mapping` | `map[string]string` | No | `{}` | Source JSON key to Destination Schema column name mappings. |
| `schema` | `object` | **Yes** | — | Typed schema for extracting and validating fields. |

#### `api_source.auth`:
- `type` (`string`, mandatory): `"bearer_token"`, `"api_key"`, `"basic_auth"`, `"oauth2_client_credentials"`.
- `token_ref` (`string`): Secret ref for bearer token.
- `api_key_header` (`string`): Header name for API key (e.g. `"X-API-Key"`).
- `api_key_query` (`string`): Query param name for API key.
- `username_ref` (`string`): Secret ref for basic auth or OAuth username.
- `password_ref` (`string`): Secret ref for basic auth or OAuth password.
- `token_url` (`string`): OAuth2 token issuance endpoint URL.
- `client_id_ref` (`string`): Secret ref for OAuth2 client ID.
- `client_secret_ref` (`string`): Secret ref for OAuth2 client secret.
- `scopes` (`[]string`): List of OAuth2 scopes.

#### `api_source.pagination`:
- `type` (`string`, mandatory):
  - `"page_number"`: Incremental page counter (`?page=1, ?page=2`).
  - `"offset_limit"`: Offset and limit (`?offset=0&limit=100`).
  - `"cursor_token"`: Opaque cursor string extracted from response payload.
  - `"link_header"`: RFC 5988 `Link: <url>; rel="next"` header navigation.
- `page_param` (`string`, default: `"page"` or `"offset"`): Query param name for the page index/offset.
- `size_param` (`string`, default: `"size"` or `"limit"`): Query param name for page size.
- `page_size` (`int`, default: `100`): Number of records requested per page.
- `initial_page` (`int`, default: `1`): Starting page index for `page_number` pagination.
- `max_pages_limit` (`int`, default: `10000`): Circuit breaker limit to prevent infinite loops.
- `total_pages_hint` (`int`, default: `0`): Known total pages hint for parallel SDF pre-splitting.
- `cursor_param` (`string`): Query param name for cursor token (for `"cursor_token"`).
- `next_cursor_path` (`string`): JSONPath in JSON response to extract next cursor (e.g. `"meta.next_cursor"`).
- `total_count_path` (`string`): JSONPath to extract total record count for dynamic split estimation.
- `has_more_path` (`string`): JSONPath to boolean flag indicating whether further pages exist.

### 4.4 Resilience & Fault Tolerance Parameters (`ResilienceConfig`)

Configured under `api_source.retry`, `database_source.resilience`, or `destination.api_options`:

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `max_retries` | `int` | `3` | Maximum retry attempts for transient errors. |
| `initial_backoff_ms` | `int` | `500` | Initial exponential backoff delay in milliseconds. |
| `max_backoff_ms` | `int` | `10000` | Maximum backoff delay cap (10s). |
| `backoff_multiplier` | `float64` | `2.0` | Exponential backoff multiplier factor. |
| `rate_limit_rps` | `float64` | `50.0` | Token bucket rate limit (requests per second). |
| `timeout_ms` | `int` | `30000` | HTTP / DB query execution timeout in milliseconds (30s). |
| `circuit_breaker_fails` | `int` | `5` | Consecutive failures before opening the circuit breaker. |
| `circuit_breaker_timeout_ms` | `int` | `10000` | Cooldown period before transitioning to half-open state. |
| `half_open_limit` | `int` | `1` | Trial requests permitted in half-open state. |

---

## 5. Destination Sinks Contract

The `destination` block configures the output writer. OmniBeam automatically routes execution to the registered sink adapter based on `destination.type`.

### 5.1 Storage & File Sinks

Applies when `destination.type` is `"file"`, `"storage"`, `"local_storage"`, `"gcs"`, `"s3"`, `"parquet"`, `"csv"`, `"jsonl"`, `"txt"`, or `"tsv"`.

| Field | Type | Mandatory | Default | Accepted Values / Description |
| :--- | :--- | :--- | :--- | :--- |
| `type` | `string` | No | `"file"` | `"file"`, `"storage"`, `"local_storage"`, `"gcs"`, `"s3"`, `"parquet"`, `"csv"`, `"jsonl"`. |
| `output_path` | `string` | **Yes** | — | Directory or file URI (`gs://bucket/path`, `s3://bucket/path`, `/local/path`). |
| `output_format` | `string` | No | `"parquet"` | `"parquet"`, `"csv"`, `"jsonl"`, `"json"`, `"jsonlines"`, `"txt"`, `"tsv"`. |
| `compression` | `string` | No | `"snappy"` | For Parquet: `"snappy"`, `"zstd"`, `"gzip"`, `"none"`.<br>For Delimited/JSONL: `"gzip"`, `"zstd"`, `"none"`. |
| `single_file` | `bool` | No | `false` | If `true`, fuses all worker partitions into a single consolidated output file. |
| _(audit columns)_ | — | — | — | The `_ingested_at` column is **always** injected into Parquet output. The `_source_file` column is injected **automatically** only when the source is a file/storage (`source` block); it is omitted for `database_source` and `api_source` pipelines. These columns are not user-configurable. |
| `metrics_dir` | `string` | No | `""` | Custom directory path where execution metrics JSON is saved (defaults to `output_path` directory). |
| `encryption` | `object` | No | `{"type":"none"}`| Stream-level envelope encryption (see Section 5.1.1). |
| `format_options` | `object` | No | `{}` | Delimited formatting options (see Section 5.1.2). |

#### 5.1.1 `destination.encryption` Specification

Applied after compression during file write. The encryption extension is automatically chained (e.g. `orders.csv.gz.pgp` or `data.parquet.pgp`).

- `type` (`string`): `"none"` (default), `"pgp"`, `"kms"`.
- `public_key_ref` (`string`): Secret reference resolving the recipient's ASCII-armored or binary OpenPGP public key (mandatory if `type = "pgp"`).
- `kms_key_ref` (`string`): GCP KMS Key Resource Name for envelope encryption (mandatory if `type = "kms"`).

#### 5.1.2 `destination.format_options` Specification

- `delimiter` (`string`, default: `","`): `","`, `";"`, `"|"`, `"\t"`.
- `include_header` (`bool`, default: `false`): Writes CSV column header row at top of file.
- `charset` (`string`, default: `"utf-8"`): `"utf-8"`, `"iso-8859-1"`, `"windows-1252"`.
- `line_terminator` (`string`, default: `"\n"`): `"\n"`, `"\r\n"`.
- `quote_all` (`bool`, default: `false`): Quotes all fields regardless of data type.

### 5.2 Google BigQuery Sink (`destination.type = "bigquery"`)

Writes records directly to BigQuery via the high-throughput, zero-copy Google Cloud Storage Write API.

| Field | Type | Mandatory | Default | Accepted Values / Description |
| :--- | :--- | :--- | :--- | :--- |
| `type` | `string` | **Yes** | — | Must be `"bigquery"`. |
| `bigquery_options` | `object` | **Yes** | — | BigQuery target parameters (see below). |

#### `destination.bigquery_options`:
- `project_id` (`string`, optional): Google Cloud Project ID (defaults to ambient GCP credentials).
- `dataset_id` (`string`, mandatory): BigQuery Dataset name.
- `table_id` (`string`, mandatory): BigQuery Table name.
- `write_disposition` (`string`, default: `"WRITE_APPEND"`):
  - `"WRITE_APPEND"`: Appends new rows to existing table data.
  - `"WRITE_TRUNCATE"`: Erases existing table data and replaces with batch output.
  - `"WRITE_EMPTY"`: Fails the job if the target table contains existing rows.
- `batch_size` (`int`, default: `500`): Number of protobuf rows per pending-stream flush chunk.

### 5.3 REST HTTP API Sink (`destination.type = "rest_api" | "http_api"`)

Batches and posts processed records to external HTTP microservices or webhook endpoints.

| Field | Type | Mandatory | Default | Accepted Values / Description |
| :--- | :--- | :--- | :--- | :--- |
| `type` | `string` | **Yes** | — | `"rest_api"` or `"http_api"`. |
| `endpoint` | `object` | **Yes** | — | Target endpoint connection parameters. |
| `api_options` | `object` | **Yes** | — | Micro-batching and HTTP request options. |

#### `destination.endpoint`:
- `base_url` (`string`, mandatory): Base endpoint URL (e.g. `https://webhook.site`).
- `credential_ref` (`string`): Secret ref for bearer auth token or API key.
- `auth_type` (`string`): `"bearer_token"`, `"api_key"`, `"basic_auth"`.
- `headers` (`map[string]string`): Custom headers (e.g. `{"Content-Type": "application/json"}`).

#### `destination.api_options`:
- `resource_path` (`string`, mandatory): Resource subpath (e.g. `"/api/v2/ingest"`).
- `method` (`string`, default: `"POST"`): `"POST"`, `"PUT"`, `"PATCH"`.
- `batch_size` (`int`, default: `100`): Number of records grouped in each HTTP request body.
- `body_envelope` (`string`, default: `""`): If populated, wraps the batch array under a root JSON key (e.g. `{"records": [...]}`).
- `rate_limit_rps` (`int`, default: `50`): Max outgoing requests per second per worker.
- `timeout_ms` (`int`, default: `30000`): Request timeout in milliseconds.
- `max_retries` (`int`, default: `3`): Retry attempts for HTTP 429, 500, 502, 503, 504.

---

## 6. Schema & Data Types Contract (`schema`)

OmniBeam employs a strongly typed, zero-allocation schema model. Schemas can be declared at the top-level `schema` or inside `source.schema`, `database_source.schema`, or `api_source.schema`.

### 6.1 Schema Structure

```json
{
  "fields": [
    {
      "name": "id",
      "type": "int64",
      "nullable": false
    },
    {
      "name": "balance",
      "type": "decimal",
      "scale": 4,
      "on_overflow": "round",
      "nullable": false
    }
  ]
}
```

### 6.2 Supported Data Types & Validation Rules

| Data Type (`type`) | Representation | Target Parquet Physical Type | Go Native Type | Supported Coercions & Formats |
| :--- | :--- | :--- | :--- | :--- |
| `"string"` | UTF-8 String | `BYTE_ARRAY` (UTF8) | `string` | Text, hex, UUIDs. |
| `"int64"` | 64-bit Integer | `INT64` | `int64` | Numeric strings, standard integer notation. |
| `"float64"` | 64-bit IEEE Float | `DOUBLE` | `float64` | Floats, scientific notation (`1.23e+04`). |
| `"bool"` | Boolean | `BOOLEAN` | `bool` | `true`/`false`, `"1"`/`"0"`, `"t"`/`"f"`, `"yes"`/`"no"`. |
| `"timestamp"` | Timestamp UTC | `INT64` (TIMESTAMP_MICROS) | `time.Time` | RFC3339 (`2026-09-29T11:00:00Z`), ISO-8601, Unix epoch seconds/millis. |
| `"date"` | Calendar Date | `INT32` (DATE) | `string` / Days | `YYYY-MM-DD`. |
| `"decimal"` | Fixed-point Number | `FIXED_LEN_BYTE_ARRAY` / `INT64` | `*apd.Decimal` / Scaled int | Exact financial math. Requires `scale` parameter. |
| `"bytes"` | Byte array | `BYTE_ARRAY` | `[]byte` | Base64 strings or binary byte slices. |
| `"json"` | Raw JSON Payload | `BYTE_ARRAY` (JSON) | `string` | Preserves valid nested JSON object/array without coercion. |

### 6.3 Decimal Precision Constraints
- `scale` (`int32`, mandatory for `"decimal"`): Number of fractional decimal digits preserved (e.g. `scale: 2` preserves `12.34`).
- `on_overflow` (`string`, default: `"round"`): `"round"` (rounds half-up to configured scale) or `"fail"` (routes record to DLQ if precision is lost).

---

## 7. Data Quality, Dead-Letter Queue & Security

### 7.1 Dead-Letter Queue (`dlq_config`)

| Field | Type | Mandatory | Default | Description |
| :--- | :--- | :--- | :--- | :--- |
| `enabled` | `bool` | **Yes** | `true` | Enables routing of unparseable or schema-violating records to quarantine. |
| `quarantine_path` | `string` | **Yes** | — | Directory URI where failed record JSONL and audit traces are written. |
| `max_error_percentage` | `float64`| No | `0.0` | Tolerance threshold (0.0 to 100.0). If the ratio of DLQ records exceeds this percentage, the pipeline terminates with exit code 1. |

### 7.2 Data Quality Rules (`quality_config.rules`)

Executes inline assertions during Beam pipeline transformation:

| Rule `type` | Parameters | Description | Action on Violation |
| :--- | :--- | :--- | :--- |
| `"not_null"` | `column: "col_name"` | Validates that the specified column is not null or whitespace-only. | Routes record to DLQ. |
| `"accepted_values"` | `column: "col_name"`, `values: ["A", "B"]` | Validates that column value is contained in the allowed enum list. | Routes record to DLQ. |
| `"row_count_min"` | `value: 100` | Asserts that total successful rows processed is $\ge$ `value`. | Fails pipeline after completion. |
| `"row_count_max"` | `value: 5000000` | Asserts that total successful rows processed is $\le$ `value`. | Fails pipeline after completion. |

### 7.3 Operational Security & PII Redaction (`security`)

```json
{
  "sensitive_fields": ["ssn", "credit_card", "password", "api_token"]
}
```
- `sensitive_fields` (`[]string`): Field names whose values are automatically masked (`[REDACTED]`) in all OpenTelemetry traces, structured logs, and quarantined DLQ error payloads to guarantee PCI-DSS / GDPR compliance.

---

## 8. Output Artifacts & Execution Telemetry

Upon completion, OmniBeam produces four distinct output categories:

1. **Target Sink Output:** Parquet columnar files, delimited files, BigQuery table partitions, or API ingest confirmation.
2. **Quarantine Files (`<quarantine_path>/dlq-*.jsonl`):** JSONL containing original payload, raw bytes, error code, failure stage, and timestamp.
3. **Audit Files (`<quarantine_path>/audit-*.jsonl`):** Job-level metadata containing run ID, worker host, pipeline ID, start/end timestamps, and final record counters.
4. **Metrics Report (`metrics_*.json`):** Machine-readable JSON summary containing associative counters:
   - `records_read`: Total raw input records detected.
   - `records_written`: Total records successfully written to analytical sink.
   - `records_quarantined`: Total records routed to DLQ.
   - `bytes_read`: Total input bytes processed across all partitions.
   - `bytes_written`: Total output bytes flushed.
   - `error_percentage`: Calculated DLQ ratio.

---

## 9. Airflow LLM Generator Reference Cheat Sheet

When generating pipeline manifests with an LLM in Airflow:

1. Always provide valid `pipeline_id` and `run_id`.
2. Select exactly one source: `source` (files/GCS), `database_source` (Postgres/MySQL/MongoDB), or `api_source` (REST).
3. Set `destination.type`:
   - Files: `"file"`, `"local_storage"`, `"gcs"` with `output_format`: `"parquet"`, `"csv"`, `"jsonl"`.
   - BigQuery: `"bigquery"` with `bigquery_options.dataset_id` and `bigquery_options.table_id`.
   - API: `"rest_api"` with `endpoint.base_url` and `api_options.resource_path`.
4. Ensure all schema `fields` have non-empty `name`, valid `type`, and boolean `nullable`.
5. For `"decimal"` fields, always specify integer `scale`.
6. Use secret URIs (`openbao:...` or `gcp:...`) rather than hardcoded credentials.
