# Omnibeam Dataflow Compute Engine — Arquitetura de Adaptadores, Fontes (SDF) e Sinks

**Versão:** 1.0  
**Data:** 2026-08-24  
**Escopo:** Guia Visual de Arquitetura, Famílias de Ingestão, Criação de Sinks e Oportunidades de Melhoria  
**Governança:** Clean Architecture | Hexagonal Ports & Adapters | Apache Beam Splittable DoFns (SDF)

---

## 1. Visão Holística da Arquitetura

O **Omnibeam Dataflow Compute Engine** foi projetado para processar terabytes de dados com **zero acoplamento** entre as tecnologias de armazenamento e o grafo de execução do **Apache Beam / Google Cloud Dataflow**.

A arquitetura adota o padrão **Ports & Adapters (Hexagonal)**:
- **`internal/domain`**: Modelos puros, tipos de dados canônicos (`GenericRecord`, `FieldValue`, `Schema`), catálogo de defaults e métricas. Zero dependências externas.
- **`internal/ports`**: Contratos abstratos (`interfaces`) para fontes, leitores, decodificadores, gravadores e cofres de segredos.
- **`internal/beam/core`**: Grafo de execução unificado do Apache Beam (DAG), validação de tipos, roteamento de quarentena (DLQ) e stager atômico de arquivos.
- **`internal/adapters/*`**: Implementações concretas de tecnologia (PostgreSQL, GCS, REST APIs, OpenBao, etc.).

```
                               ┌─────────────────────────────────────────────────────────┐
                               │                    INTERNAL / PORTS                     │
                               │  (Interfaces Abstratas: Storage, Partitions, Sinks)     │
                               └────────────────────────────┬────────────────────────────┘
                                                            │ Implementa
                       ┌────────────────────────────────────┼────────────────────────────────────┐
                       │                                    │                                    │
                       ▼                                    ▼                                    ▼
       ┌──────────────────────────────┐     ┌──────────────────────────────┐     ┌──────────────────────────────┐
       │   internal/adapters/streams  │     │ internal/adapters/partitions │     │ internal/adapters/paged_api  │
       │   • GCS / LocalStorage       │     │ • PostgreSQL / MySQL         │     │ • RESTReader (HTTP Client)   │
       │   • Codecs (Gzip/Zstd/Snappy)│     │ • MongoDB (ObjectID Slices)  │     │ • Rate Limiter / Retries     │
       │   • Parsers (CSV, JSONL)     │     │ • Range Slicers / Pools      │     │ • APISink (Micro-batching)   │
       └──────────────┬───────────────┘     └──────────────┬───────────────┘     └──────────────┬───────────────┘
                      │                                    │                                    │
                      └────────────────────────────────────┼────────────────────────────────────┘
                                                           │ Conectado via SDFs
                                                           ▼
                                            ┌──────────────────────────────┐
                                            │      INTERNAL / BEAM / CORE  │
                                            │   • Dynamic Work Rebalancing │
                                            │   • CastAndValidateFn        │
                                            │   • AuditEnricherFn          │
                                            │   • DefaultDLQBeamSink       │
                                            └──────────────┬───────────────┘
                                                           │ Produz
                                                           ▼
                                            ┌──────────────────────────────┐
                                            │      DESTINOS & SINKS        │
                                            │   • ParquetBeamSink          │
                                            │   • DelimitedFileBeamSink    │
                                            │   • BatchAPIBeamSink         │
                                            │   • BigQuerySink (Write API) │
                                            └──────────────────────────────┘
```

---

## 2. Arquitetura de Ingestão: As 3 Famílias de Fontes (SDF)

Para garantir paralelismo massivo e divisão dinâmica de trabalho (*Dynamic Work-Stealing / Rebalancing*), todas as fontes de dados do Omnibeam estendem uma das **3 Famílias de Splittable DoFns (SDF)**.

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                 AS 3 FAMÍLIAS DE INGESTÃO (SDF)                                  │
├───────────────────────────────┬───────────────────────────────┬──────────────────────────────────┤
│ 1. Família ByteStream         │ 2. Família PartitionQuery     │ 3. Família PagedAPI              │
│    (Arquivos & Object Storage)│    (Bancos SQL & Documentais) │    (APIs REST, SaaS & Webhooks)  │
│                               │                               │                                  │
│ • Tracker: `ByteOffsetTracker`│ • Tracker: `PartitionTracker` │ • Tracker: `PageRangeTracker`    │
│ • Unidade: Byte Offsets [A, B)│ • Unidade: Fatias Discretas   │ • Unidade: Páginas ou Cursores   │
│ • Exemplos: CSV, JSONL no GCS │ • Exemplos: PostgreSQL, Mongo │ • Exemplos: Salesforce, REST API │
└───────────────────────────────┴───────────────────────────────┴──────────────────────────────────┘
```

---

### 📂 2.1. Família 1: ByteStream (`ByteStreamSourceSDF`)

Especializada em ler fluxos lineares de bytes (arquivos locais ou no Google Cloud Storage) dividindo arquivos gigantescos em blocos paralelos sem corromper registros nas bordas de chunk.

```
                        CADEIA DE PROCESSAMENTO EM STREAMING (DECORATOR PATTERN)

    ┌─────────────────┐
    │  Google Cloud   │
    │  Storage (GCS)  │  ==> [1. ports.StorageReader] (GCSStorage / LocalStorage com Range Read)
    └────────┬────────┘
             │ Stream Bruto (io.Reader)
             ▼
    ┌─────────────────┐
    │ [Descriptografia│  ==> [2. ports.StreamDecryptor] (Futuro: Decodificação Envelope KMS / PGP)
    └────────┬────────┘
             │ Stream Cifrado Decodificado
             ▼
    ┌─────────────────┐
    │  Decompressão   │  ==> [3. compression.WrapDecompressor] (Gzip, Zstd, Snappy, Bzip2)
    └────────┬────────┘
             │ Stream Descomprimido
             ▼
    ┌─────────────────┐
    │  Normalização   │  ==> [4. charsets.WrapNormalizer] (ISO-8859-1, Windows-1252 -> UTF-8)
    └────────┬────────┘
             │ Stream UTF-8 Canônico
             ▼
    ┌─────────────────┐
    │  Leitura Prévia │  ==> [5. ioutils.PrefetchReader] (Double-buffering assíncrono 64KiB)
    └────────┬────────┘
             │
             ▼
    ┌─────────────────┐
    │ Stream Decoder  │  ==> [6. ports.StreamDecoder] (Parsers: CSV, JSONL, Parquet, Avro, TXT)
    └────────┬────────┘
             │
             ▼
       GenericRecord (Domínio Omnibeam)
```

#### Mecânica de Alinhamento de Linhas (`ByteOffsetTracker`):
Quando múltiplos workers processam o mesmo arquivo CSV de 10 GB em paralelo:
1. O Worker 1 lê o intervalo `[0, 100MB)`.
2. O Worker 2 lê `[100MB, 200MB)`. Para não ler meia linha quebrada no início do seu bloco, o leitor descarta os bytes iniciais até encontrar a primeira quebra de linha `\n`.
3. O Worker 1 continua lendo além dos 100MB até consumir o final daquela linha, garantindo integridade perfeita sem duplicatas nem omissões.

#### Como Adicionar Novos Codecs, Formatos ou Descriptografia:
1. **Nova Compressão**: Adicione o wrapper em `internal/adapters/streams/codecs/compression/decompressor.go` implementando a interface padrão `io.Reader`.
2. **Novo Charset**: Adicione o conversor em `internal/adapters/streams/codecs/charsets/normalizer.go` utilizando `golang.org/x/text/encoding`.
3. **Novo Parser (ex: Avro ou Formato Posicional)**: Crie uma struct em `internal/adapters/streams/parsers/` implementando:
   ```go
   type StreamDecoder interface {
       Decode(ctx context.Context, r io.Reader, schema *domain.Schema, sourceFile string) (<-chan *domain.GenericRecord, <-chan error, error)
   }
   ```
   E registre no `init()` do pacote via Registry Pattern:
   ```go
   func init() {
       parsers.Register("avro", func(cfg *domain.SourceConfig) ports.StreamDecoder {
           return NewAvroParser(...)
       })
   }
   ```
4. **Criptografia / Decodificação de Chaves (GCP KMS / PGP)**: Implemente a interface `ports.StreamDecryptor` e encaixe na cadeia `WrapStream` do `PipelineBuilder`:
   $$\text{Storage Stream} \longrightarrow \text{StreamDecryptor (KMS/PGP)} \longrightarrow \text{Decompressor} \longrightarrow \text{Charset Normalizer} \longrightarrow \text{Decoder}$$

---

### 🗄️ 2.2. Família 2: PartitionQuery (`PartitionQuerySourceSDF`)

Especializada em extração paralela de bancos de dados relacionais e documentais (PostgreSQL, MySQL, MongoDB, Cloud Bigtable).

```
                            FATIAMENTO PARALELO DE BANCOS DE DADOS

    ┌─────────────────────────────────────────────────────────────────────────────────┐
    │                             TABELA / COLEÇÃO DE ORIGEM                          │
    │                              (Ex: 5.000.000 de Registros)                       │
    └────────┬─────────────────────────┬─────────────────────────┬────────────────────┘
             │                         │                         │
             ▼ [Fatia 1]               ▼ [Fatia 2]               ▼ [Fatia N]
    ┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
    │  ID: [1, 2500)  │       │ID: [2500, 5000) │       │ ID: [4.9M, 5M]  │
    └────────┬────────┘       └────────┬────────┘       └────────┬────────┘
             │                         │                         │
             ▼                         ▼                         ▼
    ┌─────────────────────────────────────────────────────────────────────────────────┐
    │                   Worker Connection Pool Cache (GetOrCreateDB)                  │
    │              (Reutiliza instâncias *sql.DB / mongo.Client por Worker)           │
    └─────────────────────────────────────────────────────────────────────────────────┘
```

#### Componentes Principais:
1. **Universal Range Slicer ([`internal/adapters/partitions/sql/range_slicer.go`](../internal/adapters/partitions/sql/range_slicer.go))**:
   - Descobre os limites (`MIN(id)`, `MAX(id)`, `COUNT(*)`) da tabela.
   - Gera fatias contíguas e balanceadas (`[]ports.PartitionSlice`). Para chaves UUID ou textos, aplica particionamento estatístico (`NTILE`).
2. **Worker Connection Pool Cache ([`internal/adapters/partitions/sql/connection_pool.go`](../internal/adapters/partitions/sql/connection_pool.go))**:
   - Mantém uma única instância de pool `*sql.DB` por processo do worker, evitando exaustão de conexões TCP e portas no banco durante execuções no Dataflow.
3. **PartitionRangeTracker**:
   - Reivindica fatias discretas e suporta divisão dinâmica caso um worker finalize suas fatias mais rápido que os outros.

#### Como Adicionar um Novo Banco de Dados (ex: Oracle, SQL Server, Cassandra, Bigtable):
1. Crie o pacote em `internal/adapters/partitions/<novo_banco>/`.
2. Implemente o contrato [`ports.PartitionedReader`](../internal/ports/partition.go):
   ```go
   type PartitionedReader interface {
       CalculatePartitions(ctx context.Context, cfg *domain.PipelineConfig) ([]ports.PartitionSlice, error)
       ReadPartition(ctx context.Context, slice ports.PartitionSlice, emit func(*domain.GenericRecord) error) error
   }
   ```
3. Registre o conector no `init()`:
   ```go
   func init() {
       ports.RegisterSource("bigtable", bigtableReaderFactory)
   }
   ```

---

### 🌐 2.3. Família 3: PagedAPI (`PagedAPISourceSDF`)

Especializada em coleta de dados de APIs REST, Webhooks, GraphQL e plataformas SaaS externas (Salesforce, SAP, Stripe, HubSpot).

```
                             INGESTÃO RESILIENTE DE APIS REST

    ┌─────────────────────────────────────────────────────────────────────────────────┐
    │                                 PageRangeTracker                                │
    │            (Controla o intervalo de páginas/cursores a serem processados)       │
    └────────────────────────────────────────┬────────────────────────────────────────┘
                                             │
                                             ▼
    ┌─────────────────────────────────────────────────────────────────────────────────┐
    │                       RESTReader (Adapter HTTP Inteligente)                     │
    │  ┌───────────────────────────────────────────────────────────────────────────┐  │
    │  │ 1. Estratégias de Paginação: PageNumber, OffsetLimit, CursorToken, Link   │  │
    │  │ 2. Rate Limiting: Token-Bucket ativo (RPS configurável por endpoint)      │  │
    │  │ 3. Resiliência: Retry com Exponential Backoff + Jitter                    │  │
    │  │ 4. Proteção: Circuit Breaker contra saturação de serviços downstream      │  │
    │  └───────────────────────────────────────────────────────────────────────────┘  │
    └────────────────────────────────────────┬────────────────────────────────────────┘
                                             │
                                             ▼
                             GenericRecord (Domínio Omnibeam)
```

---

## 3. Arquitetura de Exportação: Criando Novos Sinks

A exportação de dados no Omnibeam é **orientada a contratos** e **isolada de falhas**. Se um registro falhar ao ser enviado para o destino final, ele é automaticamente direcionado para o **Dead Letter Queue (DLQ)** com metadados de auditoria completos.

```
                         FLUXO DE PROCESSAMENTO E GRAVAÇÃO NO SINK

                                  PCollection<GenericRecord> (Válidos)
                                                  │
                                                  ▼
                                       ┌──────────────────────┐
                                       │ ports.BeamSinkBuilder│
                                       └──────────┬───────────┘
                                                  │
                 ┌────────────────────────────────┴────────────────────────────────┐
                 │                                                                 │
                 ▼ (Sinks Baseados em Arquivo)                                     ▼ (Sinks de Rede / APIs)
      ┌──────────────────────┐                                          ┌──────────────────────┐
      │   BundleFileStager   │                                          │  ports.BatchAPIWriter│
      │  • Lazy Open         │                                          │  • Micro-lotes (50)  │
      │  • Checksum SHA-256  │                                          │  • Rate Limiting     │
      │  • Atomic Commit     │                                          │  • Vault/GCP Auth    │
      └──────────┬───────────┘                                          └──────────┬───────────┘
                 │                                                                 │
                 ▼                                                                 ▼
      Google Cloud Storage /                                            Endpoint HTTP / Webhook
      Parquet Colunar                                                   (Falhas -> Roteadas para DLQ)
```

---

### 🛠️ Guia Prático: Como Desenvolver um Novo Sink (Ex: Google BigQuery Sink)

Para implementar a gravação direta no **Google BigQuery** via *Storage Write API*, o desenvolvedor segue **4 passos simples**:

#### Passo 1: Definir o Modelo de Configuração no Domínio ([`internal/domain`](../internal/domain))
Adicione as opções de configuração do BigQuery no manifesto:
```go
type BigQueryDestinationConfig struct {
    ProjectID string `json:"project_id"`
    DatasetID string `json:"dataset_id"`
    TableID   string `json:"table_id"`
    WriteMode string `json:"write_mode"` // "APPEND", "TRUNCATE"
}
```

#### Passo 2: Implementar o Adaptador de Escrita (`internal/adapters/sinks/bigquery/writer.go`)
Cria a comunicação de baixo nível com o cliente gRPC do BigQuery:
```go
package bigquery

import (
    "context"
    "github.com/omnibeam/dataflow-compute-go/internal/domain"
)

type BigQueryWriter struct {
    cfg domain.BigQueryDestinationConfig
}

func (w *BigQueryWriter) WriteBatch(ctx context.Context, records []*domain.GenericRecord, schema *domain.Schema) error {
    // 1. Converte GenericRecords para Protobuf / JSON Rows do BigQuery
    // 2. Executa AppendRows via Storage Write API com semântica Exactly-Once
    return nil
}
```

#### Passo 3: Criar a Transformação DoFn do Apache Beam (`internal/beam/sinks/bigquery_sink.go`)
Implementa a interface [`ports.BeamSinkBuilder`](../internal/ports/sink_builder.go):
```go
package sinks

import (
    "github.com/apache/beam/sdks/v2/go/pkg/beam"
    "github.com/omnibeam/dataflow-compute-go/internal/domain"
    "github.com/omnibeam/dataflow-compute-go/internal/ports"
)

type BigQueryBeamSink struct {
    Config domain.BigQueryDestinationConfig
    Schema domain.Schema
}

var _ ports.BeamSinkBuilder = (*BigQueryBeamSink)(nil)

func (s *BigQueryBeamSink) BuildSink(scope beam.Scope, validRecords beam.PCollection) {
    dofn := NewBigQuerySinkDoFn(s.Config, s.Schema)
    beam.ParDo0(scope, dofn, validRecords)
}
```

#### Passo 4: Registrar no Catálogo Global de Sinks
```go
func init() {
    ports.RegisterSink("bigquery", bigQuerySinkFactory)
}
```

---

## 4. Evolução Arquitetural Implementada

Sob os preceitos de Clean Architecture, manutenibilidade e escalabilidade futura, as seguintes melhorias foram implementadas e consolidadas no Omnibeam:

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│                               STATUS DAS MELHORIAS ARQUITETURAIS                                 │
├───────────────────────────────┬───────────────────────────────┬──────────────────────────────────┤
│ Área / Ponto de Melhoria      │ Situação Anterior             │ Status Atual (Implementado)      │
├───────────────────────────────┼───────────────────────────────┼──────────────────────────────────┤
│ 1. Pipeline de Descriptografia│ `WrapStream` tratava apenas   │ ✅ Interface declarativa         │
│    no I/O (`StreamDecryptor`) │ compressão e charsets.        │ `ports.StreamDecryptor` integrada│
│                               │                               │ ao `PipelineBuilder.WrapStream`. │
├───────────────────────────────┼───────────────────────────────┼──────────────────────────────────┤
│ 2. Unificação de Sinks & OCP  │ `cmd/pipeline/registry.go`    │ ✅ Sinks registrados em `init()` │
│    no Composition Root        │ continha switches hardcoded.  │ e resolvidos via `BuildSink()`.  │
├───────────────────────────────┼───────────────────────────────┼──────────────────────────────────┤
│ 3. Registries de Parsers &    │ `switch case "csv", "jsonl"`  │ ✅ `parsers.Register()` e        │
│    Formatters                 │ dispersos em múltiplos pontos.│ `formatters.RegisterFormatter()` │
│                               │                               │ 100% dinâmicos e desacoplados.   │
├───────────────────────────────┼───────────────────────────────┼──────────────────────────────────┤
│ 4. Normalização no Domínio    │ `if targetType == ""` no DAG. │ ✅ `ApplyDefaults()` modular     │
│    (DRY & KISS)               │                               │ e centralizado no `domain`.      │
└───────────────────────────────┴───────────────────────────────┴──────────────────────────────────┘
```
