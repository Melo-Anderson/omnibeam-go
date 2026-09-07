# Arquitetura Colunar (RecordBatch) & Fusão de Transforms (Macro-Step Fusion)
## Análise Profunda de Impacto Arquitetural nas 3 Famílias de Ingestão do OmniBeam

**Foco Técnico:** Avaliação de viabilidade, trade-offs, pegada de memória e migração de `PCollection[*GenericRecord]` (AoS) para `PCollection[*RecordBatch]` (SoA) e fusão de estágios de validação/auditoria.  
**Governança:** Clean Architecture | SOLID | Apache Beam Go SDK v2 | High-Performance Go

---

## 1. Visão Geral e Comparativo Conceitual

Atualmente, o **OmniBeam** opera primariamente sob o modelo **Array-of-Structures (AoS)**: cada registro é representado por uma instância individual de `*domain.GenericRecord`, contendo um slice de `Values []FieldValue` e um mapa de `AuditFields map[string]string`.

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
│                           AOS: MODELO ATUAL (Array-of-Structures)                               │
├─────────────────────────────────────────────────────────────────────────────────────────────────┤
│  PCollection[*domain.GenericRecord] (10.000.000 de registros)                                   │
│  ├── Record 1 ──► Heap Alloc: [GenericRecord Struct] + [Values Slice] + [AuditFields Map]       │
│  ├── Record 2 ──► Heap Alloc: [GenericRecord Struct] + [Values Slice] + [AuditFields Map]       │
│  └── ...                                                                                        │
│  • Total de Ponteiros no Heap: ~30.000.000 objetos monitorados pelo GC                          │
│  • Localidade de Cache: Dispersa (saltos de ponteiros para cada campo e registro)               │
└─────────────────────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
│                       SOA: MODELO PROPOSTO (Structure-of-Arrays / RecordBatch)                  │
├─────────────────────────────────────────────────────────────────────────────────────────────────┤
│  PCollection[*domain.RecordBatch] (1.000 lotes de 10.000 registros)                             │
│  └── Batch 1:                                                                                   │
│      ├── Int64Cols:    [int64, int64, int64, ...]  ──► Bloco Contíguo de Memória (MemCopy/SIMD) │
│      ├── Float64Cols:  [float64, float64, ...]     ──► Bloco Contíguo de Memória                │
│      ├── StringCols:   [string, string, string...] ──► Slice Contíguo                           │
│      ├── NullBitmaps:  [uint64, uint64, uint64...] ──► 1 bit por campo                          │
│      └── AuditFields:  map[string][]string         ──► 1 slice por chave de metadado            │
│  • Total de Ponteiros no Heap: ~1.000 objetos (Redução de 99.9% de pressão no GC)               │
│  • Localidade de Cache: Máxima (acesso sequencial L1/L2 vetorizável via CPU)                    │
└─────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Impacto Arquitetural na Família 1 — Arquivos & Storage (Streams)

A família de streams lê arquivos brutos (CSV, JSONL, Parquet) locais ou no Google Cloud Storage (`gs://`) via `ByteStreamSourceSDF`.

### 2.1. Como os Parsers Operam no Modelo Colunar
* **No modelo atual (AoS):** O parser lê linha por linha, aloca `domain.NewGenericRecord`, faz parsing individual de cada coluna e emite na PCollection.
* **No modelo colunar (SoA):**
  - O DoFn aloca um `RecordBatch` pré-dimensionado (e.g., capacidade de 10.000 linhas via `sync.Pool`).
  - Durante o loop de leitura do stream, os parsers (`CSVParser`, `JSONLParser`) populam diretamente os vetores colunares contíguos (`batch.AppendInt64(colIdx, val)`).
  - Quando o batch atinge a capacidade de 10.000 linhas ou o fim da fatia de bytes (EOF da restrição SDF), o `RecordBatch` completo é emitido de uma só vez.

### 2.2. Interação com o Apache Parquet Sink e BigQuery Sink
* **Ganho Estratégico Máximo:** O formato Apache Parquet e a BigQuery Storage Write API já são nativamente colunares.
* Em vez de o `ParquetSinkDoFn` desempacotar `GenericRecord` linha a linha para montar os blocos Parquet, ele transfere diretamente as colunas de memória do `RecordBatch` para o escritor colunar (*Zero-Copy Columnar Pass-Through*).
* **Vazão esperada:** Aumento de **3x a 5x** na velocidade de escrita de arquivos Parquet e tabelas BigQuery.

---

## 3. Impacto Arquitetural na Família 2 — Partições SQL & NoSQL

A família de partições particiona tabelas e coleções relacionais/documentais (PostgreSQL, MySQL, MongoDB) via `PartitionQuerySourceSDF`.

### 3.1. Escaneamento Colunar de Tuplas (`database/sql` e `mongo.Cursor`)
* **No modelo atual (AoS):** Para cada linha retornada por `rows.Next()`, o leitor aloca um slice de interfaces `dest := make([]any, n)` e chama `rows.Scan(dest...)`.
* **No modelo colunar (SoA):**
  - O leitor SQL/NoSQL aloca vetores contíguos para o lote.
  - Para 1.000 linhas, ele reutiliza os ponteiros dos vetores colunares para o `rows.Scan`, eliminando o overhead de alocação de structs de domínio por tupla SQL.

### 3.2. Throughput de Particionamento Paralelo
* Workers lendo partições em paralelo passam a gerar lotes compactos. O tráfego de dados na rede interna do Dataflow (shuffle entre workers e sinks) é drasticamente reduzido porque os coders serializam blocos contíguos de bytes sem metadados redundantes por linha.

---

## 4. Impacto Arquitetural na Família 3 — APIs REST & Webhooks

A família de APIs consome endpoints paginados via `PagedAPISourceSDF`.

### 4.1. Projeção Direta de Arrays JSON
* **No modelo atual (AoS):** Respostas HTTP contendo arrays de 500 objetos JSON são decodificadas individualmente e emitidas como 500 elementos de PCollection.
* **No modelo colunar (SoA):**
  - A resposta JSON de uma página inteira é convertida diretamente em um único `RecordBatch` de 500 linhas.
  - Como a página já vem em lote da API, o processamento respeita a granularidade natural da fonte externa sem fragmentação.

---

## 5. Roteamento de Erros e Dead Letter Queue (DLQ) no Modelo Colunar

Um dos principais desafios de migrar para `RecordBatch` é como segregar linhas inválidas sem invalidar o restante do lote e mantendo a **Invariante de Conservação Estrita**:

$$\text{TotalRecordsRead} = \text{RowsWritten} + \text{DeadLetterCount}$$

### Estratégia de Segregação Colunar de Falhas:
1. **Bitmask de Validade no Batch:** Cada `RecordBatch` possui um bitmap interno indicando quais índices de linha são válidos (`validMask`).
2. **Emissão Eficiente de DLQ:**
   - Durante a validação de tipos (`CastAndValidate`), se a linha de índice $i$ contiver uma falha de coerção (ex: string não conversível para decimal), o validador marca o bit $i$ como inválido no batch e extrai **apenas essa linha específica** para gerar o `DeadLetterRecord` individual (ou `DeadLetterBatch`).
   - O `RecordBatch` original segue para o sink contendo apenas as linhas válidas ativas.
   - Sinks e formatadores iteram apenas sobre os índices marcados como válidos no bitmap.

---

## 6. Impacto no Dynamic Work Rebalancing (Splittable DoFns)

* No Apache Beam, o Dataflow divide o trabalho distribuído no nível de elementos de PCollection e restrições de Splittable DoFns.
* **Tamanho de Lote Ideal:** Para evitar perda de granularidade no rebalanceamento dinâmico entre workers, os `RecordBatches` devem ter tamanho balanceado (entre **1.000 e 5.000 linhas**).
* Lotes dessa magnitude garantem que o ganho de memória no Heap seja de $>95\%$ sem impedir que o Dataflow redistribua fatias de arquivos ou partições residuais para workers ociosos.

---

## 7. Impacto da Fusão de Transforms (Macro-Step Fusion — 1.3)

### 7.1. Otimização de Grafo no Beam
Atualmente o pipeline executa:
$$\text{RawRecords} \xrightarrow{\text{ParDo}} \text{AuditEnricherFn} \xrightarrow{\text{PCollection[Enriched]}} \text{CastAndValidateFn} \xrightarrow{\text{ParDo3}} (\text{Valid}, \text{DLQ}, \text{Audit})$$

* **Com Fusão por Composição:**
  $$\text{RawRecords} \xrightarrow{\text{ParDo3(FusedEnrichAndValidateFn)}} (\text{Valid}, \text{DLQ}, \text{Audit})$$
* **Benefícios Diretos:**
  1. Eliminação total da PCollection intermediária `Enriched`.
  2. Zero alocação de cópia intermediária para enriquecimento de auditoria.
  3. O registro é enriquecido e validado no mesmo ciclo de CPU e cache L1.
  4. Redução de **15% a 20%** no tempo total de pipeline em DirectRunner e Cloud Dataflow.

---

## 8. Plano de Migração e Padrão de Coexistência Híbrida

Para garantir **Zero Breaking Changes** e permitir adoção progressiva:

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
│                           PADRÃO DE COEXISTÊNCIA HÍBRIDA (Zero-Copy)                            │
├─────────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                                 │
│  [ RecordBatch (SoA) ] ──► Consumo Direto de Alta Performance por Parquet e BigQuery Sinks      │
│            │                                                                                    │
│            ▼                                                                                    │
│  [ RecordBatch.Iterator() ] ──► Zero-Copy Row View (GenericRecord compatível)                   │
│            │                                                                                    │
│            ▼                                                                                    │
│  [ Custom DoFns / Legacy Adapters ] (Consomem linha a linha sem precisar reescrever lógica)     │
│                                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────────────────────┘
```

1. **Fase 1 (Atual):** Otimizações transparentes de motor (Zero-Allocation Coders, Liquid Sharding, Adaptive Sizing, Double-Buffering Prefetch).
2. **Fase 2 (Próxima Evolução):** Introdução de `domain.RecordBatch` com `RecordBatch.Iterator()` zero-copy, permitindo que sinks colunares (Parquet e BigQuery) consumam lotes nativos enquanto DoFns legados continuam operando normalmente.
