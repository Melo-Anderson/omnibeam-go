# Apache Beam Go SDK — Architectural Practices & Production Hardening

Este documento estabelece as diretrizes normativas, padrões arquiteturais e práticas de robustez para o desenvolvimento com o **Apache Beam Go SDK v2** no projeto **OmniBeam**, garantindo execução segura tanto no `DirectRunner` local quanto no ambiente distribuído do **Google Cloud Dataflow**.

---

## 1. Topologia de Execução (Submitter vs Worker)

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                 FLUXO DE EXECUÇÃO DISTRIBUÍDA                                    │
│                                                                                                  │
│  [ Submitter / Driver ]                                                                          │
│  1. Inicia via main()                                                                            │
│  2. beam.Init() verifica: --worker=false                                                         │
│  3. Executa discovery leve O(1) (ex: CalculatePartitions)                                        │
│  4. Fecha conexões do discovery (defer db.Close())                                               │
│  5. Monta DAG com Reader: nil e configs serializáveis                                            │
│  6. Despacha pipeline via beamx.Run(ctx, p)                                                      │
│                                                                                                  │
│                                           │ DAG Serializado (RPC)                                │
│                                           ▼                                                      │
│  [ Dataflow Worker Harness ]                                                                     │
│  1. Inicia o mesmo binário com --worker=true                                                     │
│  2. beam.Init() intercepta e ABORTA qualquer execução de main()                                  │
│  3. Dispara callbacks registrados em beam.RegisterInit()                                         │
│  4. Desserializa structs dos DoFns (apenas campos PascalCase)                                   │
│  5. Ciclo de Vida: DoFn.Setup(ctx) ──► StartBundle(ctx) ──► ProcessElement ──► FinishBundle     │
│  6. Teardown() ao finalizar o worker                                                             │
└──────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Invariantes de Registro e Runtime

| Operação | Onde Executar | Invariante do Beam SDK |
|---|---|---|
| `beam.RegisterInit` | `func init()` (nível de pacote) | **SDK Ref**: `beam/core/runtime/init.go`<br>Chamar após `beam.Init()` causa `panic` fatal imediato. |
| `beam.RegisterType` / `RegisterCoder` | `func init()` | Tipos trafegados em `PCollection` devem ser registrados antes da criação da DAG. |
| `beam.RegisterDoFn` | `func init()` | Obrigatório para DoFns complexos ou que utilizem Splittable DoFn (SDF). |
| `Set*Provider` (Factories) | Dentro do hook `RegisterInit` | Provedores globais de I/O devem ser registrados via hook para que existam nos workers remotos. |
| Métricas (`metrics.NewCounter`, etc.) | Escopo de Pacote (`var`) | **Nunca** declarar métricas como campos da struct do DoFn (a serialização do Beam zera os handles). |

---

## 3. Serialização de DoFns (Exportado vs Não-Exportado)

O Apache Beam Go SDK utiliza reflexão e coders estruturais para serializar os DoFns do nó submitter para os workers:

| Visibilidade do Campo | Serializado pelo Beam | Conteúdo Permitido | Comportamento no Worker |
|---|---|---|---|
| **Exportado (`PascalCase`)** | **SIM** (JSON/Coders) | Configurações puras e serializáveis (`Endpoint`, `Schema`, `APIOptions`, `DLQPath`, `SecretToken`). | Preservado com os mesmos valores do submitter. |
| **Não-Exportado (`camelCase`)** | **NÃO** | Sockets, conexões ativas (`*sql.DB`, `*mongo.Client`), storage writers, buffers e pipes. | Inicializado como `nil` / zero-value. |

> [!CAUTION]
> **Regra de Ouro**: Conexões de rede, pools de banco de dados ou closures `func(...)` **nunca** devem ser campos exportados. Devem ser reconstruídos de forma determinística dentro de `Setup(ctx context.Context) error`.

```go
// Exemplo canônico de DoFn serializável e seguro:
type APISinkDoFn struct {
    // 1. Configurações exportadas (serializadas pelo Beam)
    Endpoint    domain.APIEndpointConfig `json:"endpoint"`
    APIOptions  domain.APISinkOptions    `json:"api_options"`
    Schema      domain.Schema            `json:"schema"`
    DLQPath     string                   `json:"dlq_path"`
    SecretToken string                   `json:"secret_token,omitempty"`

    // 2. Estado local do worker (unexported, reconstruído no Setup)
    writer  ports.BatchAPIWriter    `json:"-"`
    dlqSink ports.StorageWriter     `json:"-"`
    buffer  []*domain.GenericRecord `json:"-"`
}

func (fn *APISinkDoFn) Setup(ctx context.Context) error {
    if fn.writer == nil && globalAPIWriterFactory != nil {
        fn.writer = globalAPIWriterFactory(fn.Endpoint, fn.APIOptions, fn.SecretToken)
    }
    return nil
}
```

---

## 4. Ciclo de Vida do DoFn e Assinaturas Canônicas

Validadas por reflexão em `beam/core/graph/fn.go` (máximo de 1 parâmetro `context.Context` e 1 retorno `error`):

| Método | Assinatura Canônica | Responsabilidade |
|---|---|---|
| **`Setup`** | `Setup(ctx context.Context) error` | Inicializa pools de conexão, clientes HTTP e storage writers no processo worker. |
| **`StartBundle`** | `StartBundle(ctx context.Context) error` | Inicializa buffers de lote e zera contadores do bundle ativo. |
| **`ProcessElement`** | `ProcessElement(ctx, elem, emitValid, emitDLQ, ...) error` | Loop quente $O(1)$. Proibido abrir conexões aqui. |
| **`FinishBundle`** | `FinishBundle(ctx context.Context) error` | Realiza flush de buffers, fecha streams, comita arquivos temporários. |
| **`Teardown`** | `Teardown() error` | Fecha sockets e libera pools no encerramento do worker. |

---

## 5. Padrões das 3 Famílias de Ingestão (Splittable DoFns)

O OmniBeam padroniza a ingestão de dados em **3 Famílias Canônicas de SDF**, garantindo métricas precisas de backlog (`RestrictionSize`) para o **Dataflow Dynamic Work Rebalancing & Autoscaling**:

### Família 1 — Byte Streams (Arquivos e Object Storage)
- **Tracker**: `ByteOffsetTracker` operando sobre intervalos `[Start, End)` de bytes.
- **Tamanho Inicial**: Consulta $O(1)$ de metadados (`StorageReader.Size`), evitando download antecipado.
- **Compressão**: Arquivos comprimidos (`gzip`, `zstd`, `snappy`) **não suportam seek arbitrário**. Devem retornar uma **restrição única indivisível** no `SplitRestriction`.
- **Segurança de Pipes**: O feeder de stream deve fechar o pipe leitor com `defer pr.CloseWithError(ctx.Err())` e monitorar `select { case <-ctx.Done(): ... }` para evitar vazamento de goroutines zumbis.

### Família 2 — Partições de Banco de Dados (SQL & NoSQL)
- **Tracker**: `PartitionRangeTracker` operando sobre fatias analíticas balanceadas (`[]PartitionSlice`).
- **Range Slicing**: Construção de janelas contíguas via `NTILE`/`ROW_NUMBER()` no banco de dados.
- **Boundary Aberto à Direita**: O último slice **deve sempre usar `WHERE col >= $1`** (sem limite superior). Isso evita perda silenciosa de linhas inseridas concorrentemente durante o pipeline.

### Família 3 — Paginação de API REST / HTTP
- **Tracker**: `PageRangeTracker` operando sobre páginas calculadas ou descobertas dinamicamente.
- **Resiliência HTTP**: Clientes HTTP isolados com rate limiting via Token Bucket, backoff exponencial com jitter e respeito a cabeçalhos `Retry-After`.

---

## 6. Imutabilidade em PCollections e Roteamento Multicanal

1. **Imutabilidade Absoluta**:
   - Elementos em `PCollection` são compartilhados entre múltiplos branches e workers.
   - **Proibida a mutação direta de mapas ou ponteiros compartilhados**.
   - Transformações que enriquecem dados (ex: `AuditEnricherFn`) **devem criar um clone superficial** e alocar um novo mapa isolado antes de emitir:
     ```go
     enriched := &domain.GenericRecord{
         SchemaID:    rec.SchemaID,
         Values:      rec.Values,
         AuditFields: make(map[string]string, len(rec.AuditFields)+1),
     }
     for k, v := range rec.AuditFields {
         enriched.AuditFields[k] = v
     }
     enriched.AuditFields["_ingested_at"] = time.Now().UTC().Format(time.RFC3339)
     emit(enriched)
     ```

2. **Roteamento Multicanal via `beam.ParDo3`**:
   - `CastAndValidateFn` avalia cada registro e roteia para 3 streams desacopladas:
     - `Valid` $\rightarrow$ Destino final.
     - `DLQ` $\rightarrow$ Quarentena isolada com payload bruto e causa do erro.
     - `Audit` $\rightarrow$ Histórico de auditoria e qualidade.
   - **Invariante de Conservação Estrita**:
     $$\text{TotalRecordsRead} = \text{RowsWritten} + \text{DeadLetterCount}$$

---

## 7. Padrão Lazy-Open em Sinks de Arquivo

Para evitar a sobrecarga de operações de metadados no Google Cloud Storage (GCS) gerada por bundles ociosos:
- **Não abrir arquivos temporários no `StartBundle()`**.
- Inicializar `storage.CreateTemp()` apenas no primeiro `ProcessElement()` via helper `lazyOpen(ctx)`.
- Se `fn.recordCount == 0`, o `FinishBundle()` retorna `nil` imediatamente sem chamar APIs de storage.

---

## 8. Arquitetura de Plugins e Auto-Registro OCP

Novos conectores (ex: Cassandra, Kafka, BigQuery, ClickHouse) devem ser desenvolvidos seguindo o **Self-Registration Pattern**:

1. Implementar o adaptador em `internal/adapters/{família}/{conector}` aderente aos contratos em `internal/ports`.
2. Registrar o driver no `init()` do pacote:
   ```go
   func init() {
       ports.RegisterSource("cassandra", func(ctx context.Context, cfg *domain.PipelineConfig, sec ports.SecretResolver) (ports.PartitionedReader, error) {
           return NewCassandraReader(cfg.DatabaseSource), nil
       })
   }
   ```
3. Declarar o blank import em `cmd/pipeline/imports.go`:
   ```go
   import _ "github.com/omnibeam/dataflow-compute-go/internal/adapters/partitions/cassandra"
   ```
4. **Zero alterações no código central da DAG ou em switches de controle**.

---

## 9. Estratégia de Testes para Apache Beam Go

1. **Harness em Memória (`ptest` e `passert`)**:
   - Todo arquivo de teste de pipeline que utilize `ptest.Run(p)` deve inicializar o harness no `TestMain`:
     ```go
     func TestMain(m *testing.M) {
         ptest.Main(m)
     }
     ```
   - Utilizar asserções de PCollection (`passert.Count`, `passert.Equals`) para validação determinística de contagens e bifurcações multicanal.

2. **Teste de Sanidade de Serialização do DoFn**:
   - Todo DoFn deve possuir um teste unitário que simula a serialização e desserialização via JSON/Coder, executando `Setup()` e `StartBundle()` na instância desserializada para assegurar que nenhum campo obrigatório vire `nil`.

---

---

## 11. Custom Binary Coders de Alta Performance (`beam.RegisterCoder`)

Por padrão, o Beam Go SDK utiliza serialização baseada em reflexão ou fallback JSON quando structs trafegam entre transformações e workers remotos. Em pipelines de alta vazão (milhões de registros), isso gera sobrecarga excessiva de CPU e alocações no Heap.

### Diretrizes para Implementação de Coders:
1. **Zero-Reflection**: Utilizar `binary.PutVarint` / `binary.Varint` para inteiros e bitmasks para estados booleanos e nulos.
2. **Header de Metadados Compacto**: 1 byte para identificar quais campos do registro são nulos (bitmask).
3. **Registro Obrigatório no `init()`**:
   ```go
   func init() {
       beam.RegisterCoder(reflect.TypeOf((*domain.GenericRecord)(nil)).Elem(), EncodeGenericRecord, DecodeGenericRecord)
   }
   ```
4. **Invariante de Round-Trip**: O coder deve garantir estritamente que $\text{Decode}(\text{Encode}(x)) == x$ para todos os tipos primitivos e estruturas aninhadas.

---

## 12. Estimativa de Backlog e Progresso em Splittable DoFns (SDF)

Para que o **Autoscaler do Google Cloud Dataflow** possa tomar decisões precisas sobre aumentar ou diminuir o número de workers:

1. **Implementar `GetProgress()` no `RestrictionTracker`**: Retornar a fração float64 entre `0.0` (início) e `1.0` (concluído).
2. **Implementar `GetSize()` ou `RestrictionSize()`**: Retornar a estimativa quantitativa de trabalho restante (em bytes para `ByteStream` ou em contagem de fatias para `PartitionQuery` / `PagedAPI`).
3. **Thread-Safety**: Como o runtime do Dataflow chama `GetProgress()` concorrentemente enquanto `TryClaim()` está executando na thread do DoFn, o tracker deve proteger as variáveis de estado com mutex ou operações atômicas (`sync.Mutex`).

---

## 13. Combiner Lifting e Pré-Agregação Local de Métricas

Para evitar o congestionamento de rede no Shuffle do Beam ao agregar métricas e contadores de milhões de registros:

1. **Utilizar `beam.Combine` / `beam.CombinePerKey`** com implementações de `CombineFn`:
   - `AddInput(accum, input)`: Executa no worker local durante o `ProcessElement`.
   - `MergeAccumulators(accum1, accum2)`: Executa tanto no worker local quanto no nó agregador.
   - `ExtractOutput(accum)`: Produz a saída final já sumarizada.
2. **Combiner Lifting**: O runner do Dataflow identifica a comutatividade e associatividade do `CombineFn` e executa a soma parcial localmente antes de despachar bytes pela rede.

---

## 14. Checklist de Conformidade para Novos DoFns

- [ ] Todos os campos de configuração da struct são **exportados** (`PascalCase`).
- [ ] Todos os sockets, clientes, storage writers e buffers são **não-exportados** (`camelCase`).
- [ ] A struct possui método `Setup(ctx context.Context) error` que reconstrói os recursos efêmeros.
- [ ] Se o DoFn abrir conexões fecháveis, implementa `Teardown() error`.
- [ ] Métricas (`metrics.NewCounter`, `metrics.NewDistribution`) estão declaradas no escopo do pacote, fora da struct.
- [ ] Em DoFns que geram arquivos, a criação do arquivo temporário usa o padrão **Lazy-Open** via `BundleFileStager`.
- [ ] Tipos trafegados em `PCollection` possuem **Custom Binary Coders** registrados quando representam o volume principal de dados.
- [ ] Splittable DoFns implementam estimativa de progresso e backlog para suporte a Autoscaling.
- [ ] DoFns que alteram metadados realizam **cópia profunda ou clone de mapas** para respeitar a imutabilidade.
- [ ] O DoFn possui asserção estática de interface em tempo de compilação:
  ```go
  var _ interface {
      Setup(context.Context) error
      StartBundle(context.Context) error
      ProcessElement(context.Context, *domain.GenericRecord) error
      FinishBundle(context.Context) error
  } = (*MySinkDoFn)(nil)
  ```

---

## 15. Zero-Allocation Custom Binary Coders & Stack Buffer Reuse

Para pipelines que processam centenas de milhões de registros, os serializadores/coders binários de `PCollection` não devem realizar alocações dinâmicas no Heap (`make([]byte, ...)`) por registro:

1. **Stack Allocation para Schemas Típicos ($\le 128$ colunas):**
   - Utilizar buffers alocados na stack (`var stackMask [16]byte`, `var typeBuf [1]byte`, `var numBuf [8]byte`) para codificar bitmasks de nulabilidade e descritores de tipo.
2. **Buffer Pooling com `sync.Pool` para Schemas Grandes ($> 128$ colunas):**
   - Utilizar `sync.Pool` para alocar e reaproveitar slices de bytes em tabelas extremamente largas, liberando buffers ao final da serialização.
3. **Preservação de Compatibilidade Binária:**
   - Nunca alterar tipos primitivos (como `writeVarint` / `readVarint`) sem migração formal versionada de coder.

---

## 16. Sub-Megabyte Dynamic Work Rebalancing (Liquid Sharding) em SDFs

Para garantir que o Autoscaler do Google Cloud Dataflow distribua o trabalho residual eficientemente sem produzir workers atrasados (*stragglers*):

1. **Resolução de Split Mínima (`MinSplitChunkSize = 64KB`):**
   - O `TrySplit(fraction)` deve aceitar divisões com frações de sub-megabytes, permitindo que workers ociosos assumam o final de fatias de arquivos ou partições de banco de dados.
2. **Cálculo de Progresso Normalizado em `GetProgress()`:**
   - Retornar o par `(done, remaining float64)` baseado na posição exata do cursor (`claimed`), permitindo estimativas de backlog em tempo real para o Dataflow.

---

## 17. Dynamic Adaptive Bundle Sizing em Sinks Analíticos

Para maximizar a vazão de rede (MB/s) em conectores de egress de alta velocidade (BigQuery Storage Write API, Apache Parquet, APIs SaaS):

1. **Ajuste Dinâmico de Lote (`AdaptiveBatchSizer`):**
   - Monitorar a latência das operações de flush de lote. Se a latência for baixa ($< 200\text{ms}$), o DoFn expande progressivamente o lote até 10.000 registros. Se a latência for alta ($> 1.5\text{s}$), o lote é contraído.
2. **Salvaguarda de Memória (`runtime.ReadMemStats`):**
   - Se o uso de memória do Heap atingir limites de segurança ($> 1\text{GB}$ em workers padrão), o sizer reduz imediatamente o tamanho do lote para o valor mínimo configurado, prevenindo OOM (*Out Of Memory*).


