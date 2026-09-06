# Omnibeam Dataflow Compute Engine — Features & Roadmap de Evolução

**Foco:** Computação Distribuída Batch & Micro-Batch de Alta Performance (Google Cloud Dataflow & Ambiente Local)  
**Governança Arquitetural:** Clean Architecture | SOLID | DDD | TDD | High-Performance Go  
**Maturidade Técnica:** 9.6/10 (Padrão World-Class Atingido)

---

## 1. Visão Geral e Posicionamento

O **Omnibeam Dataflow Compute Engine** é uma plataforma corporativa de movimentação e computação de dados em larga escala, construída em Go e desenhada nativamente para o ecossistema do **Google Cloud Dataflow / Apache Beam Go SDK v2**, com suporte completo a execução e testes locais.

A plataforma é especializada no paradigma de **Batch e Micro-Batch de Alta Performance**: execução sob demanda via **Dataflow Flex Templates** disparados por agendadores corporativos (Cloud Scheduler, Cloud Workflows, Apache Airflow), alocando e desalocando recursos em nuvem dinamicamente para garantir custo computacional otimizado (FinOps) e zero consumo ocioso de infraestrutura.

```
┌───────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                   ARQUITETURA FUNCIONAL OMNIBEAM                                  │
├───────────────────────────────┬───────────────────────────────┬───────────────────────────────────┤
│       1. Fontes de Entrada    │    2. Motor de Processamento  │       3. Destinos de Saída        │
│  • Arquivos (Local & GCS)     │  • Fatiamento Distribuído SDF │  • Google BigQuery (Write API)    │
│  • Bancos Relacionais (SQL)   │  • Enriquecimento & Auditoria │  • Formato Colunar (Parquet)      │
│  • Bancos NoSQL (Document)    │  • Validação Estrita de Dados │  • Arquivos Delimitados / JSON    │
│  • APIs REST e Plataformas    │  • Buffer Pooling (sync.Pool) │  • Egress para APIs e Webhooks    │
│  • Cofres Secret Manager/Vault│  • Sanitização PII/Segredos   │  • Quarentena de Falhas (DLQ)     │
└───────────────────────────────┴───────────────────────────────┴───────────────────────────────────┘
```

---

## 2. Features da Plataforma (Capacidades Operacionais Entregues)

Esta seção reúne todas as capacidades funcionais, técnicas e de engenharia 100% implementadas, testadas e prontas para uso em produção corporativa.

### 📥 2.1. Ingestão e Conectores de Origem
* **Arquivos e Storage (Local & Google Cloud Storage)**:
  - Leitura paralela de arquivos em discos locais e buckets **Google Cloud Storage (`gs://`)**.
  - Suporte nativo a formatos tabulares e semiestruturados (**CSV** com qualquer delimitador, **JSONLines / NDJSON** e **JSON Arrays `[...]`** com streaming decode).
  - Suporte a linhas JSON de até **64 MB** por registro sem travamento de buffer e com envio de erros não-bloqueante.
  - Descompressão automática e transparente em tempo real para **Gzip**, **Zstandard (Zstd)**, **Snappy** e **Bzip2**.
  - Normalização automática de codificações legadas (**ISO-8859-1 / Latin-1**, **Windows-1252**) para **UTF-8** padrão.
* **Bancos de Dados Relacionais e Documentais**:
  - Conexão e extração paralelizada em **PostgreSQL**, **MySQL** e **MongoDB**.
  - **Fatiamento Dinâmico (Range Slicing)**: O motor divide tabelas volumosas em fatias balanceadas sem sobrecarregar a memória dos nós workers.
  - *Worker Connection Pool Cache*: Reutilização segura de pools de conexão nos nós do Dataflow.
* **APIs REST e Serviços Web**:
  - Ingestão flexível através de 4 estratégias de paginação (**Número de Página**, **Offset/Limite**, **Tokens de Cursor** e **Cabeçalhos de Link**).
  - Controle ativo de taxa de requisições (*Rate Limiting*) via Token Bucket e retries com backoff exponencial.
  - Ciclo de vida DoFn com `Setup`/`Teardown` por bundle para reuso de clientes e conexões HTTP sem handshake TLS repetido.

### 📤 2.2. Destinos e Exportação Analítica (Sinks)
* **Google BigQuery Storage Write API (gRPC Pending Streams)**:
  - Carga analítica direta de alta performance no **Google BigQuery** utilizando a **Storage Write API** via gRPC.
  - **Atomic Bundle Commit**: Uso de *Pending Streams* com flush em buffer limitado por memória e finalização/commit atômico em `FinishBundle` e `Drain`, garantindo semântica *Exactly-Once*.
  - **Governança Estrita `CREATE_NEVER`**: Gravação estrita em tabelas pré-existentes governadas via IaC/Terraform.
  - **Zero Data Loss no Flush Final**: Registros com falha de serialização são roteados para a DLQ inclusive no flush de encerramento do bundle (`FinishBundle`).
* **Formato Colunar Apache Parquet**:
  - Gravação otimizada de dados colunares com compressão (**Snappy** e **Zstd**) diretamente em buckets GCS ou disco local, prontos para consultas analíticas imediatas.
* **Arquivos Delimitados e JSONL**:
  - Exportação de arquivos formatados com delimitadores configuráveis, quebras de linha personalizadas, compactação e checksum SHA-256 para auditoria.
* **Envio em Lote para APIs Externas**:
  - Disparo de registros validados em micro-lotes para sistemas externos e SaaS.
* **Operações Atômicas de Escrita (Zero Arquivos Órfãos)**:
  - Escrita temporária (*Stage Temp File*) com commit atômico no encerramento do bundle (`BundleFileStager`). Em caso de erro, arquivos parciais são descartados automaticamente via `AbortTemp`.
  - Campos de interface internos estritamente encapsulados (*unexported*), prevenindo falhas de serialização nula em workers remotos.

### ⚡ 2.3. Performance, Eficiência & Apache Beam Go SDK
* **Buffer Pooling com `sync.Pool`**:
  - Reutilização de buffers de memória em formatadores delimitados (`CSVFormatter`) e serializadores, eliminando alocações no Heap e reduzindo a pressão do Garbage Collector.
* **Zero-Allocation Binary Coders**:
  - Coders customizados otimizados para `GenericRecord`, `DeadLetterRecord`, `PipelineMetrics`, `PartitionSlice` e `PageSlice` registrados canonicamente em `graph_builder.go`.
* **Sub-Megabyte Liquid Sharding & Autoscaling**:
  - Implementação de `RestrictionSize` e `ByteOffsetTracker.TrySplit` thread-safe com cálculo de split fracionário, permitindo ao Dataflow Autoscaler ajustar dinamicamente os workers.
* **Graceful Shutdown & Dataflow Drain**:
  - Suporte completo a `Drain(ctx)` no `BundleFileStager` e `BigQuerySinkDoFn`, permitindo que workers salvem dados em voo com segurança durante preemptions em Spot VMs ou ordens de shutdown.
* **Coerção Numérica de Baixíssima Latência (Zero Allocations)**:
  - Parsers otimizados para inteiros (`CoerceInt64Fast` ~24ns/op) e decimais em ponto fixo (`CoerceDecimalFast` ~14ns/op) com zero alocação (`0 B/op`).
* **Blindagem do Ciclo de Vida DoFn**:
  - DoFns com ciclo estrito `Setup` → `StartBundle` → `ProcessElement` → `FinishBundle` → `Teardown`.
  - Eliminação de chamadas de `Setup` no hot path com proteção fail-fast para a DLQ.

### 🛡️ 2.4. Qualidade de Dados, Resiliência e Quarentena (DLQ)
* **Validação Estrita de Esquema**:
  - Coerção e verificação de tipos para inteiros, decimais, booleanos, textos, timestamps, datas e binários com regras configuráveis de overflow (fail, truncate, round).
* **Quarentena Auditável (Dead Letter Queue - DLQ)**:
  - Registros com inconsistências são isolados em JSONL com motivo detalhado, coluna afetada e carga bruta sanitizada.
* **Invariante de Conservação Estrita Observável**:
  - Garantia formal auditável persistida no DAG via `MetricsSinkDoFn` gerando `<outputDir>/pipeline_metrics.json`:
    $$\text{Total de Registros Lidos} = \text{Registros Gravados no Destino} + \text{Registros em Quarentena (DLQ)}$$
* **Proteção contra Falhas Transitórias**:
  - Resiliência com *Circuit Breakers* e *Memory Interning Limitado* para estabilidade em jobs longos.

### 🔐 2.5. Segurança, Privacidade e Observabilidade
* **Sanitização Operacional de PII & Segredos (LGPD / GDPR)**:
  - Motor de mascaramento automático (`pkg/telemetry`) para palavras-chave sensíveis (`password`, `secret`, `token`, `apikey`, `api_key`, `auth`, `credential`, `private_key`) e campos personalizados (`sensitive_fields` via `SecurityConfig`).
  - Redação automática (`[REDACTED]`) em logs estruturados (`StructuredLogger`), traces OpenTelemetry, payloads de erro no DLQ e amostras de auditoria.
* **Cofres de Segredos**:
  - Resolução dinâmica de senhas e tokens via **Google Cloud Secret Manager** e **OpenBao / HashiCorp Vault** (KV v1 e v2).
* **Tracing Distribuído OpenTelemetry Completo**:
  - Traces OTel com propagação W3C TraceContext em todos os adapters (GCS, LocalStorage, Secrets, SQL e Sinks) com marcação explícita de status de erro (`span.SetStatus(codes.Error, ...)`).

### 🧪 2.6. Engenharia de Software, Governança & Testes
* **Clean Architecture & SOLID**:
  - Segregação de responsabilidades estrita (Single Responsibility Principle) com todos os arquivos sob o limite de 300 linhas (modularização de `config.go` em 6 sub-estruturas).
  - Assertivas de tipo em tempo de compilação (`var _ Port = (*Adapter)(nil)`) mantidas 100% nos arquivos de produção.
* **Testes Baseados em Propriedades (Property-Based Testing)**:
  - Baterias generativas com `flyingmutant/rapid` validando o invariante de conservação estrita e integridade de indexação de esquemas sob milhares de combinações aleatórias.
* **Testes de Mutação (Mutation Testing)**:
  - Target automatizado `make mutation-test` com `gremlins unleash` para blindagem contra mutantes em lógica de domínio e transformações do Beam.
* **Fuzz Testing & Golden Schemas**:
  - Fuzzing contínuo (`go test -fuzz`) em decodificadores e parsers numéricos; validação de compatibilidade retroativa de schemas com fixtures versionadas.

---

## 3. Matriz de Capacidades da Plataforma (Status Consolidado)

| Categoria | Conector / Tecnologia | Capacidade Funcional | Status na Plataforma |
|---|---|---|:---:|
| **Origem (Source)** | Arquivos Locais & GCS (CSV, JSONL, JSON, TXT) | Leitura paralela, descompressão (Gzip/Zstd/Snappy/Bzip2) e conversão de charsets | ✅ **Disponível (Feature)** |
| **Origem (Source)** | Google Cloud Storage (GCS) | Ingestão em nuvem de buckets GCS (`gs://`) e emuladores locais | ✅ **Disponível (Feature)** |
| **Origem (Source)** | PostgreSQL | Leitura fatiada por chaves primárias e particionamento paralelo | ✅ **Disponível (Feature)** |
| **Origem (Source)** | MySQL | Extração paralela de tabelas relacionais | ✅ **Disponível (Feature)** |
| **Origem (Source)** | MongoDB | Ingestão documental particionada por intervalos de `ObjectID` | ✅ **Disponível (Feature)** |
| **Origem (Source)** | APIs REST / Webhooks | Coleta automatizada com 4 estratégias de paginação e rate limiting | ✅ **Disponível (Feature)** |
| **Origem (Source)** | Arquivos XML (Streaming Parser) | Extração hierárquica baseada em tokens de elementos sem carga total em RAM | 💡 **Roadmap (RD-01)** |
| **Origem (Source)** | Salesforce (SOQL / Bulk API) | Extração direta de objetos e relatórios corporativos SaaS | 💡 **Roadmap (RD-04)** |
| **Origem (Source)** | Apache Cassandra / Cloud Bigtable | Ingestão distribuída por token ranges e row keys | 💡 **Roadmap (RD-05)** |
| **Origem (Source)** | Databricks / Spark SQL Reader | Leitura direta via protocolo JDBC/Thrift ou extração de Delta Lake | 💡 **Roadmap (RD-06)** |
| **Destino (Sink)** | Google BigQuery | Carga direta de alta velocidade via BigQuery Storage Write API (gRPC) | ✅ **Disponível (Feature)** |
| **Destino (Sink)** | Apache Parquet (Local e GCS) | Escrita colunar otimizada (Snappy/Zstd) para Data Lakehouse | ✅ **Disponível (Feature)** |
| **Destino (Sink)** | Arquivos Delimitados / JSONL | Exportação de arquivos com checksum SHA-256 e compactação | ✅ **Disponível (Feature)** |
| **Destino (Sink)** | Quarentena DLQ (JSONL) | Segregação auditável de dados corrompidos com sanitização de PII | ✅ **Disponível (Feature)** |
| **Destino (Sink)** | Envio para APIs Externas | Disparo de dados em lotes para sistemas corporativos e SaaS | ✅ **Disponível (Feature)** |
| **Destino (Sink)** | Apache Iceberg / BigLake (GCS) | Gravação em tabelas abertas de Lakehouse com versionamento ACID | 💡 **Roadmap (RD-03)** |
| **Segurança & Ops** | Sanitização PII / Segredos | Mascaramento de dados confidenciais em logs, traces e DLQ (LGPD/GDPR) | ✅ **Disponível (Feature)** |
| **Segurança & Ops** | Secret Manager & OpenBao | Resolução centralizada de credenciais em cofres de segredos | ✅ **Disponível (Feature)** |
| **Observabilidade** | OpenTelemetry Completo | Tracing distribuído com status de erro explícito em todos os adapters | ✅ **Disponível (Feature)** |
| **Performance & I/O** | Buffer Pooling em Streams | Reúso via `sync.Pool` em descompressão (Gzip/Zstd/Snappy) e compressão | ✅ **Disponível (Feature)** |
| **Observabilidade & DAG** | Métricas Particionadas por Origem | Agregação granular por arquivo/tabela no Beam com Combiner Lifting e Coder binário | ✅ **Disponível (Feature)** |
| **Performance & Sinks** | JIT Parquet Direct Row Mapping | Gravação colunar direta via `WriteRows` sem reflexão ou alocações de maps | ✅ **Disponível (Feature)** |

---

## 4. Roadmap de Evolução da Plataforma (O que NÃO está desenvolvido)

Este roadmap consolida todas as oportunidades de expansão funcional e refinamentos de engenharia em aberto, priorizadas pelo método **WSJF (Weighted Shortest Job First)** com pontuação em **Fibonacci ($1, 2, 3, 5, 8, 13, 21$)**:

$$\text{Score de Prioridade} = \frac{\text{Valor de Negócio }(V)}{\text{Complexidade Técnica }(C)}$$

---

### 📥 4.1. Conectores de Origem & Formatos

#### **RD-01: Decodificador Streaming XML**
* **Score WSJF:** $V: 5 \ / \ C: 3 = \mathbf{1.67}$ (Prioridade: Alta)
* **Descrição Técnica:**
  - Implementar decodificador baseado em `xml.Decoder` aderente à interface `ports.StreamDecoder`.
  - Processar elementos de registros repetitivos em fluxo contínuo sem carregar o arquivo XML completo na memória Heap.
* **Valor de Negócio:** Viabiliza a ingestão direta de notas fiscais eletrônicas (NF-e/CT-e) e extratos bancários legados sem gargalo de memória.

#### **RD-04: Conector Salesforce Ingestão Direta (SOQL / Bulk API)**
* **Score WSJF:** $V: 8 \ / \ C: 5 = \mathbf{1.60}$ (Prioridade: Alta)
* **Descrição Técnica:**
  - Criar adaptador especializado aderente a `ports.PagedAPIReader` para autenticação OAuth2 e consumo de queries SOQL com paginação por cursor (`queryMore`).
* **Valor de Negócio:** Elimina ferramentas intermediárias proprietárias de extração de CRM para Data Lake / BigQuery.

#### **RD-05: Conectores NoSQL Corporativos (Apache Cassandra / Cloud Bigtable)**
* **Score WSJF:** $V: 8 \ / \ C: 5 = \mathbf{1.60}$ (Prioridade: Média-Alta)
* **Descrição Técnica:**
  - Implementação de leitores particionados por *Token Ranges* (Cassandra) e *Row Key Ranges* (Bigtable), integrados à família `PartitionQuerySourceSDF`.
* **Valor de Negócio:** Ingestão massiva e paralela de bancos de dados NoSQL transacionais de alta escala.

#### **RD-06: Conector Databricks / Spark SQL Reader**
* **Score WSJF:** $V: 8 \ / \ C: 5 = \mathbf{1.60}$ (Prioridade: Média-Alta)
* **Descrição Técnica:**
  - Leitor com suporte ao dialeto Spark SQL e transporte de dados otimizado via Apache Arrow Flight / JDBC particionado.
* **Valor de Negócio:** Interoperabilidade analítica direta entre clusters Databricks e o ecossistema Google Cloud.

---

### 🏛️ 4.2. Destinos Analíticos & Lakehouse

#### **RD-03: Sink para Apache Iceberg no GCP (BigLake / GCS)**
* **Score WSJF:** $V: 13 \ / \ C: 8 = \mathbf{1.63}$ (Prioridade: Alta)
* **Descrição Técnica:**
  - Gravação de dados em formato Apache Iceberg (arquivos de dados Parquet + metadados Avro/JSON de catálogo).
  - Integração com o catálogo BigLake REST Catalog e Google Cloud Storage.
* **Valor de Negócio:** Estabelece arquitetura de Lakehouse aberta com transações ACID, *time-travel queries* e evolução flexível de esquemas.

---

### ⚡ 4.3. Performance, Otimização de I/O & DAG (Entregue)

Todas as iniciativas prioritárias de otimização de I/O, DAG e alocação de memória foram entregues com sucesso:
- **RD-02 (Buffer Pooling em Descompressão/Compressão):** Reúso de instâncias via `sync.Pool` para `zstd`, `gzip` e `snappy`, reduzindo alocações transitórias em mais de 90%.
- **RD-09 (Métricas Particionadas por Origem no DAG):** Agregação granular de métricas com *Combiner Lifting* no Dataflow e Coder binário compacto de zero reflexão.
- **RD-10 (Compilação JIT de Mapeamento Parquet):** Escrita colunar direta via `WriteRows` sem instanciar `map[string]any`, alcançando ganho de 66% na latência de mapeamento e 90% na redução de alocações de Heap.

---

### 🔒 4.4. Segurança, Operações & Testes

#### **RD-07: Rotação Dinâmica de Credenciais em Execuções Longas**
* **Score WSJF:** $V: 8 \ / \ C: 5 = \mathbf{1.60}$ (Prioridade: Média-Alta)
* **Descrição Técnica:**
  - Rotina em background para renovar leases do Vault/OpenBao e tokens de autenticação GCP OAuth2 antes do término do seu TTL em jobs batch de grande volume (> 2 horas).
* **Valor de Negócio:** Prevenção de falhas por expiração de credenciais em pipelines de múltiplos terabytes.

#### **RD-08: Testes de Integração com Runner Local `prism`**
* **Score WSJF:** $V: 5 \ / \ C: 3 = \mathbf{1.67}$ (Prioridade: Alta)
* **Descrição Técnica:**
  - Adição de testes de integração executando o grafo completo via `prism` (runner local oficial do Apache Beam SDK v2).
* **Valor de Negócio:** Testes fiéis ao comportamento de shuffle distribuído sem custos de nuvem.

#### **RD-11: Documentação Operacional, Runbooks e SLOs**
* **Score WSJF:** $V: 5 \ / \ C: 3 = \mathbf{1.67}$ (Prioridade: Alta)
* **Descrição Técnica:**
  - Criação dos documentos `docs/slos.md` (SLOs de latência e limites de DLQ) e `docs/runbook.md` (procedimentos de resposta a incidentes e troubleshooting de Dataflow).
* **Valor de Negócio:** Redução do tempo médio de recuperação (MTTR) e padronização operacional para times de SRE e DataOps.

---

## 5. Tabela Geral do Roadmap Repriorizado (WSJF)

| ID | Oportunidade / Evolução | Área | Complexidade ($C$) | Valor ($V$) | Score WSJF ($V/C$) | Prioridade |
|---|---|---|:---:|:---:|:---:|:---:|
| **RD-01** | **Decodificador Streaming XML** | Origens & Ingestão | **3** | **5** | **1.67** | 🔴 **Alta** |
| **RD-08** | **Testes de Integração com Runner `prism`** | Qualidade & Testes | **3** | **5** | **1.67** | 🔴 **Alta** |
| **RD-11** | **Documentação Operacional (SLOs & Runbook)** | Operações & SRE | **3** | **5** | **1.67** | 🔴 **Alta** |
| **RD-03** | **Sink para Apache Iceberg / BigLake (GCS)** | Destinos & Lakehouse | **8** | **13** | **1.63** | 🔴 **Alta** |
| **RD-04** | **Conector Salesforce (SOQL / Bulk API)** | Origens & SaaS | **5** | **8** | **1.60** | 🟡 **Média-Alta** |
| **RD-05** | **Conectores Cassandra / Cloud Bigtable** | Origens & NoSQL | **5** | **8** | **1.60** | 🟡 **Média-Alta** |
| **RD-06** | **Conector Databricks / Spark SQL** | Origens & Lakehouse | **5** | **8** | **1.60** | 🟡 **Média-Alta** |
| **RD-07** | **Rotação Dinâmica de Credenciais** | Segurança & Resiliência | **5** | **8** | **1.60** | 🟡 **Média-Alta** |

