# Guia Técnico — Ambiente Docker, Simulação, Debug Local e Deploy GCP Dataflow Flex Template

**Versão:** 1.0  
**Data:** 2026-08-16  
**Status:** Aprovado  
**Público:** Engenheiros de Dados, Arquitetos de Soluções e Desenvolvedores Go

---

## 1. Visão Geral da Arquitetura de Execução

O **Omnibeam Dataflow Compute Engine** foi projetado sob os princípios de **Clean Architecture** e **Portabilidade de Execução**. Ele suporta execução idêntica em três ambientes:

1. **Desenvolvimento e Debug Local no Host**: Código Go executando na máquina do desenvolvedor (via VS Code ou CLI), conectando-se aos serviços emulados no Docker Compose (`PostgreSQL`, `MySQL`, `OpenBao`, `Fake-GCS-Server`).
2. **Execução Containerizada Local**: Container Docker empacotado executando com o launcher oficial do Apache Beam / Flex Template.
3. **Google Cloud Dataflow (Produção)**: Container serverless distribuído no GCP Dataflow Flex Template gerenciado pelo Google Cloud.

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                                 Ambientes de Execução                                        │
├──────────────────────────────┬───────────────────────────────┬──────────────────────────────┤
│      1. Debug Local Host     │  2. Container Local (Docker)  │   3. GCP Cloud Dataflow      │
│                              │                               │                              │
│   VS Code (F5) / Go CLI      │   Docker Run (Flex Launcher)  │   Dataflow Flex Template     │
│   DirectRunner (Goroutines)  │   DirectRunner (Container)    │   DataflowRunner (Workers VM)│
│               │              │               │               │               │              │
│               ▼              │               ▼               │               ▼              │
│   Docker Compose Infra:      │   Docker Compose Infra:       │   Infraestrutura GCP / Cloud:│
│   • Postgres (:5432)         │   • Postgres (:5432)          │   • Cloud SQL                │
│   • MySQL (:3306)            │   • MySQL (:3306)             │   • Google Cloud Storage     │
│   • OpenBao (:8200)          │   • OpenBao (:8200)           │   • Secret Manager           │
│   • Fake-GCS (:4443)         │   • Fake-GCS (:4443)          │   • Cloud Logging / OTel     │
└──────────────────────────────┴───────────────────────────────┴──────────────────────────────┘
```

---

## 2. Subindo o Ambiente Docker de Desenvolvimento

O arquivo [`docker-compose.test.yml`](../docker-compose.test.yml) disponibiliza todos os serviços de suporte necessários para testes e simulações.

### 2.1. Iniciar os Serviços de Infraestrutura

Para subir todos os bancos de dados, cofre de segredos, emulador de storage, tracing Jaeger, métricas Prometheus e dashboards Grafana (mantendo o host livre para debug):

```powershell
# Subir os serviços de suporte em background
docker compose -f docker-compose.test.yml up -d postgres mysql fake-gcs-server openbao jaeger otel-collector prometheus grafana
```

### 2.2. Validando a Saúde dos Serviços

| Serviço | Porta no Host | Endpoint de Verificação | Credenciais / Configuração |
|---|---|---|---|
| **PostgreSQL** | `5432` | `pg_isready -h localhost -p 5432 -U testuser` | `user: testuser`, `pass: testpassword`, `db: testdb` |
| **MySQL** | `3306` | `mysqladmin -h 127.0.0.1 -P 3306 -u testuser -ptestpassword ping` | `user: testuser`, `pass: testpassword`, `db: testdb` |
| **OpenBao (Vault)** | `8200` | `http://127.0.0.1:8200/v1/sys/health` | Token Root: `testtoken` |
| **Fake GCS Server** | `4443` | `http://127.0.0.1:4443/storage/v1/b` | Bucket padrão: `test-bucket` |
| **Jaeger (Tracing UI)** | `16686` (UI) | `http://127.0.0.1:16686` | Visualizador de Spans e Rastreamento |
| **OTel Collector** | `4317` (gRPC)<br/>`4318` (HTTP)<br/>`8889` (Prom) | `http://127.0.0.1:8889/metrics` | Roteador OTLP (Traces $\rightarrow$ Jaeger, Metrics $\rightarrow$ Prometheus) |
| **Prometheus** | `9090` | `http://127.0.0.1:9090` | Engine de métricas em tempo real e PromQL |
| **Grafana** | `3000` | `http://127.0.0.1:3000` | Dashboards estilo Google Cloud Dataflow (Sem login) |

### 2.3. Parar ou Resetar o Ambiente

```powershell
# Parar os containers mantendo os volumes
docker compose -f docker-compose.test.yml down

# Parar e limpar completamente volumes e dados de teste
docker compose -f docker-compose.test.yml down -v
```

---

## 3. Simulando os Cenários de Ingestão

O projeto conta com manifestos prontos em [`testdata/configs/`](../testdata/configs/) cobrindo todos os casos de uso.

### 3.1. Cenário 1: Ingestão de Arquivo CSV Local para Parquet (Happy Path)

Executa a leitura de arquivo delimitado por vírgula e gera arquivo Parquet particionado:

```powershell
# PowerShell
$env:GOTMPDIR = "$PWD\.gotmp"
go run ./cmd/pipeline --config testdata/configs/manifest_happy_path.json --runner=direct
```

**Resultado esperado:**
- Saída gravada em `testdata/output/e2e-01/data-000.parquet`.
- Log JSON com severidade `INFO` comprovando $TotalRead = RowsWritten + DLQCount$.

---

### 3.2. Cenário 2: Ingestão com Decompressão Gzip, Charset Latin-1 e Delimitador Ponto-e-Vírgula

Demonstra a capacidade de composição do pipeline de I/O (`StreamWrapper`):

```powershell
$env:GOTMPDIR = "$PWD\.gotmp"
go run ./cmd/pipeline --config testdata/configs/manifest_compressed_latin1.json --runner=direct
```

---

### 3.3. Cenário 3: Tolerância a Falhas e Quarentena DLQ (Dead Letter Queue)

Processa dados contendo linhas com tipos inválidos e nulos em colunas obrigatórias:

```powershell
$env:GOTMPDIR = "$PWD\.gotmp"
go run ./cmd/pipeline --config testdata/configs/manifest_dlq_quarantine.json --runner=direct
```

**Resultado esperado:**
- Linhas válidas gravadas em Parquet.
- Linhas com erro segregadas em `testdata/output/e2e-03/dlq/quarantine-*.jsonl` com o payload original e mensagem de erro detalhada.

---

### 3.4. Cenário 4: Ingestão Paralela SQL (PostgreSQL & MySQL com Range Slicing)

Gera fatias dinâmicas de consulta SQL utilizando o `SQLSourceSDF`:

```powershell
# 1. PostgreSQL (Range slicing numérico em paralelo)
$env:GOTMPDIR = "$PWD\.gotmp"
go run ./cmd/pipeline --config testdata/configs/manifest_pg_int_pk.json --runner=direct

# 2. MySQL (Múltiplos tipos e particionamento)
go run ./cmd/pipeline --config testdata/configs/manifest_mysql_types.json --runner=direct
```

---

### 3.5. Cenário 5: Resolução Dinâmica de Segredos com OpenBao (KV v1 e v2)

Antes de rodar, popule os segredos de teste no OpenBao via API REST:

```powershell
# Popula credencial no OpenBao (KV v2)
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8200/v1/secret/data/postgres" `
  -Headers @{ "X-Vault-Token" = "testtoken" } `
  -ContentType "application/json" `
  -Body '{"data":{"password":"testpassword","user":"testuser","host":"localhost","port":5432,"database":"testdb"}}'

# Executa o pipeline com resolução via OpenBao
$env:GOTMPDIR = "$PWD\.gotmp"
go run ./cmd/pipeline --config testdata/configs/manifest_sec_openbao_kv2.json --runner=direct
```

---

### 3.6. Cenário 6: Observabilidade Completa — Tracing (Jaeger) e Telemetria em Tempo Real (Grafana & Prometheus)

O projeto implementa observabilidade de ponta a ponta com **OpenTelemetry (OTel)**, **Prometheus** e **Grafana** em conformidade com os mandatos M009 e M011.

#### 1. Execução do Pipeline com Telemetria Ativa:
Ao executar qualquer pipeline localmente apontando para o receptor OTLP gRPC (`localhost:4317`), os dados de tracing e métricas de throughput em tempo real são exportados em background:

```powershell
# Executa ingestão com telemetria habilitada
$env:GOTMPDIR = "$PWD\.gotmp"
$env:OTEL_EXPORTER_OTLP_ENDPOINT = "localhost:4317"
$env:WORKER_ID = "local-worker-01"
go run ./cmd/pipeline --config testdata/configs/manifest_happy_path.json --runner=direct
```

#### 2. Inspecionando Throughput no Dashboard Grafana:
1. Abra o navegador em **`http://localhost:3000`** (login anônimo habilitado automaticamente).
2. Acesse o dashboard pré-provisionado **"OmniBeam - Pipeline Throughput & Quality"**.
3. Visualize os gráficos estilo Google Cloud Dataflow em tempo real:
   - **Pipeline Stage Throughput (Elements / sec)**: Taxa de transferência por estágio (`extractor_valid`, `extractor_dlq`).
   - **Dataflow Worker Breakdown**: Segregação de throughput individual por worker (`service_instance_id`), permitindo diagnosticar data skew.
   - **I/O Write Speed (Bytes / sec / MB / sec)**: Taxa de gravação física no sink Parquet.
   - **Quarantine / DLQ Routing Rate**: Taxa de anomalias e erros roteados para a quarentena.
   - **Total Processed Elements & Bytes Written**: Contadores consolidados e gauges de volumetria.

#### 3. Inspecionando Spans no Jaeger UI:
1. Abra o navegador em **`http://localhost:16686`**.
2. No menu **Service**, selecione **`omnibeam-pipeline`**.
3. Clique em **Find Traces** e abra o trace mais recente para ver a árvore de execução em cascata (*Waterfall*):
   - **`IngestionOrchestrator.Run`** (Span raiz).
   - **Goroutines de Worker**: Spans paralelos de extração e gravação.
   - **`ParquetSink.WriteStream`**: Tempo de serialização e flush.
   - **`DLQSink.WriteStream`**: Persistência de quarentena.

---

## 4. Estratégia de Configuração e Gestão de Ambientes (Local vs Container vs GCP)

Para evitar caminhos e endereços IP "chumbados" (*hardcoded*) no código-fonte, o **Omnibeam** adota o padrão **12-Factor App** e **Clean Architecture**, desacoplando totalmente as configurações dos binários:

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                           Estratégia de Resolução de Configuração                           │
├──────────────────────────────┬───────────────────────────────┬──────────────────────────────┤
│      1. Desenvolvimento Local│  2. Container Docker Local    │   3. GCP Cloud Dataflow      │
├──────────────────────────────┼───────────────────────────────┼──────────────────────────────┤
│ • Arquivo: .env ou Env Vars  │ • docker run --env-file .env  │ • Variáveis de Job Dataflow  │
│ • OTEL: localhost:4317       │ • OTEL: jaeger:4317           │ • OTEL: Cloud Trace Exporter │
│ • Storage: 127.0.0.1:4443    │ • Storage: fake-gcs:4443      │ • Storage: gs:// (GCP ADC)   │
│ • Vault: 127.0.0.1:8200      │ • Vault: openbao:8200         │ • Secrets: GCP Secret Manager│
│ • Manifestos: testdata/local │ • Manifestos: testdata/local  │ • Manifestos: gs://...json   │
└──────────────────────────────┴───────────────────────────────┴──────────────────────────────┘
```

### 4.1. Uso do Arquivo `.env` para Desenvolvimento Local

O repositório disponibiliza o modelo [`.env.example`](../.env.example). Para utilizá-lo:

```powershell
# Copiar o template de variáveis de ambiente
Copy-Item .env.example .env
```

Principais variáveis suportadas pelo engine:

| Variável de Ambiente | Descrição | Valor Padrão Local | Produção (GCP Dataflow) |
|---|---|---|---|
| `GOTMPDIR` | Diretório de arquivos temporários do compilador Go | `.gotmp` | Gerenciado pelo OS container |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Endpoint do receptor OTLP gRPC para tracing e métricas | `localhost:4317` | OTel Collector ou Cloud Trace |
| `WORKER_ID` | Identificador único da instância / worker para métricas | Hostname | ID da VM do Dataflow Worker |
| `OTEL_SERVICE_NAME` | Nome do serviço para traces e métricas | `omnibeam-pipeline` | Nome do Job Dataflow |
| `STORAGE_EMULATOR_HOST` | Endpoint do emulador de GCS (Fake-GCS-Server) | `127.0.0.1:4443` | *Omitido* (usa credencial GCP) |
| `BAO_ADDR` | Endpoint da API REST do OpenBao / HashiCorp Vault | `http://127.0.0.1:8200` | URL do cofre corporativo |
| `GCP_PROJECT_ID` | Identificador do projeto Google Cloud | `local-project` | ID do Projeto GCP real |

### 4.2. Separação de Manifestos de Configuração (Local vs GCP)

Em vez de alterar o código Go, criam-se variações de manifestos JSON específicos para cada ambiente:

1. **Manifesto para Execução Local (`testdata/configs/manifest_pg_int_pk.json`)**:
   - Conecta no banco em `localhost:5432`.
   - Grava a saída em disco local `testdata/output/...`.
   - Utiliza resolução de segredo literal ou OpenBao local.
2. **Manifesto para Execução na GCP Dataflow (`gs://.../manifest_prod.json`)**:
   - Conecta no **Cloud SQL** via IP privado de VPC ou Cloud SQL Auth Proxy (`127.0.0.1:5432`).
   - Grava a saída no **Google Cloud Storage** (`gs://meu-bucket/trusted/...`).
   - Utiliza segredos gerenciados via **GCP Secret Manager** (`gcp:projects/123/secrets/db-pass/versions/latest`).

### 4.3. Redirecionamento Dinâmico de Hosts (`host_override`)

Quando o mesmo manifesto é compartilhado entre máquinas ou containers, o bloco `secrets_config.host_override` permite mapear nomes de host lógicos para o IP do ambiente sem modificar a query ou o manifesto base:

```json
"secrets_config": {
  "provider": "openbao",
  "host_override": {
    "db.internal.corp": "localhost",
    "production-db.gcp": "127.0.0.1"
  }
}
```

---

## 5. Como Debugar o Código Localmente com VS Code

A melhor prática de desenvolvimento no projeto é rodar o debugger nativo do Go no host, conectando-se diretamente às portas expostas dos serviços Docker.

### 5.1. Configuração do `.vscode/launch.json`

O arquivo de debug `.vscode/launch.json` pode ser configurado conforme o exemplo abaixo:

```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "Debug Pipeline (Happy Path File)",
      "type": "go",
      "request": "launch",
      "mode": "auto",
      "program": "${workspaceFolder}/cmd/pipeline",
      "args": [
        "--config",
        "${workspaceFolder}/testdata/configs/manifest_happy_path.json",
        "--runner=direct"
      ],
      "env": {
        "GOTMPDIR": "${workspaceFolder}/.gotmp"
      }
    },
    {
      "name": "Debug Pipeline (PostgreSQL SQL Source)",
      "type": "go",
      "request": "launch",
      "mode": "auto",
      "program": "${workspaceFolder}/cmd/pipeline",
      "args": [
        "--config",
        "${workspaceFolder}/testdata/configs/manifest_pg_int_pk.json",
        "--runner=direct"
      ],
      "env": {
        "GOTMPDIR": "${workspaceFolder}/.gotmp"
      }
    },
    {
      "name": "Debug Pipeline (Fake GCS Server)",
      "type": "go",
      "request": "launch",
      "mode": "auto",
      "program": "${workspaceFolder}/cmd/pipeline",
      "args": [
        "--config",
        "${workspaceFolder}/testdata/configs/manifest_gcs_csv_to_parquet.json",
        "--runner=direct"
      ],
      "env": {
        "GOTMPDIR": "${workspaceFolder}/.gotmp",
        "STORAGE_EMULATOR_HOST": "127.0.0.1:4443"
      }
    }
  ]
}
```

### 5.2. Passo a Passo para Debugar:

1. **Suba os containers necessários**:
   ```powershell
   docker compose -f docker-compose.test.yml up -d postgres fake-gcs-server openbao jaeger prometheus grafana
   ```
2. **Abra o VS Code** e coloque breakpoints nos pontos de interesse das 3 Famílias:
   - **Família 1 (Streams)**: `internal/beam/streams/byte_offset_tracker.go` $\rightarrow$ Método `TryClaim` ou `TrySplit`.
   - **Família 1 (Sinks)**: `internal/beam/streams/parquet_sink.go` $\rightarrow$ `ProcessElement` ou `FinishBundle`.
   - **Família 2 (Partições)**: `internal/beam/partitions/partition_query_sdf.go` $\rightarrow$ Execução de cada `PartitionSlice`.
   - **Família 3 (Paged API)**: `internal/beam/paged_api/paged_api_sdf.go` $\rightarrow$ Leitura de cada `PageSlice`.
   - **Universal Core**: `internal/beam/core/cast_and_validate.go` $\rightarrow$ Validação de tipos e emissão para DLQ/Audit.
   - **Segredos**: `internal/adapters/secrets/composite_secret_resolver.go` $\rightarrow$ Roteamento de segredos OpenBao / GCP.
3. Pressione **`F5`** (ou selecione a configuração desejada no menu *Run & Debug* do VS Code).
4. O debugger pausará exatamente na linha selecionada, permitindo inspecionar variáveis de restrição, buffers de memória e chamadas de rede.

---

## 6. Como Testar o Template Localmente como Container

Para simular exatamente como o Google Cloud Dataflow executará o template na nuvem, podemos rodar a imagem final com o entrypoint do launcher oficial:

### 6.1. Build da Imagem Docker

```powershell
# Build usando o Makefile
make docker-build

# Ou via comando docker direto
docker build -t omnibeam-pipeline:latest .
```

### 6.2. Execução Local com Payload Inline (`--config_payload`)

```powershell
# Leitura do manifesto JSON em uma variável e execução no container
$PAYLOAD = Get-Content -Raw testdata/configs/manifest_happy_path.json

docker run --rm `
  --network host `
  -v "${PWD}/testdata:/testdata" `
  omnibeam-pipeline:latest `
  --config_payload="$PAYLOAD"
```

---

## 7. Deploy no Google Cloud Dataflow (GCP Flex Template)

O deploy para a GCP segue o padrão de **Dataflow Flex Templates**, utilizando o **Google Artifact Registry** para armazenar a imagem Docker e o **Google Cloud Storage (GCS)** para o manifesto do template.

### 7.1. Pré-Requisitos na GCP

1. **Google Cloud SDK (`gcloud`)** instalado e autenticado:
   ```bash
   gcloud auth login
   gcloud auth configure-docker <REGION>-docker.pkg.dev
   ```
2. **Variáveis de Ambiente**:
   ```bash
   export PROJECT_ID="seu-projeto-gcp"
   export REGION="southamerica-east1" # Ex: São Paulo
   export REPO_NAME="dataflow-templates"
   export IMAGE_NAME="omnibeam-pipeline"
   export TAG="v1.0.0"
   export TEMPLATE_GCS_PATH="gs://${PROJECT_ID}-dataflow-templates/templates/omnibeam-pipeline.json"
   ```

---

### 7.2. Passo 1: Criar o Repositório no Artifact Registry (caso não exista)

```bash
gcloud artifacts repositories create ${REPO_NAME} \
    --repository-format=docker \
    --location=${REGION} \
    --description="Dataflow Flex Templates Docker Repository"
```

---

### 7.3. Passo 2: Build e Push da Imagem Docker

```bash
export IMAGE_URI="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/${IMAGE_NAME}:${TAG}"

# Build e push da imagem multiplataforma linux/amd64
docker build --platform linux/amd64 -t ${IMAGE_URI} .
docker push ${IMAGE_URI}
```

---

### 7.4. Passo 3: Construção da Especificação do Flex Template no GCS

O comando `gcloud dataflow flex-template build` combina a imagem Docker e o arquivo [`spec/metadata.json`](../spec/metadata.json):

```bash
gcloud dataflow flex-template build ${TEMPLATE_GCS_PATH} \
    --image="${IMAGE_URI}" \
    --sdk-language="GO" \
    --metadata-file="spec/metadata.json"
```

---

### 7.5. Passo 4: Execução do Job no Cloud Dataflow

Com o template construído, qualquer usuário ou orquestrador (ex: Cloud Composer / Airflow) pode disparar o job fornecendo o JSON de configuração:

```bash
# Exemplo de disparo de job no GCP Dataflow
gcloud dataflow flex-template run "omnibeam-ingestion-$(date +%Y%m%d-%H%M%S)" \
    --template-file-gcs-location="${TEMPLATE_GCS_PATH}" \
    --region="${REGION}" \
    --temp-location="gs://${PROJECT_ID}-dataflow-temp/temp/" \
    --staging-location="gs://${PROJECT_ID}-dataflow-temp/staging/" \
    --parameters config_payload='{
      "pipeline_id": "prod-orders-ingestion",
      "run_id": "run-prod-001",
      "pipeline_type": "ingestion",
      "source": {
        "type": "gcs",
        "path": "gs://meu-bucket-origem/raw/*.csv",
        "format": "csv",
        "delimiter": ",",
        "charset": "utf-8",
        "compression": "none",
        "schema": {
          "fields": [
            {"name": "id", "type": "int64", "nullable": false},
            {"name": "customer_id", "type": "string", "nullable": false},
            {"name": "total_amount", "type": "float64", "nullable": false}
          ]
        }
      },
      "destination": {
        "type": "gcs",
        "output_format": "parquet",
        "output_path": "gs://meu-bucket-destino/trusted/orders/",
        "compression": "snappy",
        "include_audit_columns": true
      },
      "dlq_config": {
        "enabled": true,
        "quarantine_path": "gs://meu-bucket-destino/quarantine/orders/",
        "max_error_percentage": 0.05
      }
    }'
```

```bash
# Exemplo de disparo de job SQL (Cloud SQL / PostgreSQL com Secret Manager)
gcloud dataflow flex-template run "omnibeam-sql-ingestion-$(date +%Y%m%d-%H%M%S)" \
    --template-file-gcs-location="${TEMPLATE_GCS_PATH}" \
    --region="${REGION}" \
    --temp-location="gs://${PROJECT_ID}-dataflow-temp/temp/" \
    --staging-location="gs://${PROJECT_ID}-dataflow-temp/staging/" \
    --parameters config_payload='{
      "pipeline_id": "prod-cloudsql-orders",
      "run_id": "run-prod-sql-001",
      "pipeline_type": "sql",
      "secrets_config": {
        "provider": "gcp",
        "gcp_project_id": "meu-projeto-gcp"
      },
      "database_source": {
        "driver": "postgres",
        "connection_uri": "gcp:projects/meu-projeto-gcp/secrets/cloudsql-orders-uri/versions/latest",
        "query": "SELECT id, customer_id, amount, status, created_at FROM orders WHERE id > 0",
        "partition_config": {
          "partition_column": "id",
          "batch_size": 5000
        },
        "schema": {
          "fields": [
            {"name": "id", "type": "int64", "nullable": false},
            {"name": "customer_id", "type": "string", "nullable": false},
            {"name": "amount", "type": "float64", "nullable": false},
            {"name": "status", "type": "string", "nullable": false},
            {"name": "created_at", "type": "timestamp", "nullable": false}
          ]
        }
      },
      "destination": {
        "type": "gcs",
        "output_format": "parquet",
        "output_path": "gs://meu-bucket-destino/trusted/orders_sql/",
        "compression": "snappy"
      },
      "dlq_config": {
        "enabled": true,
        "quarantine_path": "gs://meu-bucket-destino/quarantine/orders_sql/"
      }
    }'
```

---

## 8. Boas Práticas e Resumo de Comandos Rápidos

| Ação | Comando |
|---|---|
| **Subir Infra de Testes Completa** | `docker compose -f docker-compose.test.yml up -d` |
| **Acessar UI do Grafana (Throughput)** | Abrir `http://localhost:3000` no navegador (Sem login) |
| **Acessar UI do Prometheus (Metrics)** | Abrir `http://localhost:9090` no navegador |
| **Acessar UI do Jaeger (Tracing)** | Abrir `http://localhost:16686` no navegador |
| **Gerar Fixtures de Teste** | `go run ./pkg/testutils/cmd/generate_fixtures` (ou `make generate-fixtures`) |
| **Rodar Testes Unitários** | `go test -v ./internal/... ./pkg/... ./cmd/pipeline/...` (ou `make test`) |
| **Rodar Testes de Integração & Escala** | `go test -v -count=1 -tags=integration ./tests/functional/...` |
| **Rodar Cenários E2E Containerizados** | `make docker-test-functional` |
| **Build Binário Local** | `go build -o bin/pipeline.exe ./cmd/pipeline` (ou `make build`) |
| **Build Imagem Docker** | `docker build -t omnibeam-pipeline:latest .` (ou `make docker-build`) |
| **Limpeza Geral** | `docker compose -f docker-compose.test.yml down -v` (ou `make clean`) |

