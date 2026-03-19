- [my firts commit](#my-firts-commit)

# Pipeline Batch Bovespa: ingestão e arquitetura de dados

## Objetivo
Construir um pipeline de dados completo para **extrair, processar e analisar** dados de ações ou índices da B3, utilizando AWS S3, Glue, Lambda e Athena.

## Passo a passo dos requisitos
### Passo a Passo Final (Aderente ao PDF)

#### 1. Ingestão Diária dos Dados B3 (Requisito 1)
Implementar script Python com `yfinance` para coletar dados diários de ações/índices da B3.
* **Ação:** Salvar dados em Parquet no caminho `s3://bovespa_data/raw/dt=YYYY-MM-DD/`.
* **Pontos de Atenção:**
    * Confirmar granularidade diária.
    * Garantir timezone/data correta da partição.
    * Padronizar schema (tipos numéricos e data).

---

#### 2. Armazenamento Bruto no S3 com Partição Diária (Requisito 2)
O bucket/prefixo `raw/` deve conter apenas dados brutos em Parquet.
* **Ação:** Particionamento obrigatório por data (`dt`).
* **Pontos de Atenção:**
    * Não salvar CSV/JSON no `raw` final.
    * Validar nome de partição consistente (`dt=...`).

---

#### 3. Gatilho por Evento no S3 (Requisito 3)
Configurar `S3 ObjectCreated` no prefixo `raw/` para acionar a Lambda trigger.
* **Pontos de Atenção:**
    * O requisito pede gatilho por bucket, não apenas agendamento da Lambda.
    * Evitar múltiplos disparos indevidos (filtro por sufixo `.parquet` e prefixo `raw/`).

---

#### 4. Lambda apenas para Iniciar Glue (Requisito 4)
A Lambda trigger deve chamar a função `StartJobRun` no AWS Glue.
* **Ação:** Pode passar argumentos como `--process_date` e o caminho de entrada.
* **Pontos de Atenção:**
    * A Role IAM da Lambda precisa de permissão `glue:StartJobRun`.
    * A Lambda **não** deve fazer o ETL, apenas a orquestração.

---

#### 5. ETL no Glue com Transformações Obrigatórias (Requisito 5)
No Glue Job (visual ou PySpark), aplicar as seguintes transformações:
* **A - Agregação/Sumarização:** (Ex: média, soma ou contagem por ticker/data).
* **B - Renomeação:** Renomear pelo menos 2 colunas existentes.
* **C - Cálculo Temporal:** (Ex: média móvel, variação diária ou extremos do período).
* **Pontos de Atenção:**
    * Deixar explícito no código/log quais colunas foram renomeadas.
    * Garantir ordenação temporal antes de calcular média móvel ou diferença.

---

#### 6. Saída Refinada no S3 (Requisito 6)
Gravar o resultado final em Parquet no prefixo `refined/`.
* **Ação:** Particionar obrigatoriamente por `dt` **e** `ticker` (ou código do índice).
* **Exemplo:** `s3://bovespa_data/refined/dt=2026-03-19/ticker=PETR4/`.
* **Pontos de Atenção:**
    * Confirmar que ambas as partições existem fisicamente na estrutura de pastas.
    * Evitar sobrescrita incorreta de partições antigas.

---

#### 7. Catálogo Automático no Glue (Requisito 7)
Atualizar ou registrar a tabela no **Glue Data Catalog** automaticamente.
* **Ação:** Pode ser via job com *catalog update* ou um Crawler executado ao final.
* **Pontos de Atenção:**
    * O banco de dados pode ser o `default`, conforme o enunciado.
    * Garantir que o schema seja compatível com o Athena após novas cargas.

---

#### 8. Consulta SQL no Athena (Requisito 8)
Consultar a tabela refinada no Athena utilizando SQL.
* **Ação:** Mostrar exemplos de consulta filtrando por `dt` e `ticker`.
* **Pontos de Atenção:**
    * Configurar o *S3 output location* do Athena.
    * Sempre usar filtro por partição para reduzir custo e melhorar a performance.

---

### Checklist de Validação Final (Para o Vídeo)

- [ ] **Raw Data:** Mostrar arquivos Parquet no S3 dentro de `dt=...`.
- [ ] **S3 Trigger:** Mostrar a configuração do evento acionando a Lambda.
- [ ] **Orquestração:** Mostrar logs da Lambda iniciando o `StartJobRun`.
- [ ] **Transformações:** Mostrar o código/fluxo do Glue com as 3 transformações (Agregação, Renomeação, Cálculo Temporal).
- [ ] **Refined Data:** Mostrar estrutura de pastas com dupla partição (`dt` e `ticker`).
- [ ] **Catalog:** Mostrar a tabela criada no Glue Data Catalog.
- [ ] **Athena:** Realizar uma query SQL de exemplo e mostrar o resultado.

### As 7 Magníficas
- Apple (`AAPL`): Eletrônicos, software e serviços.
- Microsoft (`MSFT`): Software, nuvem e IA.
- Nvidia (`NVDA`): Processadores e infraestrutura para IA.
- Alphabet (`GOOGL`): Serviços de busca, nuvem e IA.
- Amazon (`AMZN`): E-commerce e serviços em nuvem (AWS).
- Meta (`META`): Redes sociais e realidade virtual/IA.
- Tesla (`TSLA`): Veículos elétricos e energia sustentável.
## API de ingestao

A ingestao agora pode ser executada por API HTTP usando FastAPI.

### Subir a API localmente

```bash
uvicorn app.api.main:app --host 0.0.0.0 --port 8000
```

### Endpoints

- `GET /health`: healthcheck simples.
- `POST /ingest/yfinance`: dispara ingestao assincrona e retorna `job_id`.
- `GET /ingest/yfinance/{job_id}`: consulta status do job (`queued`, `running`, `succeeded`, `failed`).

### Exemplo de disparo

```bash
curl -X POST http://localhost:8000/ingest/yfinance \
  -H "Content-Type: application/json" \
  -d '{
    "bucket": "bovespa_data",
    "prefix": "raw",
    "execution_date": "2026-03-19",
    "aws_region": "us-east-1",
    "lookback_days": 10
  }'
```

### Exemplo de consulta de status

```bash
curl http://localhost:8000/ingest/yfinance/<job_id>
```
