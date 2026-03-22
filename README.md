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

## Configurando a AWS

### Criando um novo usuário IAM
1. Acesse o console AWS IAM.
2. Clique em "Users" e depois em "Add user".
3. Digite um nome de usuário (ex: `techchallenge-dev`).
4. Selecione "Programmatic access" para permitir acesso via API.
5. Selecione "Attach policies directly" e crie uma nova política personalizada com as seguintes permissões:
   1. Essas permissões permitem que o usuário crie e gerencie recursos necessários para o pipeline, como buckets S3, funções Lambda, jobs Glue e consultas Athena. O ideal em produção seria restringir os recursos específicos, mas para desenvolvimento e testes, o acesso amplo pode ser mais prático, ajudando em ambientes de aprendizado.
```json
{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Sid": "S3ProjectAccess",
			"Effect": "Allow",
			"Action": [
				"s3:CreateBucket",
				"s3:ListAllMyBuckets",
				"s3:ListBucket",
				"s3:GetBucketLocation",
				"s3:GetBucketNotification",
				"s3:PutBucketNotification",
				"s3:GetBucketPolicy",
				"s3:PutBucketPolicy",
				"s3:GetBucketAcl",
				"s3:PutBucketAcl",
				"s3:GetObject",
				"s3:PutObject",
				"s3:DeleteObject"
			],
			"Resource": "*"
		},
		{
			"Sid": "LambdaProjectAccess",
			"Effect": "Allow",
			"Action": [
				"lambda:CreateFunction",
				"lambda:GetFunction",
				"lambda:UpdateFunctionCode",
				"lambda:UpdateFunctionConfiguration",
				"lambda:InvokeFunction",
				"lambda:AddPermission",
				"lambda:RemovePermission",
				"lambda:CreateEventSourceMapping",
				"lambda:DeleteFunction",
				"lambda:ListFunctions",
				"lambda:GetPolicy"
			],
			"Resource": "*"
		},
		{
			"Sid": "GlueProjectAccess",
			"Effect": "Allow",
			"Action": [
				"glue:CreateJob",
				"glue:UpdateJob",
				"glue:GetJob",
				"glue:StartJobRun",
				"glue:GetJobRun",
				"glue:GetJobRuns",
				"glue:BatchStopJobRun",
				"glue:CreateCrawler",
				"glue:UpdateCrawler",
				"glue:StartCrawler",
				"glue:StopCrawler",
				"glue:GetCrawler",
				"glue:GetCrawlers",
				"glue:CreateDatabase",
				"glue:GetDatabase",
				"glue:GetDatabases",
				"glue:CreateTable",
				"glue:GetTable",
				"glue:GetTables",
				"glue:UpdateTable",
				"glue:DeleteTable"
			],
			"Resource": "*"
		},
		{
			"Sid": "AthenaProjectAccess",
			"Effect": "Allow",
			"Action": [
				"athena:StartQueryExecution",
				"athena:GetQueryExecution",
				"athena:GetQueryResults",
				"athena:StopQueryExecution",
				"athena:ListWorkGroups",
				"athena:GetWorkGroup"
			],
			"Resource": "*"
		},
		{
			"Sid": "GlueCatalogAndLogsAccess",
			"Effect": "Allow",
			"Action": [
				"logs:CreateLogGroup",
				"logs:CreateLogStream",
				"logs:PutLogEvents",
				"logs:DescribeLogGroups",
				"logs:DescribeLogStreams",
				"cloudwatch:PutMetricData"
			],
			"Resource": "*"
		},
		{
			"Sid": "AllowRoleManagementForProject",
			"Effect": "Allow",
			"Action": [
				"iam:CreateRole",
				"iam:GetRole",
				"iam:PassRole",
				"iam:AttachRolePolicy",
				"iam:PutRolePolicy",
				"iam:UpdateAssumeRolePolicy",
				"iam:TagRole"
			],
			"Resource": "*"
		},
		{
			"Sid": "IAMConsoleReadAccess",
			"Effect": "Allow",
			"Action": [
				"iam:ListUsers",
				"iam:GetUser",
				"iam:ListRoles",
				"iam:GetRole",
				"iam:ListPolicies",
				"iam:GetPolicy",
				"iam:GetPolicyVersion",
				"iam:ListAttachedUserPolicies",
				"iam:ListAttachedRolePolicies",
				"iam:ListRolePolicies",
				"iam:ListUserPolicies",
				"iam:GetAccountSummary",
				"iam:ListMFADevices",
				"iam:ListInstanceProfiles",
				"iam:ListOpenIDConnectProviders",
				"iam:ListSAMLProviders",
				"iam:GenerateServiceLastAccessedDetails",
				"iam:GetServiceLastAccessedDetails"
			],
			"Resource": "*"
		}
	]
}
```
   2. Revise as permissões e clique em "Next: Tags" (opcional) e depois em "Next: Review".
   3. Dê um nome à política (ex: `policy-fiap-tech-challenge-2`) e clique em "Create policy".
6. Usuário criado com sucesso! Anote a:
   1. Console sign-in URL (ex: `https://697169065885.signin.aws.amazon.com/console`).
   2. User name (ex: `techchallenge-dev`).
   3. Console password (confidencial).
7. Use a URL de login para acessar o console AWS com o usuário criado e a senha fornecida.

### Criar Access Key para o usuário IAM
1. No console AWS IAM, clique em "Users" e selecione o usuário criado (ex: `techchallenge-dev`).
2. Clique em "Security credentials" e depois em "Create access key".
3. Selecione "Command Line Interface (CLI)" e clique em "Next".
4. Copie a Access Key ID e Secret Access Key gerados. 
   1. **Importante:** O Secret Access Key só é mostrado uma vez, então guarde-o em um local seguro (ex: gerenciador de senhas).

### Configurando o CLI V2
1. Instale o AWS CLI V2 seguindo as instruções oficiais: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html
2. Abra o terminal e configure o AWS CLI com as credenciais do usuário criado:
```bash
aws configure
```
3. Insira a Access Key ID, Secret Access Key, região padrão (ex: `sa-east-1`) e formato de saída (ex: `json`).
4. Teste a configuração listando os buckets S3:
```bash
aws s3 ls
```

### Criando o bucket S3
1. No console AWS S3, clique em "Create bucket".
2. Dê um nome único ao bucket (ex: `bovespa-data')
3. Sobre o Versionamento:
   1. Para esse projeto, não é obrigatório. Como vou gravar dados particionados por data e ticker, já vai ter organização suficiente.
4. O resto das opções pode ser mantido como padrão para simplificar a configuração.
5. Clique em "Create bucket" para finalizar.
6. Clique no bucket;
   1. Crie as pastas `raw/` e `refined/` para organizar os dados.

## Script de Ingestão
O script de ingestão utiliza a biblioteca `yfinance` para coletar dados diários de ações ou índices da B3 e salva os dados em formato Parquet no S3, seguindo a estrutura de partição por data.
1. O script será executado localmente e de forma manual.
2. Busca os dados usando a API do Yahoo Finance.
3. Organiza em DataFrame.
4. Converte para Parquet.
5. Salva no S3 com partição por data (`dt=YYYY-MM-DD`).

### Codificando
1. Instale as dependências necessárias:
```bash
pip install boto3 pandas pyarrow yfinance
```
2. Crie o script de ingestão (ex: `ingest_bovespa.py`) com o seguinte conteúdo:
3. Implemente o script de ingestão.

### Bucket
Para separar este projeto do restante dos dados, foi criado um bucket específico para a Bovespa:
```bash
