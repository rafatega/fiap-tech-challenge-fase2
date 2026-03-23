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

## Configurando o Lambda
1. Entrar no IAM para criar a role necessária para a Lambda.
   1. No menu esquerdo, clique em "Roles" e depois em "Create role".
   2. Em "Select type of trusted entity", escolha "AWS service".
   3. Em Use case, selecione "Lambda" e clique em "Next".
   4. Adicione a política `AWSLambdaBasicExecutionRole` para permitir que a Lambda escreva logs no CloudWatch.
   5. Crie uma policy personalizada com as seguintes permissões para permitir que a Lambda inicie o Glue Job:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "StartSpecificGlueJob",
      "Effect": "Allow",
      "Action": [
        "glue:StartJobRun",
        "glue:GetJob",
        "glue:GetJobRun",
        "glue:GetJobRuns"
      ],
      "Resource": "*"
    }
  ]
}
```
   6. Dê um nome à role (ex: `LambdaStartGlueJobPolicy`) e finalize a criação.
   7. Volte para a criação de role, atualize e marque também a policy personalizada criada.
   8. Defina o nome da role (ex: `LabRole-Lambda-TechChallenge`) e finalize a criação.
2. Criando o Lambda function:
   1. Create function.
   2. Author from scratch.
   3. Function name: `lambda-start-glue-bovespa`.
   4. Runtime: Python 3.13 (ou mais recente disponível).
   5. Architecture: x86_64 (para compatibilidade com dependências).
   6. Execution role: Use an existing role e selecione a role criada para a Lambda (ex: `LabRole-Lambda-TechChallenge`).
   7. Após criada, vá na aba de código e implemente a função para iniciar o Glue Job usando `boto3`:
```python
import json
import urllib.parse
import boto3
from botocore.exceptions import ClientError

glue = boto3.client("glue")

GLUE_JOB_NAME = "glue-etl-bovespa-refined"


def extract_process_date_from_key(key: str) -> str:
    """
    Extrai a data de uma key no formato:
    raw/date=YYYY-MM-DD/ticker=XXXX/dados.parquet
    """
    parts = key.split("/")
    for part in parts:
        if part.startswith("date="):
            return part.replace("date=", "")
    raise ValueError(f"Não foi possível extrair process_date da key: {key}")


def lambda_handler(event, context):
    print("Evento recebido:")
    print(json.dumps(event))

    for record in event.get("Records", []):
        event_name = record.get("eventName", "")
        if not event_name.startswith("ObjectCreated"):
            continue

        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

        if not key.startswith("raw/"):
            print(f"Ignorando objeto fora de raw/: {key}")
            continue

        process_date = extract_process_date_from_key(key)

        print(
            f"Tentando iniciar Glue Job para bucket={bucket}, "
            f"process_date={process_date}, key={key}"
        )

        try:
            response = glue.start_job_run(
                JobName=GLUE_JOB_NAME,
                Arguments={
                    "--source_bucket": bucket,
                    "--process_date": process_date
                }
            )
            print(f"Glue Job iniciado com JobRunId: {response['JobRunId']}")

        except ClientError as e:
            error_code = e.response["Error"]["Code"]

            if error_code == "ConcurrentRunsExceededException":
                print(
                    f"Glue job já está em execução. "
                    f"Novo disparo ignorado para process_date={process_date}, key={key}"
                )
            else:
                raise

    return {
        "statusCode": 200,
        "body": json.dumps("Evento processado com sucesso")
    }
```
   8. Clique em deploy para salvar a função.
   9. O que esse código faz:
      1.  recebe o evento do S3
      2.  decodifica a key do objeto
      3.  ignora qualquer coisa fora de raw/
      4.  chama o Glue Job: glue-etl-bovespa-refined
      5.  passa os argumentos: --source_bucket e --source_key
   10. Configuração recomendada:
       1.  Vá em General configuration e aumente a Memory para 256 MB e o timeout para 3 minuto, para garantir que a Lambda tenha recursos suficientes para iniciar o Glue Job mesmo em picos de carga.
       2. Também pode configurar a variável de ambiente GLUE_JOB_NAME para evitar hardcoding do nome do job no código: GLUE_JOB_NAME = glue-etl-bovespa-refined
    11. Também é possível fazer um teste manual da Lambda usando um evento de exemplo do S3 para garantir que a função está funcionando corretamente antes de configurar o gatilho:
        1.  Na tela da Lambda, clique em "Test" e depois em "Configure test event".
        2.  Create new event e deixe as demais configurações como estão.
		3.  No editor de evento, cole o seguinte JSON de exemplo, que simula um evento de criação de objeto no S3 dentro do prefixo raw/.
	    4. Salve o evento de teste e clique em "Test" para executar a Lambda com esse evento simulado.
		5. Verifique os logs no CloudWatch para confirmar que a Lambda processou o evento corretamente e iniciou o Glue Job.

## Configurando o Glue Jobs
1. Entrar no IAM para criar a role necessária para o Glue Job.
   1. No menu esquerdo, clique em "Roles" e depois em "Create role".
   2. Em "Select type of trusted entity", escolha "AWS service".
   3. Em Use case, selecione "Glue" e clique em "Next".
   4. Adicione a política `AWSGlueServiceRole` para permitir que o Glue execute as tarefas necessárias.
   5. Crie uma policy personalizada com as seguintes permissões para permitir que o Glue acesse os buckets S3:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListBucketAccess",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": "arn:aws:s3:::bovespa-data"
    },
    {
      "Sid": "ReadRawWriteRefinedAndAthenaResults",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::bovespa-data/raw/*",
        "arn:aws:s3:::bovespa-data/refined/*",
        "arn:aws:s3:::bovespa-data/athena-results/*"
      ]
    }
  ]
}
```
   6. Dê um nome à policy (ex: `GlueS3BovespaDataAccessPolicy`) e finalize a criação.
   7. Volte para a criação de role, atualize e marque também a policy personalizada criada.
   8. Defina o nome da role (ex: `LabRole-Glue-TechChallenge`) e finalize a criação.

### Estratégia do ETL
Vamos ler um arquivo bruto com colunas como:
* date
* ticker
* open
* high
* low
* close
* volume

E no Glue vamos:
* renomear open para opening_price
* renomear close para closing_price
* calcular price_range = high - low
* calcular daily_return = closing_price - opening_price
* calcular média móvel de 3 períodos para closing_price
* manter date e ticker
* gravar no refined/

### Passo a passo
1. Entre no AWS Glue.
2. Clique em "ETL Jobs"
3. Na tela de Create job, clique em 'Author code with a script editor".
4. Selecione Spark no engine, opção Start fresh.
5. Renomeie o job (ex: `glue-etl-bovespa-refined`).
6. No script, cole o código python para realizar as transformações necessárias:
```python
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "source_bucket",
        "process_date"
    ]
)

source_bucket = args["source_bucket"]
process_date = args["process_date"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

input_path = f"s3://{source_bucket}/raw/date={process_date}/"
output_path = f"s3://{source_bucket}/refined/"

print(f"Lendo dados brutos de: {input_path}")
print(f"Gravando dados refinados em: {output_path}")

df = spark.read.parquet(input_path)

required_columns = {"date", "ticker", "open", "high", "low", "close", "volume"}
missing_columns = required_columns - set(df.columns)

if missing_columns:
    raise ValueError(
        f"Colunas obrigatórias ausentes: {missing_columns}. "
        f"Colunas encontradas: {df.columns}"
    )

df = (
    df
    .withColumnRenamed("open", "opening_price")
    .withColumnRenamed("close", "closing_price")
)

df = df.withColumn("date", F.to_timestamp("date"))

df = (
    df
    .withColumn("price_range", F.col("high") - F.col("low"))
    .withColumn("daily_return", F.col("closing_price") - F.col("opening_price"))
)

window_spec = Window.partitionBy("ticker").orderBy("date").rowsBetween(-2, 0)

df = df.withColumn(
    "moving_avg_3",
    F.avg("closing_price").over(window_spec)
)

summary_df = (
    df.groupBy("ticker")
    .agg(
        F.count("*").alias("record_count"),
        F.sum("volume").alias("total_volume"),
        F.max("high").alias("max_price"),
        F.min("low").alias("min_price")
    )
)

print("Resumo agregado por ticker:")
summary_df.show(truncate=False)

df = df.withColumn("process_date", F.lit(process_date))

(
    df.write
    .mode("overwrite")
    .partitionBy("process_date", "ticker")
    .parquet(output_path)
)

print(f"Dados refinados salvos em: {output_path}")
```
7. Em "Job details", selecione a role criada para o Glue (ex: `LabRole-Glue-TechChallenge`).
   1. Glue version: Selecione a mais recente.
   2. Language: Python.
   3. Worker type: G 1X (a mais barata).
   4. Worker count: 2 (para garantir paralelismo mínimo).

Atenção: Na parte de parâmetros do job, não precisa colocar source_bucket e source_key fixos agora, porque a Lambda vai passar isso dinamicamente quando chamar o Glue.

## Configurando o gatilho no S3
1. Acesse o console AWS S3 e clique no bucket criado (ex: `bovespa-data`).
2. Vá para a aba "Properties" e role até "Event notifications".
3. Clique em "Create event notification".
4. Event name: trigger-lambda-raw-upload
5. Event types: Marque "All object create events".
6. Prefix: raw/
7. Destination: Lambda function
8. Function: Selecione a Lambda criada (ex: `lambda-start-glue-bovespa`).
9. Salve a configuração.

## Configurando o Glue Data Catalog
Vamos criar um Crawler para atualizar o catálogo automaticamente após o job de ETL, garantindo que a tabela esteja sempre atualizada com os dados refinados.
1. Crie uma estrutura no S3 para os resultados do Athena (ex: `athena-results/`).
2. Abra o console AWS Glue e vá para "Data Catalog" e depois "Crawlers".
3. Clique em "Create crawler".
4. Crawler name: `crawler-bovespa-refined`
5. Is your data already mapped to Glue tables? No
6. Add a data source: S3
7. Location of S3 data: In this account, select the bucket e marque a pasta `s3://bovespa-data/refined/`.
8. Clique em "Next".
9. Escolha um IAM role para o crawler (pode ser a mesma do Glue Job, ex: `LabRole-Glue-TechChallenge`) e clique em "Next".
10. Em Output configuration, em Target database, selecione "Add database" e crie um novo banco de dados (ex: `bovespa_db`).
    1.  Database type: Glue Database
11. Selecione esse database criado.
12. Crawler schedule: On demand
13. Crie o crawler.
14. Abra o crawler criado e clique em "Run crawler" para executar a primeira varredura e criar a tabela no Glue Data Catalog.
    1.  Verifique se o status do crawler mudou para "Succeeded"
    2.  Na aba "Data catalog", vá em "Tables" dentro do database criado e confirme que a tabela foi criada com o schema correto.

## Configurando o Athena
1. Acesse o console AWS Athena.
2. Na primeira vez, será solicitado configurar um local de saída para os resultados das consultas.
   1. Use o bucket criado para isso (ex: `s3://bovespa-data/athena-results/`).
3. Em "Query settings">"Query result encryption", clique em "Manage" e coloque o bucket criado para salvar os resultados (ex: `s3://bovespa-data/athena-results/`).

Query exemplo:
```sql
SELECT * FROM "AwsDataCatalog"."bovespa_db"."refined"
WHERE process_date = '2026-03-23'
LIMIT 10;
```
---
```sql
SELECT
    ticker,
    process_date,
    date,
    opening_price,
    closing_price,
    price_range,
    daily_return,
    moving_avg_3,
    volume
FROM "AwsDataCatalog"."bovespa_db"."refined"
WHERE process_date = '2026-03-23'
ORDER BY process_date DESC, ticker, date DESC;
```
---
```sql
SELECT
    ticker,
    process_date,
    COUNT(*) AS total_registros,
    SUM(volume) AS volume_total,
    MAX(closing_price) AS max_closing_price,
    MIN(closing_price) AS min_closing_price,
    AVG(moving_avg_3) AS avg_moving_avg_3
FROM "AwsDataCatalog"."bovespa_db"."refined"
WHERE process_date = '2026-03-23'
GROUP BY ticker, process_date
ORDER BY process_date DESC, ticker;
```




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
