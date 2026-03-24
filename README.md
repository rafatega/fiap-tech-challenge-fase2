Link do vídeo de apresentação: https://youtu.be/X1v6E7OrbgY

- [FIAP Tech Challenge Fase 2](#fiap-tech-challenge-fase-2)
  - [1. Resumo Executivo](#1-resumo-executivo)
  - [2. Objetivo do Desafio](#2-objetivo-do-desafio)
  - [2.1 Escopo desta entrega](#21-escopo-desta-entrega)
  - [3. Arquitetura da Solucao](#3-arquitetura-da-solucao)
    - [3.1 Fluxo operacional](#31-fluxo-operacional)
  - [4. Componentes do Projeto](#4-componentes-do-projeto)
    - [4.1 Codigo versionado](#41-codigo-versionado)
    - [4.2 Servicos AWS utilizados](#42-servicos-aws-utilizados)
  - [5. Estrutura de Dados no S3](#5-estrutura-de-dados-no-s3)
    - [5.1 Camada bruta (`raw`)](#51-camada-bruta-raw)
    - [5.2 Camada refinada (`refined`)](#52-camada-refinada-refined)
  - [6. Regras de Transformacao (Glue ETL)](#6-regras-de-transformacao-glue-etl)
  - [7. Aderencia aos Requisitos do PDF](#7-aderencia-aos-requisitos-do-pdf)
  - [8. Execucao do Projeto](#8-execucao-do-projeto)
    - [8.1 Pre-requisitos](#81-pre-requisitos)
    - [8.2 Instalar dependencias](#82-instalar-dependencias)
    - [8.3 Executar ingestao local](#83-executar-ingestao-local)
    - [8.4 Validar processamento na AWS](#84-validar-processamento-na-aws)
  - [9. Consultas de Exemplo (Athena)](#9-consultas-de-exemplo-athena)
  - [10. Seguranca e Governanca](#10-seguranca-e-governanca)
  - [11. Evidencias para Apresentacao](#11-evidencias-para-apresentacao)
  - [13. Configurando o ambiente AWS](#13-configurando-o-ambiente-aws)
    - [Usuário IAM](#usuário-iam)
    - [Role, Policy e código Lambda](#role-policy-e-código-lambda)
    - [Role, Policy e código Glue](#role-policy-e-código-glue)

# FIAP Tech Challenge Fase 2

Pipeline batch para ingestao, transformacao e consulta de dados de mercado financeiro (B3) usando AWS.

## 1. Resumo Executivo
Este projeto implementa um pipeline de dados orientado a eventos para:
- coletar dados diarios de ativos via `yfinance`;
- armazenar dados brutos em Parquet no Amazon S3 (`raw/`);
- disparar processamento ETL no AWS Glue via evento de upload no S3;
- publicar camada refinada em Parquet particionada para consulta;
- disponibilizar analise SQL no Amazon Athena.

Resultado: arquitetura simples, auditavel e aderente aos requisitos do desafio, com separacao clara entre ingestao, orquestracao e transformacao.

## 2. Objetivo do Desafio
Atender ao enunciado do PDF `Tech_Challenge_Fase_2_Requisitos.pdf`, cobrindo ponta a ponta:
- ingestao diaria;
- armazenamento particionado em S3;
- gatilho por evento;
- Lambda iniciando Glue Job;
- ETL com regras obrigatorias;
- publicacao refinada;
- catalogacao no Glue Data Catalog;
- consulta no Athena.

## 2.1 Escopo desta entrega
Arquitetura implementada e apresentada neste projeto:
- Script Bovespa (ingestao sob demanda);
- S3 Raw (bucket Bovespa);
- Lambda para trigger do Glue;
- Glue ETL Bovespa;
- S3 Refined;
- Athena para consulta.

## 3. Arquitetura da Solucao

```mermaid
flowchart LR
    A[Script Python\nIngestao yfinance] -->|Parquet| B[(S3 Raw\nraw/date=YYYY-MM-DD/ticker=...)]
    B -->|ObjectCreated| C[AWS Lambda\nStart Glue Job]
    C --> D[AWS Glue ETL\nPySpark]
    D -->|Parquet Particionado| E[(S3 Refined\nrefined/process_date=.../ticker=...)]
    D --> F[Glue Data Catalog\nTabela refinada]
    F --> G[Amazon Athena\nConsultas SQL]
```

### 3.1 Fluxo operacional
1. O script local executa a coleta dos tickers e grava Parquet no prefixo `raw/`.
2. O evento `ObjectCreated` do S3 aciona a Lambda.
3. A Lambda extrai a data da `key` e executa `glue:StartJobRun`.
4. O Glue processa os dados e grava a camada `refined/` com particoes.
5. O catalogo e atualizado (via crawler).
6. O Athena consulta a tabela refinada com filtro de particao.

## 4. Componentes do Projeto

### 4.1 Codigo versionado
- Script de ingestao: [app/ingest_bovespa.py](C:/Users/rafaeltegazzini/Documents/Pessoais/fiap-tech-challenge-fase2/app/ingest_bovespa.py)
- Dependencias: [pyproject.toml](C:/Users/rafaeltegazzini/Documents/Pessoais/fiap-tech-challenge-fase2/pyproject.toml)

### 4.2 Servicos AWS utilizados
- Amazon S3 (camadas `raw/`, `refined/` e saida de consultas do Athena)
- AWS Lambda (orquestracao do Glue)
- AWS Glue (ETL e catalogacao)
- AWS Glue Data Catalog (metadados)
- Amazon Athena (consulta analitica)
- IAM + CloudWatch Logs (seguranca e observabilidade)

## 5. Estrutura de Dados no S3

### 5.1 Camada bruta (`raw`)
Padrao de escrita usado no projeto:

```text
s3://bovespa-data/raw/date=YYYY-MM-DD/ticker=XXXX/dados.parquet
```

### 5.2 Camada refinada (`refined`)
Padrao de saida do ETL:

```text
s3://bovespa-data/refined/process_date=YYYY-MM-DD/ticker=XXXX/part-*.parquet
```

Observacao: o enunciado cita `dt`. Nesta implementacao foi adotado `date/process_date` mantendo a mesma estrategia de particionamento por data + ticker.

## 6. Regras de Transformacao (Glue ETL)
O job ETL aplica as transformacoes exigidas no desafio:
- agregacao/sumarizacao por ticker (contagem, volume total, maximo, minimo);
- renomeacao de colunas:
  - `open` -> `opening_price`
  - `close` -> `closing_price`
- calculos temporais e derivacoes:
  - `price_range = high - low`
  - `daily_return = closing_price - opening_price`
  - media movel de 3 periodos (`moving_avg_3`) por ticker ordenado por data.

## 7. Aderencia aos Requisitos do PDF

| Requisito | Como foi atendido |
|---|---|
| 1. Ingestao diaria | Script Python com `yfinance` para coleta de series diarias |
| 2. Raw no S3 particionado | Escrita Parquet no prefixo `raw/` com particao por data e ticker |
| 3. Gatilho S3 | Evento `ObjectCreated` no prefixo `raw/` |
| 4. Lambda inicia Glue | Lambda faz apenas `StartJobRun` do Glue |
| 5. ETL com 3 transformacoes | Agregacao, renomeacao e calculo temporal no Glue |
| 6. Saida refinada | Parquet em `refined/` com particoes por data e ticker |
| 7. Catalogo | Tabela registrada/atualizada no Glue Data Catalog |
| 8. Consulta SQL | Athena consultando tabela refinada com filtro de particao |

## 8. Execucao do Projeto

### 8.1 Pre-requisitos
- Python 3.13+
- Credenciais AWS configuradas (`aws configure`)
- Bucket S3 criado (ex.: `bovespa-data`)
- Permissoes IAM para S3, Glue, Lambda e Athena

### 8.2 Instalar dependencias
```bash
pip install boto3 pandas pyarrow yfinance
```

### 8.3 Executar ingestao local
```bash
python app/ingest_bovespa.py
```

### 8.4 Validar processamento na AWS
1. Confirmar arquivo no `raw/`.
2. Confirmar disparo da Lambda no CloudWatch.
3. Confirmar execucao do Glue Job.
4. Confirmar gravacao no `refined/`.
5. Confirmar tabela no Glue Data Catalog.
6. Executar query no Athena.

## 9. Consultas de Exemplo (Athena)

```sql
SELECT *
FROM "AwsDataCatalog"."bovespa_db"."refined"
WHERE process_date = '2026-03-23'
LIMIT 10;
```

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
ORDER BY ticker, date DESC;
```

## 10. Seguranca e Governanca
Boas praticas adotadas:
- separacao de roles IAM por servico (Lambda e Glue);
- principio do menor privilegio (escopo por bucket/prefixo);
- logging centralizado no CloudWatch;
- particionamento para eficiencia de custo e performance no Athena.

## 11. Evidencias para Apresentacao
Checklist objetivo para demonstracao:
- [✅] arquivo Parquet em `raw/date=.../ticker=.../`
- [✅] evento S3 configurado para a Lambda
- [✅] log da Lambda com `StartJobRun`
- [✅] job Glue com transformacoes obrigatorias
- [✅] arquivo Parquet em `refined/process_date=.../ticker=.../`
- [✅] tabela no Glue Data Catalog
- [✅] query no Athena com resultado retornado

## 13. Configurando o ambiente AWS
### Usuário IAM
Criei um usuário IAM `techchallenge-dev` com policy que o usuário crie e gerencie recursos necessários para o pipeline, como buckets S3, funções Lambda, jobs Glue e consultas Athena. O ideal em produção seria restringir os recursos específicos, mas para desenvolvimento e testes, o acesso amplo pode ser mais prático, ajudando em ambientes de aprendizado. A partir desse usuário foram geradas as chaves de acesso para configurar o AWS CLI e permitir que o script de ingestão e outros componentes interajam com os serviços AWS.
policy: policy-fiap-tech-challenge-2
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

### Role, Policy e código Lambda
Criei uma role `LabRole-Lambda-TechChallenge` para a função Lambda, com a seguinte 
policy:LambdaStartGlueJobPolicy
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
E o código da função Lambda é o seguinte:
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

### Role, Policy e código Glue
Criei uma role `LabRole-Glue-TechChallenge` para o job Glue, com a seguinte 
policy:GlueS3BovespaDataAccessPolicy
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
E o código do job Glue é o seguinte:
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
