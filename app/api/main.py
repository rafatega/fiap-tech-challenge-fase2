from __future__ import annotations

import logging
from datetime import date, datetime
from threading import Lock
from typing import Literal
from uuid import uuid4

from fastapi import BackgroundTasks, FastAPI, HTTPException
from pydantic import BaseModel, Field

from app.ingest.ingest_yfinance_toS3 import (
    Config,
    IngestionResult,
    LOCAL_TZ,
    run_ingestion,
)

LOGGER = logging.getLogger("ingest_api")

# Registro em memoria para estado de jobs.
#
# Justificativa:
# - simples para primeiro passo;
# - suficiente para um unico processo da API;
# - facil evolucao para persistencia externa (DynamoDB/Redis/Postgres) depois.
JOB_STORE: dict[str, dict] = {}
JOB_LOCK = Lock()


class IngestRequest(BaseModel):
    """
    Payload de disparo da ingestao.

    Campos com default permitem chamada simples da API, enquanto bucket continua
    obrigatorio para evitar gravar em destino indefinido.
    """

    bucket: str = Field(..., description="Bucket S3 de destino.")
    prefix: str = Field(default="raw", description="Prefixo raiz no bucket.")
    execution_date: date | None = Field(
        default=None,
        description="Data de processamento no formato YYYY-MM-DD. Se omitido, usa hoje em America/Sao_Paulo.",
    )
    aws_region: str = Field(default="us-east-1", description="Regiao AWS do cliente S3.")
    lookback_days: int = Field(
        default=10,
        ge=1,
        le=60,
        description="Janela de dias para localizar o ultimo pregao valido.",
    )


class IngestAcceptedResponse(BaseModel):
    """Resposta de aceite do job assincorno."""

    job_id: str
    status: Literal["queued", "running"]
    message: str


class JobStatusResponse(BaseModel):
    """
    Estado atual de um job de ingestao.

    O campo result e preenchido apenas em sucesso.
    O campo error e preenchido apenas em falha.
    """

    job_id: str
    status: Literal["queued", "running", "succeeded", "failed"]
    created_at: str
    started_at: str | None = None
    finished_at: str | None = None
    result: dict | None = None
    error: str | None = None


app = FastAPI(
    title="YFinance Ingestion API",
    description=(
        "API para disparar ingestao da MAG7 via yfinance e upload em parquet no S3. "
        "Fluxo pensado para integracao com servicos AWS (API Gateway, EventBridge, Step Functions)."
    ),
    version="1.0.0",
)


def _utc_now_iso() -> str:
    """Retorna timestamp UTC em ISO-8601 para auditoria dos jobs."""

    return datetime.utcnow().isoformat() + "Z"


def _build_config(payload: IngestRequest) -> Config:
    """
    Converte payload HTTP para Config de dominio.

    Mantemos essa traducao isolada para desacoplar contrato da API do contrato
    interno da regra de negocio. Isso facilita evolucao de um lado sem quebrar o outro.
    """

    resolved_execution_date = payload.execution_date or datetime.now(LOCAL_TZ).date()
    return Config(
        bucket=payload.bucket,
        prefix=payload.prefix.strip("/"),
        execution_date=resolved_execution_date,
        aws_region=payload.aws_region,
        lookback_days=payload.lookback_days,
    )


def _job_runner(job_id: str, payload: IngestRequest) -> None:
    """
    Worker em background que executa a ingestao.

    Regras de status:
    - queued -> running no inicio da execucao real;
    - running -> succeeded com resultado resumido;
    - running -> failed em qualquer excecao capturada.
    """

    with JOB_LOCK:
        JOB_STORE[job_id]["status"] = "running"
        JOB_STORE[job_id]["started_at"] = _utc_now_iso()

    try:
        cfg = _build_config(payload)
        result: IngestionResult = run_ingestion(cfg)

        with JOB_LOCK:
            JOB_STORE[job_id]["status"] = "succeeded"
            JOB_STORE[job_id]["finished_at"] = _utc_now_iso()
            JOB_STORE[job_id]["result"] = {
                "s3_uri": result.s3_uri,
                "rows_written": result.rows_written,
                "partition_date": result.partition_date,
                "execution_date": result.execution_date,
            }
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Falha no job %s", job_id)
        with JOB_LOCK:
            JOB_STORE[job_id]["status"] = "failed"
            JOB_STORE[job_id]["finished_at"] = _utc_now_iso()
            JOB_STORE[job_id]["error"] = str(exc)


@app.get("/health")
def health() -> dict[str, str]:
    """Healthcheck simples para monitoramento e readiness probes."""

    return {"status": "ok"}


@app.post("/ingest/yfinance", response_model=IngestAcceptedResponse, status_code=202)
def start_ingestion(payload: IngestRequest, background_tasks: BackgroundTasks) -> IngestAcceptedResponse:
    """
    Dispara um job assincorno de ingestao.

    Importante:
    - o endpoint retorna rapido com 202 para nao sofrer timeout em API Gateway;
    - o progresso deve ser consultado via GET /ingest/yfinance/{job_id}.
    """

    job_id = str(uuid4())
    created_at = _utc_now_iso()

    with JOB_LOCK:
        JOB_STORE[job_id] = {
            "job_id": job_id,
            "status": "queued",
            "created_at": created_at,
            "started_at": None,
            "finished_at": None,
            "result": None,
            "error": None,
        }

    background_tasks.add_task(_job_runner, job_id, payload)

    return IngestAcceptedResponse(
        job_id=job_id,
        status="queued",
        message="Ingestao enfileirada. Consulte o status com GET /ingest/yfinance/{job_id}.",
    )


@app.get("/ingest/yfinance/{job_id}", response_model=JobStatusResponse)
def get_job_status(job_id: str) -> JobStatusResponse:
    """Retorna o estado atual de um job de ingestao pelo identificador."""

    with JOB_LOCK:
        job = JOB_STORE.get(job_id)

    if not job:
        raise HTTPException(status_code=404, detail="job_id nao encontrado")

    return JobStatusResponse(**job)
