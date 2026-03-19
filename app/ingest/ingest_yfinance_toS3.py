from __future__ import annotations

import argparse
import io
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import boto3
import pandas as pd
import yfinance as yf

LOGGER = logging.getLogger("ingest_mag7_to_s3")

# Lista fixa de ativos que queremos coletar neste pipeline.
MAG7_TICKERS = ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA"]

# Timezone local para data de execucao e carimbo de ingestao.
LOCAL_TZ = ZoneInfo("America/Sao_Paulo")
# Timezone da bolsa americana para interpretar corretamente o pregrao.
MARKET_TZ = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class Config:
    """
    Configuracao de execucao do pipeline.

    Este contrato e usado tanto na CLI quanto na API, para garantir que ambos
    os caminhos de entrada executem a mesma regra de negocio.
    """

    bucket: str
    prefix: str
    execution_date: date
    aws_region: str
    lookback_days: int


@dataclass(frozen=True)
class IngestionResult:
    """
    Resultado resumido da ingestao.

    Evita expor DataFrame entre camadas e facilita logs, telemetria e retorno
    de API com dados pequenos e estaveis.
    """

    s3_uri: str
    rows_written: int
    partition_date: str
    execution_date: str


def parse_args() -> Config:
    parser = argparse.ArgumentParser(
        description=(
            "Coleta o OHLCV diario das 7 Magnificas via yfinance e salva "
            "em s3://<bucket>/<prefix>/dt=YYYY-MM-DD/*.parquet"
        )
    )
    parser.add_argument("--bucket", required=True, help="Nome do bucket S3 de destino.")
    parser.add_argument(
        "--prefix",
        default="raw",
        help="Prefixo raiz no S3 (default: raw). Ex.: bovespa_data/raw",
    )
    parser.add_argument(
        "--execution-date",
        help="Data de processamento no formato YYYY-MM-DD. Default: hoje (America/Sao_Paulo).",
    )
    parser.add_argument("--aws-region", default="us-east-1", help="Regiao AWS para o cliente S3.")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=10,
        help=(
            "Janela retrospectiva para encontrar o ultimo pregao <= execution-date "
            "(default: 10)."
        ),
    )
    args = parser.parse_args()

    exec_date = (
        date.fromisoformat(args.execution_date)
        if args.execution_date
        else datetime.now(LOCAL_TZ).date()
    )

    return Config(
        bucket=args.bucket,
        prefix=args.prefix.strip("/"),
        execution_date=exec_date,
        aws_region=args.aws_region,
        lookback_days=args.lookback_days,
    )


def _index_to_market_date(index_value: pd.Timestamp) -> date:
    """
    Converte qualquer timestamp para data de mercado em New York.

    Motivo:
    - yfinance pode retornar indice com ou sem timezone;
    - sem normalizacao, a data pode "virar" em timezone local e quebrar
      a logica de selecionar o ultimo pregao valido.
    """

    ts = pd.Timestamp(index_value)
    if ts.tzinfo is None:
        ts = ts.tz_localize(MARKET_TZ)
    else:
        ts = ts.tz_convert(MARKET_TZ)
    return ts.date()


def _fetch_latest_row_for_ticker(ticker: str, cfg: Config) -> pd.DataFrame | None:
    """
    Busca historico diario e retorna apenas a ultima linha valida para um ticker.

    A funcao aplica:
    - janela de busca (lookback);
    - filtro por trade_date <= execution_date;
    - padronizacao de schema/nomes para consumo analitico.
    """

    start = cfg.execution_date - timedelta(days=cfg.lookback_days)
    end = cfg.execution_date + timedelta(days=1)

    hist = yf.Ticker(ticker).history(
        start=start.isoformat(),
        end=end.isoformat(),
        interval="1d",
        auto_adjust=False,
    )
    if hist.empty:
        LOGGER.warning("Sem dados para %s no periodo %s a %s.", ticker, start, end)
        return None

    hist = hist.reset_index().rename(columns={"Date": "trade_timestamp"})
    hist["trade_date"] = hist["trade_timestamp"].apply(_index_to_market_date)
    valid = hist[hist["trade_date"] <= cfg.execution_date].copy()
    if valid.empty:
        LOGGER.warning("Sem pregao valido para %s ate %s.", ticker, cfg.execution_date.isoformat())
        return None

    latest = valid.sort_values("trade_timestamp").tail(1).copy()
    latest["ticker"] = ticker
    latest = latest.rename(
        columns={
            "Open": "open_price",
            "High": "high_price",
            "Low": "low_price",
            "Close": "close_price",
            "Adj Close": "adj_close_price",
            "Volume": "volume",
        }
    )

    # Schema estavel para Glue/Athena e consumidores downstream.
    out = latest[
        [
            "ticker",
            "trade_date",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "adj_close_price",
            "volume",
        ]
    ].copy()

    # Tipagem explicita para evitar inferencia inconsistente entre lotes.
    out["open_price"] = out["open_price"].astype("float64")
    out["high_price"] = out["high_price"].astype("float64")
    out["low_price"] = out["low_price"].astype("float64")
    out["close_price"] = out["close_price"].astype("float64")
    out["adj_close_price"] = out["adj_close_price"].astype("float64")
    out["volume"] = out["volume"].astype("int64")
    return out


def build_dataset(cfg: Config) -> pd.DataFrame:
    """
    Monta dataset consolidado da MAG7 para uma unica execucao.

    Regra atual: uma linha por ticker (ultimo pregao disponivel).
    """

    frames: list[pd.DataFrame] = []
    for ticker in MAG7_TICKERS:
        frame = _fetch_latest_row_for_ticker(ticker, cfg)
        if frame is not None:
            frames.append(frame)

    if not frames:
        raise RuntimeError("Nenhum dado retornado para os tickers da MAG7.")

    data = pd.concat(frames, ignore_index=True)
    data["ingestion_ts"] = datetime.now(tz=LOCAL_TZ).isoformat()
    data["dt"] = data["trade_date"].astype(str)
    return data.sort_values(["trade_date", "ticker"]).reset_index(drop=True)


def upload_parquet_to_s3(df: pd.DataFrame, cfg: Config) -> str:
    """
    Serializa DataFrame em parquet e publica no S3.

    Particionamento adotado:
    - <prefix>/dt=YYYY-MM-DD/mag7_daily_YYYY-MM-DD.parquet

    Observacao:
    - se o lote vier com mais de uma trade_date, mantemos aviso no log e usamos
      a maior data para nome/pasta de particao.
    """

    partition_date = str(df["trade_date"].max())
    distinct_dates = df["trade_date"].astype(str).nunique()
    if distinct_dates > 1:
        LOGGER.warning(
            "Foram encontradas %s datas de pregao no lote; usando dt=%s para particao.",
            distinct_dates,
            partition_date,
        )
    key = f"{cfg.prefix}/dt={partition_date}/mag7_daily_{partition_date}.parquet"

    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    buffer.seek(0)

    s3 = boto3.client("s3", region_name=cfg.aws_region)
    s3.upload_fileobj(buffer, cfg.bucket, key)
    return f"s3://{cfg.bucket}/{key}"


def run_ingestion(cfg: Config) -> IngestionResult:
    """
    Caso de uso principal: coletar dados e fazer upload no S3.

    Esta funcao e o ponto unico de execucao. Qualquer gatilho (CLI, API,
    scheduler) deve chamar este metodo para evitar divergencia funcional.
    """

    LOGGER.info("Iniciando ingestao MAG7 para dt=%s", cfg.execution_date.isoformat())
    df = build_dataset(cfg)
    s3_uri = upload_parquet_to_s3(df, cfg)
    LOGGER.info("Upload concluido: %s", s3_uri)
    LOGGER.info("Linhas gravadas: %s", len(df))

    return IngestionResult(
        s3_uri=s3_uri,
        rows_written=len(df),
        partition_date=str(df["trade_date"].max()),
        execution_date=cfg.execution_date.isoformat(),
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    cfg = parse_args()
    run_ingestion(cfg)


if __name__ == "__main__":
    main()
