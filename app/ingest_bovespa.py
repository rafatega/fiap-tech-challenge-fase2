import io
from datetime import datetime, timezone
from typing import List

import boto3
import pandas as pd
import yfinance as yf
from botocore.exceptions import BotoCoreError, ClientError


AWS_REGION = "us-east-1"
BUCKET_NAME = "bovespa-data"

TICKERS = [
    "PETR4.SA",
    "VALE3.SA",
    "ITUB4.SA",
]


def download_ticker_data(ticker: str, period: str = "1mo", interval: str = "1d") -> pd.DataFrame:
    """
    Baixa dados históricos de um ticker da B3 usando yfinance.
    """
    ticker_obj = yf.Ticker(ticker)
    df = ticker_obj.history(
        period=period, interval=interval, auto_adjust=False)

    if df is None or df.empty:
        raise ValueError(f"Nenhum dado retornado para o ticker {ticker}")

    df = df.reset_index()

    # Padronização de nomes
    df.columns = [str(col).strip().lower().replace(" ", "_")
                  for col in df.columns]

    # Algumas versões podem retornar "stock_splits" e "dividends", o que é normal
    rename_map = {
        "datetime": "date",
    }
    df = df.rename(columns=rename_map)

    required_columns = {"date", "open", "high", "low", "close", "volume"}
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(
            f"Colunas obrigatórias ausentes para {ticker}: {missing}. "
            f"Colunas encontradas: {list(df.columns)}"
        )

    ticker_clean = ticker.replace(".SA", "")
    now_utc = datetime.now(timezone.utc)

    df["ticker"] = ticker_clean
    df["source"] = "yfinance"
    df["ingestion_date"] = now_utc.strftime("%Y-%m-%d")
    df["ingestion_timestamp_utc"] = now_utc.isoformat()

    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)

    final_columns = [
        "date",
        "ticker",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "dividends" if "dividends" in df.columns else None,
        "stock_splits" if "stock_splits" in df.columns else None,
        "source",
        "ingestion_date",
        "ingestion_timestamp_utc",
    ]
    final_columns = [col for col in final_columns if col is not None]

    return df[final_columns]


def upload_dataframe_to_s3_parquet(
    df: pd.DataFrame,
    bucket_name: str,
    ticker: str,
    region_name: str = AWS_REGION,
) -> str:
    """
    Salva o DataFrame em parquet e envia para o bucket S3.
    """
    s3_client = boto3.client("s3", region_name=region_name)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    ticker_clean = ticker.replace(".SA", "")

    s3_key = f"raw/date={today}/ticker={ticker_clean}/dados.parquet"

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream",
        )
    except (BotoCoreError, ClientError) as exc:
        raise RuntimeError(f"Erro ao enviar para o S3: {exc}") from exc

    return f"s3://{bucket_name}/{s3_key}"


def process_ticker(ticker: str, bucket_name: str) -> None:
    print(f"\nBaixando dados de {ticker}...")
    df = download_ticker_data(ticker=ticker, period="1mo", interval="1d")

    print(f"Total de registros para {ticker}: {len(df)}")
    print(df.head())

    s3_uri = upload_dataframe_to_s3_parquet(
        df=df,
        bucket_name=bucket_name,
        ticker=ticker,
    )
    print(f"Upload concluído: {s3_uri}")


def main(tickers: List[str], bucket_name: str) -> None:
    for ticker in tickers:
        try:
            process_ticker(ticker=ticker, bucket_name=bucket_name)
        except Exception as exc:
            print(f"Falha ao processar {ticker}: {exc}")


if __name__ == "__main__":
    main(tickers=TICKERS, bucket_name=BUCKET_NAME)
