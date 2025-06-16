import yfinance as yf
import pandas as pd
import os
import time
from datetime import datetime, timedelta


def download_all_stock_data(
    tickers,
    start_date,
    end_date,
    chunk_size=100,
    delay_seconds=30,
    max_retries=5,
    retry_delay_seconds=600,
    output_dir="./stock_data_chunks",
):
    os.makedirs(output_dir, exist_ok=True)
    chunks = [tickers[i : i + chunk_size] for i in range(0, len(tickers), chunk_size)]

    for i, chunk in enumerate(chunks):
        retries = 0
        success = False
        while retries < max_retries and not success:
            try:
                chunk_data = yf.download(
                    chunk,
                    start=start_date,
                    end=end_date,
                    interval="1d",
                    group_by="ticker",
                )
                if not chunk_data.empty:
                    chunk_file_path = os.path.join(
                        output_dir, f"stock_data_chunk_{i + 1}.parquet"
                    )
                    chunk_data.to_parquet(
                        chunk_file_path, engine="pyarrow", compression="snappy"
                    )
                success = True
            except Exception as e:
                print(str(e))
                retries += 1
                if "429" in str(e) or "rate limit" in str(e).lower():
                    time.sleep(retry_delay_seconds)
                else:
                    time.sleep(delay_seconds)

        if not success:
            pass
        if i < len(chunks) - 1:
            time.sleep(delay_seconds)

    combined_df = pd.DataFrame()
    for file_name in os.listdir(output_dir):
        if file_name.endswith(".parquet"):
            file_path = os.path.join(output_dir, file_name)
            try:
                df_chunk = pd.read_parquet(file_path, engine="pyarrow")
                if not df_chunk.empty:
                    combined_df = pd.concat([combined_df, df_chunk], axis=1)
            except Exception as e:
                pass

    if not combined_df.empty:
        combined_df = combined_df.loc[:, ~combined_df.columns.duplicated()]
    return combined_df


def update_stock_data(df, file_name="stock_data.parquet"):
    df.to_parquet(file_name, engine="pyarrow", compression="snappy")


def get_all_us_tickers_from_nasdaq_ftp_files_compact(
    nasdaq_file="nasdaqlisted.txt", other_file="otherlisted.txt"
):
    all_tickers = []
    if os.path.exists(nasdaq_file):
        df_nasdaq = pd.read_csv(
            nasdaq_file, sep="|", header=0, skipfooter=1, engine="python"
        )
        nasdaq_tickers = df_nasdaq.iloc[:, 0].tolist()
        all_tickers.extend(nasdaq_tickers)

    if os.path.exists(other_file):
        df_other = pd.read_csv(
            other_file, sep="|", header=0, skipfooter=1, engine="python"
        )
        other_tickers = (
            df_other["ACT Symbol"].tolist()
            if "ACT Symbol" in df_other.columns
            else df_other.iloc[:, 3].tolist()
        )
        all_tickers.extend(other_tickers)

    all_tickers = [
        ticker.strip()
        for ticker in all_tickers
        if isinstance(ticker, str)
        and ticker.strip() != ""
        and not ticker.strip().lower().startswith(("symbol", "act symbol"))
    ]
    unique_tickers = sorted(list(set(all_tickers)))
    return unique_tickers


if __name__ == "__main__":
    df = pd.read_parquet("./all_stock_data_15y_daily.parquet", engine="pyarrow")
    print(df.shape, df.head)
