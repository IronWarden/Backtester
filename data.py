import yfinance as yf
import pandas as pd
import os
import time
import sys
from datetime import datetime, timedelta
from fredapi import Fred
from dotenv import load_dotenv
import duckdb
import ccxt


load_dotenv()


def fetch_and_store_crypto_data(db_path="stock_data.db"):
    """
    Fetches OHLCV data for top cryptocurrencies from Yahoo Finance,
    transforms it into a long format, and stores it in a DuckDB database.
    This new format is clean and easily queryable.

    Args:
        db_path (str): The path to the DuckDB database file.
    """
    crypto_symbols = ["BTC-USD", "ETH-USD", "BNB-USD", "SOL-USD", "XRP-USD"]

    try:
        start_date = "2010-01-01"
        end_date = datetime.today().strftime("%Y-%m-%d")

        print(f"Fetching data for {len(crypto_symbols)} cryptocurrencies...")

        # Fetch all data in one go, resulting in a wide DataFrame
        # with multi-level columns (e.g., ('Open', 'BTC-USD'))
        wide_df = yf.download(crypto_symbols, start=start_date, end=end_date)

        if wide_df.empty:
            print("No data returned from yfinance.")
            return

        # --- Transform from Wide to Long Format ---
        # Stack the DataFrame to convert multi-level columns into rows
        long_df = wide_df.stack(level=1)

        # Reset the index to turn 'Date' and 'Ticker' from index to columns
        long_df.reset_index(inplace=True)

        # Rename the columns for clarity and simplicity
        long_df.rename(
            columns={
                "level_1": "Ticker",
                "Adj Close": "Adj_Close",  # DuckDB doesn't like spaces in names
            },
            inplace=True,
        )

        # Ensure 'Adj Close' exists, if not, create it
        if "Adj_Close" not in long_df.columns:
            long_df["Adj_Close"] = float("nan")

        # --- Final Column Selection and Ordering ---
        # Select and reorder columns to the desired final schema
        final_columns = [
            "Date",
            "Ticker",
            "Open",
            "High",
            "Low",
            "Close",
            "Adj_Close",
            "Volume",
        ]
        long_df = long_df[final_columns]

        print(
            f"Successfully transformed data into long format. Total records: {len(long_df)}"
        )

        # --- Store in DuckDB ---
        conn = duckdb.connect(db_path)
        table_name = "crypto_ohlcv"

        # Create or replace the table with the clean, long-format data
        conn.sql(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM long_df")

        print(f"Successfully stored data in table '{table_name}' in '{db_path}'.")

        # --- Verification ---
        print("\nSample data from DuckDB:")
        print(conn.execute(f"SELECT * FROM {table_name} LIMIT 10").fetchdf())
        print("\nSchema from DuckDB:")
        print(conn.execute(f"DESCRIBE {table_name}").fetchdf())

        conn.close()

    except Exception as e:
        print(f"An error occurred: {e}")


def fetch_and_save_treasury_yield(
    series_id: str,
    output_dir: str = ".",
    annualizing_factor: int = 252,
    start_date: str = "2010-06-17",
) -> str | None:
    """
    Fetches historical daily U.S. Treasury yield data from FRED,
    converts it to daily decimal rates, and saves it as a Parquet file.

    Args:
        series_id (str): The FRED series ID for the desired Treasury yield.
                         Examples: 'DGS3MO' (3-Month), 'DGS1YR' (1-Year), 'DGS10' (10-Year).
        output_dir (str): Directory where the Parquet file will be saved.
                          Defaults to the current directory.
        annualizing_factor (int): The factor to convert annual yields to daily rates.
                                  252 for trading days, 365 for calendar days.
                                  Defaults to 252.
        start_date (str): The start date for fetching data in 'YYYY-MM-DD' format.
                          Defaults to '2010-01-01'.

    Returns:
        str | None: The path to the saved Parquet file if successful, otherwise None.
    """
    # Ensure the FRED API key is available
    fred_api_key = os.environ.get("FRED_API_KEY")
    if not fred_api_key:
        print("Error: FRED_API_KEY environment variable not set.")
        print(
            "Please obtain a free API key from https://fred.stlouisfed.org/docs/api/api_key.html"
        )
        print("Then set it: export FRED_API_KEY='your_key_here'")
        return None

    fred = Fred(api_key=fred_api_key)

    # Define the end date as today
    end_date = "2025-06-17"

    print(
        f"Attempting to fetch FRED series: '{series_id}' from {start_date} to {end_date}..."
    )

    try:
        # Fetch the series
        # get_series returns a pandas Series with dates as index
        treasury_yield_series = fred.get_series(
            series_id, observation_start=start_date, observation_end=end_date
        )

        if treasury_yield_series.empty:
            print(
                f"No data found for FRED series '{series_id}' in the specified range."
            )
            print(
                f"Please check if '{series_id}' is a valid and available series ID for the dates."
            )
            return None

        # Convert Series to DataFrame and rename the value column
        df = treasury_yield_series.to_frame(name="annual_yield_percent")

        # FRED yields are typically in percentage points (e.g., 5.00 for 5%)
        # Convert to daily decimal rate
        # R_daily = (1 + R_annual)^(1/annualizing_factor) - 1
        df["daily_risk_free_rate_decimal"] = (1 + df["annual_yield_percent"] / 100) ** (
            1 / annualizing_factor
        ) - 1

        # Reset index to make 'Date' a regular column for clarity in Parquet
        df.index.name = "Date"
        df = df.reset_index()

        # Ensure the output directory exists
        os.makedirs(output_dir, exist_ok=True)

        # Define output file path
        output_filepath = os.path.join(output_dir, f"{series_id}_daily_rates.parquet")

        # Save to Parquet
        df.to_parquet(
            output_filepath, index=False, engine="pyarrow", compression="snappy"
        )

        print(f"Successfully fetched and saved '{series_id}' data.")
        print(f"File saved to: {output_filepath}")
        print("DataFrame head:")
        print(df.head())
        print("DataFrame tail:")
        print(df.tail())

        return output_filepath

    except Exception as e:
        print(
            f"An error occurred while fetching or processing data for '{series_id}': {e}"
        )
        return None


# Configuration for retry mechanism
MAX_RETRY_SECONDS = 300  # Maximum time to keep retrying (5 minutes)
RETRY_DELAY_SECONDS = 60  # Delay between retries for rate limiting


def _fetch_data_with_retry(ticker_obj, data_attribute_name, ticker_symbol):
    """
    Attempts to fetch a specific data attribute from a yfinance Ticker object.
    It will only retry on rate-limit related HTTP errors (like 429).
    For other errors, it will log the issue and continue without retrying.

    Args:
        ticker_obj (yf.Ticker): The yfinance Ticker object.
        data_attribute_name (str): The name of the attribute to fetch (e.g., 'info', 'financials').
        ticker_symbol (str): The ticker symbol, for logging purposes.

    Returns:
        pd.DataFrame or dict or pd.Series or None: The fetched data, or None if unsuccessful.
    """
    start_time = time.time()
    attempts = 0
    while (time.time() - start_time) < MAX_RETRY_SECONDS:
        attempts += 1
        try:
            data = getattr(ticker_obj, data_attribute_name)
            if (
                data is not None
                and (isinstance(data, (pd.DataFrame, pd.Series)) and not data.empty)
                or (isinstance(data, dict) and data)
            ):
                return data
            # If data is an empty DataFrame/Series or None, it means no data was found.
            # This is not an error, so we return it as is without retrying.
            return data
        except Exception as e:
            # Check if the error message contains rate-limit status codes
            error_str = str(e).lower()
            if "429" in error_str or "rate limit" in error_str:
                print(
                    f"  Attempt {attempts}: Rate limit hit for {ticker_symbol}.{data_attribute_name}. Retrying in {RETRY_DELAY_SECONDS}s..."
                )
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                # For any other error, log it and stop retrying
                print(
                    f"  Error fetching {ticker_symbol}.{data_attribute_name}: {e}. Not retrying."
                )
                return None
    print(
        f"  Failed to fetch {ticker_symbol}.{data_attribute_name} after {MAX_RETRY_SECONDS} seconds due to persistent rate limiting."
    )
    return None


def fetch_and_store_non_ohlcv_data(
    tickers, db_path="stock_data.db", output_directory="yfinance_non_ohlcv_data"
):
    """
    Fetches various non-OHLCV data for a list of stock tickers using yfinance
    and stores them as separate tables in a DuckDB database.

    Args:
        tickers (list): A list of stock ticker symbols (e.g., ['AAPL', 'MSFT']).
        db_path (str): The path to the DuckDB database file.
        output_directory (str): The directory where Parquet files will be saved.
                                Defaults to 'yfinance_data'.
    """

    print(f"Starting data fetching for {len(tickers)} tickers...")

    # Create output directory if it doesn't exist
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
        print(f"Created output directory: {output_directory}")

    conn = duckdb.connect(db_path) # Open connection once

    try:
        for ticker_symbol in tickers:
            print(f"--- Processing {ticker_symbol} ---")
            try:
                ticker = yf.Ticker(ticker_symbol)

                # --- 1. General Company Information (.info) ---
                info = _fetch_data_with_retry(ticker, "info", ticker_symbol)
                if info:
                    info_df = pd.DataFrame([info])
                    info_df["Ticker"] = ticker_symbol

                    # Ensure the table exists with a basic schema if not already
                    conn.execute("CREATE TABLE IF NOT EXISTS company_info (Ticker VARCHAR, PRIMARY KEY (Ticker))")

                    # Get existing columns from the table
                    existing_columns_info = conn.execute("PRAGMA table_info('company_info');").fetchall()
                    existing_column_names = {col[1] for col in existing_columns_info}

                    # Identify new columns in info_df that are not in the table
                    new_columns_to_add = []
                    for col in info_df.columns:
                        if col not in existing_column_names:
                            # Attempt to infer a DuckDB type, default to VARCHAR
                            duckdb_type = "VARCHAR"
                            if pd.api.types.is_integer_dtype(info_df[col]):
                                duckdb_type = "BIGINT"
                            elif pd.api.types.is_float_dtype(info_df[col]):
                                duckdb_type = "DOUBLE"
                            elif pd.api.types.is_bool_dtype(info_df[col]):
                                duckdb_type = "BOOLEAN"
                            elif pd.api.types.is_datetime64_any_dtype(info_df[col]):
                                duckdb_type = "TIMESTAMP"
                            new_columns_to_add.append((col, duckdb_type))

                    # Add new columns to the table
                    for col_name, col_type in new_columns_to_add:
                        try:
                            conn.execute(f"ALTER TABLE company_info ADD COLUMN {col_name} {col_type}")
                            print(f"Added column '{col_name}' ({col_type}) to company_info table.")
                        except Exception as e:
                            print(f"Error adding column '{col_name}': {e}")
                            # Fallback to VARCHAR if specific type fails
                            try:
                                conn.execute(f"ALTER TABLE company_info ADD COLUMN "{col_name}" VARCHAR")
                                print(f"Added column '{col_name}' as VARCHAR to company_info table.")
                            except Exception as e_varchar:
                                print(f"Critical error: Could not add column '{col_name}' even as VARCHAR: {e_varchar}")
                                continue # Skip this column if it can't be added

                    # Filter info_df to only include columns that exist in the table
                    # After adding new columns, re-fetch existing column names
                    existing_columns_info_after_alter = conn.execute("PRAGMA table_info('company_info');").fetchall()
                    final_existing_column_names = {col[1] for col in existing_columns_info_after_alter}

                    cols_to_insert = [col for col in info_df.columns if col in final_existing_column_names]
                    info_df_filtered = info_df[cols_to_insert]

                    # Insert data using BY NAME, which should now work as schemas align
                    # Use a temporary view for the DataFrame to insert
                    conn.create_view("temp_info_df_view", info_df_filtered)
                    conn.execute(f"INSERT INTO company_info BY NAME SELECT * FROM temp_info_df_view")
                    conn.drop_view("temp_info_df_view") # Clean up temporary view

                    print(f"Fetched general info for {ticker_symbol}.")
                else:
                    print(f"No general info found for {ticker_symbol}.")

                datasets_to_fetch = {
                    "annual_income_statement": "financials",
                    "annual_balance_sheet": "balance_sheet",
                    "annual_cash_flow": "cashflow",
                    "quarterly_income_statement": "quarterly_financials",
                    "quarterly_balance_sheet": "quarterly_balance_sheet",
                    "quarterly_cash_flow": "quarterly_cashflow",
                    "institutional_holders": "institutional_holders",
                    "major_holders": "major_holders",
                    "dividends": "dividends",
                    "splits": "splits",
                    "earnings_calendar": "calendar",
                }

                # Use the existing connection, no need to open again

                for name, attribute in datasets_to_fetch.items():
                    data = _fetch_data_with_retry(ticker, attribute, ticker_symbol)

                    if data is not None and not (
                        isinstance(data, (pd.DataFrame, pd.Series)) and data.empty
                    ):
                        if isinstance(data, dict):
                            data = pd.Series(data).to_frame().T
                        elif isinstance(data, pd.Series):
                            data = data.to_frame(name=name.split("_")[-1].capitalize())

                        data["Ticker"] = ticker_symbol

                        # Reshape financial data
                        if (
                            "income_statement" in name
                            or "balance_sheet" in name
                            or "cash_flow" in name
                        ):
                            id_vars = [
                                col
                                for col in data.columns
                                if col.lower()
                                in ["ticker", "as of date", "period type", "currency code"]
                            ]
                            if "Ticker" in data.index.names:
                                data = data.reset_index()
                                id_vars.append("Ticker")
                            if not any(
                                x in data.columns for x in ["as of date", "period type"]
                            ):
                                data = data.reset_index().rename(
                                    columns={"index": "Metric"}
                                )
                                id_vars.append("Metric")

                            value_vars = [col for col in data.columns if col not in id_vars]
                            long_df = data.melt(
                                id_vars=id_vars,
                                value_vars=value_vars,
                                var_name="Date",
                                value_name="Value",
                            )
                            long_df["Date"] = pd.to_datetime(
                                long_df["Date"], errors="coerce"
                            )
                            long_df.dropna(subset=["Date"], inplace=True)
                            final_df = long_df
                        else:
                            final_df = data

                        conn.execute(
                            f'CREATE TABLE IF NOT EXISTS "{name}" (Ticker VARCHAR, Date TIMESTAMP, Value VARCHAR)'
                        )
                        conn.execute(f'INSERT INTO "{name}" SELECT * FROM final_df')
                        print(
                            f"  Successfully fetched and saved {name} for {ticker_symbol}."
                        )
                    else:
                        print(f"  No {name} data found for {ticker_symbol}.")

            except KeyboardInterrupt:
                print("\nInterrupted by user. Saving progress and exiting...")
                raise # Re-raise the exception to ensure the script stops
            except Exception as e:
                print(f"Failed to process ticker {ticker_symbol}: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("DuckDB connection closed.")


def download_all_stock_data(
    tickers,
    start_date,
    end_date,
    chunk_size=100,
    delay_seconds=10,
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


def create_table_from_parquet_duckdb(
    duckdb_path: str, parquet_file_path: str, table_name: str
) -> bool:
    # ... (previous code) ...

    try:
        con = duckdb.connect(database=db_path)
        print(f"Connected to DuckDB: {duckdb_path}")

        # FIX: Double-quote the table_name in the SQL query
        query = f"CREATE OR REPLACE TABLE \"{table_name}\" AS SELECT * FROM '{parquet_file_path}';"

        print(f"Executing SQL: {query}")

        con.execute(query)

        row_count = con.execute(f'SELECT COUNT(*) FROM "{table_name}";').fetchone()[
            0
        ]  # Also quote here!
        print(
            f"Successfully loaded data from '{parquet_file_path}' into DuckDB table '{table_name}'."
        )
        print(f"Table '{table_name}' now contains {row_count} rows.")

        return True

    except duckdb.DuckDBError as e:
        print(
            f"DuckDB Error: Failed to create table '{table_name}' from '{parquet_file_path}': {e}"
        )
        return False
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return False
    finally:
        if "con" in locals() and con is not None:
            con.close()
            print(f"DuckDB connection to {db_path} closed.")


def copy_crypto_to_stock_data(db_path="stock_data.db"):
    """
    Copies data from the crypto_ohlcv table to the stock_data table,
    mapping columns correctly between the two schemas.
    """
    source_table = "crypto_ohlcv"
    destination_table = "stock_data"

    try:
        con = duckdb.connect(database=db_path)
        print(
            f"Copying data from '{source_table}' to '{destination_table}' with schema mapping..."
        )

        # Explicit column mapping to handle schema differences
        query = f"""
        INSERT INTO {destination_table} (Date, Ticker, Open, High, Low, "Close", Volume, "Adj Close")
        SELECT Date, Ticker, Open, High, Low, "Close", Volume, Adj_Close
        FROM {source_table};
        """

        con.execute(query)

        dest_rows = con.execute(f"SELECT COUNT(*) FROM {destination_table}").fetchone()[
            0
        ]
        print(
            f"Success! The destination table '{destination_table}' now contains {dest_rows} rows."
        )

    except duckdb.Error as e:
        print(f"A DuckDB error occurred: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if "con" in locals():
            con.close()


def remove_duplicate_stock_data(db_path="stock_data.db"):
    """
    Removes duplicate rows from the stock_data table based on Ticker and Date.
    """
    table_name = "stock_data"
    try:
        con = duckdb.connect(database=db_path)
        print(f"Removing duplicate data from '{table_name}'...")

        # Create a new table with distinct rows
        query = f"""
        CREATE OR REPLACE TABLE {table_name}_dedup AS
        SELECT DISTINCT *
        FROM {table_name};
        """
        con.execute(query)

        # Drop the old table
        con.execute(f"DROP TABLE {table_name}")

        # Rename the new table
        con.execute(f"ALTER TABLE {table_name}_dedup RENAME TO {table_name}")

        print(f"Successfully removed duplicates from '{table_name}'.")

    except duckdb.Error as e:
        print(f"A DuckDB error occurred: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if "con" in locals():
            con.close()


if __name__ == "__main__":
    processed_tickers_file = "processed_tickers.txt"

    def load_processed_tickers():
        if os.path.exists(processed_tickers_file):
            with open(processed_tickers_file, "r") as f:
                return set(f.read().splitlines())
        return set()

    def save_processed_tickers(tickers):
        with open(processed_tickers_file, "a") as f:
            for ticker in tickers:
                f.write(ticker + "\n")

    all_tickers = get_all_us_tickers_from_nasdaq_ftp_files_compact()
    processed_tickers = load_processed_tickers()
    tickers_to_process = [t for t in all_tickers if t not in processed_tickers]

    batch_size = 500
    batches = [
        tickers_to_process[i : i + batch_size]
        for i in range(0, len(tickers_to_process), batch_size)
    ]

    try:
        for i, batch in enumerate(batches):
            print(f"--- Processing Batch {i + 1}/{len(batches)} ---")
            fetch_and_store_non_ohlcv_data(batch)
            save_processed_tickers(batch)
            print(f"--- Finished Batch {i + 1}/{len(batches)} ---")
    except KeyboardInterrupt:
        print("\nInterrupted by user. Exiting...")

    print("All tickers processed!")
