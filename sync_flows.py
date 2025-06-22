# Файл: sync_flows.py

import pandas as pd
import gspread
import psycopg2
import json
import re
import datetime
from typing import Callable

from prefect import flow, task
from prefect.blocks.system import JSON
from oauth2client.service_account import ServiceAccountCredentials
from psycopg2.extras import execute_values

# --- ОБЩИЕ ЗАДАЧИ (TASKS) ---

@task(log_prints=True, retries=2, retry_delay_seconds=10)
def load_gsheet_task(spreadsheet_id: str, worksheet_gid: int) -> pd.DataFrame:
    """Загружает данные из Google Sheet, используя секреты из блока 'google-creds'."""
    print(f"Загрузка из GSheet ID: {spreadsheet_id}, GID: {worksheet_gid}")
    
    google_creds_block = JSON.load("google-creds")
    creds_dict = google_creds_block.value
    
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scopes)
    gc = gspread.authorize(credentials)

    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.get_worksheet_by_id(worksheet_gid)
    
    data = ws.get_all_records(head=1, default_blank="")
    df = pd.DataFrame(data)
    df.replace("", None, inplace=True)
    
    print(f"Загружено {len(df)} строк.")
    return df

@task(log_prints=True)
def write_to_db_task(df: pd.DataFrame, table_name: str, use_drop_create: bool = False):
    """
    Записывает DataFrame в PostgreSQL, используя секреты из блока 'db-creds'.
    Может работать в двух режимах: TRUNCATE (по умолчанию) или DROP+CREATE.
    """
    if df.empty:
        print(f"DataFrame для таблицы '{table_name}' пуст. Пропуск записи в БД.")
        return

    print(f"Запись {len(df)} строк в таблицу '{table_name}'...")
    
    db_creds_block = JSON.load("db-creds")
    cfg = db_creds_block.value

    conn = psycopg2.connect(**cfg)
    with conn.cursor() as cur:
        # Авто-DDL
        cols_ddl = []
        for col_name, dtype in df.dtypes.items():
            safe_col_name = f'"{col_name}"'
            sql_type = 'TEXT'

            if pd.api.types.is_datetime64_any_dtype(dtype):
                sql_type = 'TIMESTAMP WITH TIME ZONE'
            elif pd.api.types.is_float_dtype(dtype):
                sql_type = 'DECIMAL(12, 4)' if 'fee' in col_name.lower() or 'price' in col_name.lower() or 'cost' in col_name.lower() else 'DOUBLE PRECISION'
            elif pd.api.types.is_integer_dtype(dtype):
                sql_type = 'BIGINT'
            elif pd.api.types.is_bool_dtype(dtype):
                sql_type = 'BOOLEAN'
            elif str(dtype) == 'object' and not df[col_name].dropna().empty and isinstance(df[col_name].dropna().iloc[0], datetime.date):
                sql_type = 'DATE'

            cols_ddl.append(f'{safe_col_name} {sql_type}')
        
        if use_drop_create:
            print(f"Режим DROP+CREATE: Удаление старой таблицы (если существует) '{table_name}'...")
            cur.execute(f'DROP TABLE IF EXISTS "{table_name}";')
        
        ddl = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(cols_ddl)});'
        print(f"DDL для таблицы:\n{ddl}")
        cur.execute(ddl)

        if not use_drop_create:
            print(f"Режим TRUNCATE: Очистка таблицы '{table_name}'...")
            cur.execute(f'TRUNCATE TABLE "{table_name}" RESTART IDENTITY;')
        
        # Вставка
        cols = [f'"{c}"' for c in df.columns]
        query = f"INSERT INTO \"{table_name}\" ({', '.join(cols)}) VALUES %s"
        
        values = [tuple(None if pd.isna(y) else (y.date() if isinstance(y, pd.Timestamp) else y) for y in x) for x in df.itertuples(index=False, name=None)]
        
        execute_values(cur, query, values, page_size=1000)

    conn.commit()
    conn.close()
    print(f"Успешно записано в таблицу '{table_name}'.")

# --- ЗАДАЧИ ПРЕДОБРАБОТКИ (PREPROCESSING TASKS) ---

@task(log_prints=True)
def preprocess_catalog_data(df: pd.DataFrame) -> pd.DataFrame:
    """Предобработка для данных 'catalog'."""
    if df.empty: return df
    print("Предобработка данных 'catalog'...")
    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
    if '_fivetran_synced' in df.columns:
        df['_fivetran_synced'] = pd.to_datetime(df['_fivetran_synced'], errors='coerce')
    if 'referral_fee' in df.columns:
        fee_str = df['referral_fee'].astype(str).str.replace('%', '', regex=False).str.strip()
        df['referral_fee'] = pd.to_numeric(fee_str, errors='coerce') / 100.0
    return df

@task(log_prints=True)
def preprocess_promo_data(df: pd.DataFrame) -> pd.DataFrame:
    """Предобработка для данных 'promo'."""
    if df.empty: return df
    print("Предобработка данных 'promo'...")
    df.rename(columns={'Start date': 'start_date', 'End date': 'end_date'}, inplace=True)
    for col in ['start_date', 'end_date']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    return df

@task(log_prints=True)
def preprocess_cogs_data(df: pd.DataFrame) -> pd.DataFrame:
    """Предобработка для данных 'cogs'."""
    if df.empty: return df
    print("Предобработка данных 'cogs'...")
    df.rename(columns={'Date Start': 'DateStart', 'Date End': 'DateEnd'}, inplace=True)
    for col in ['shipping', 'production', 'COGs_total']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace('$', '', regex=False), errors='coerce')
    for date_col in ['DateStart', 'DateEnd']:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    return df

@task(log_prints=True)
def preprocess_mediaplan_deals_data(df: pd.DataFrame) -> pd.DataFrame:
    """Предобработка для данных 'mediaplan_deals'."""
    if df.empty: return df
    print("Предобработка данных 'mediaplan_deals'...")
    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
    if 'id' in df.columns:
        df['id'] = pd.to_numeric(df['id'], errors='coerce')
    for price_col in ['plan_price', 'fact_price']:
        if price_col in df.columns:
            df[price_col] = pd.to_numeric(df[price_col].astype(str).str.replace(',', '.', regex=False), errors='coerce')
    for date_col in ['plan_date', 'fact_date']:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], format='%d.%m.%Y', errors='coerce')
    return df
    
@task(log_prints=True)
def preprocess_mediaplan_affl_data(df: pd.DataFrame) -> pd.DataFrame:
    """Предобработка для данных 'infl_affl'."""
    if df.empty: return df
    print("Предобработка данных 'infl_affl'...")
    df.rename(columns={c: c.lower().strip() for c in df.columns}, inplace=True)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], format='%d.%m.%Y', errors='coerce')
    if 'asin' in df.columns:
        df['asin'] = df['asin'].astype(str).str.strip().replace(['nan', 'None'], None)
    if 'cost' in df.columns:
        df['cost'] = pd.to_numeric(df['cost'].astype(str).str.replace('$', '').str.replace(' ', '').str.replace(',', '.'), errors='coerce')
    return df

# --- ГЛАВНЫЙ ПОТОК (MASTER FLOW) ---

@flow(name="Generic GSheet Sync")
def gsheet_to_db_flow(
    spreadsheet_id: str, 
    worksheet_gid: int, 
    table_name: str, 
    preprocess_task: Callable,
    use_drop_create: bool
):
    """Универсальный поток для синхронизации."""
    raw_df = load_gsheet_task(spreadsheet_id, worksheet_gid)
    processed_df = preprocess_task(raw_df)
    write_to_db_task(processed_df, table_name, use_drop_create)

# --- ПОТОКИ-ОБЕРТКИ ДЛЯ КАЖДОЙ ТАБЛИЦЫ ---

@flow(name="Sync Catalog Table")
def sync_catalog_flow():
    gsheet_to_db_flow(
        spreadsheet_id="1bvpoIrqsgAlUiVOvAMBunelDM8JqmFj_x8V6jLElFpk",
        worksheet_gid=960225775,
        table_name="catalog",
        preprocess_task=preprocess_catalog_data,
        use_drop_create=True
    )

@flow(name="Sync Promo Table")
def sync_promo_flow():
    gsheet_to_db_flow(
        spreadsheet_id="1R_hC4wzTBDTVb_Dcg4vodbCmHGsiJJiz7pd8mGKrXK8",
        worksheet_gid=0,
        table_name="promo",
        preprocess_task=preprocess_promo_data,
        use_drop_create=False
    )

@flow(name="Sync COGS Table")
def sync_cogs_flow():
    gsheet_to_db_flow(
        spreadsheet_id="1dN-ChcCFus5Jvw2Obes3aERIszF7jLTl8TQ83IjtQAE",
        worksheet_gid=0,
        table_name="cogs",
        preprocess_task=preprocess_cogs_data,
        use_drop_create=False
    )
    
@flow(name="Sync Mediaplan Deals Table")
def sync_mediaplan_deals_flow():
    gsheet_to_db_flow(
        spreadsheet_id="1_0E9V1EGjk_F1iAuD1cQlMWAeu9OXHSeo-NVjbx2Ycg",
        worksheet_gid=1417701970,
        table_name="mediaplan_deals",
        preprocess_task=preprocess_mediaplan_deals_data,
        use_drop_create=True
    )

@flow(name="Sync Mediaplan Affiliate Table")
def sync_mediaplan_affl_flow():
    gsheet_to_db_flow(
        spreadsheet_id="1_0E9V1EGjk_F1iAuD1cQlMWAeu9OXHSeo-NVjbx2Ycg",
        worksheet_gid=1356798291,
        table_name="infl_affl",
        preprocess_task=preprocess_mediaplan_affl_data,
        use_drop_create=False
    )

if __name__ == "__main__":
    # Запуск одного из потоков для локального теста
    # Убедитесь, что у вас установлены блоки google-creds и db-creds
    sync_catalog_flow()
