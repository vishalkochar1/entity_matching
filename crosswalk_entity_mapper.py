#!/usr/bin/env python3
"""
Crosswalk Entity Mapper
----------------------
Extract ALL columns from Pitchbook COMPANY_COMMON and Voldemort VOLDEMORT_FIRMOGRAPHICS tables
for each pair of IDs in FORAGE.VOLDEMORT_PITCHBOOK_CROSSWALK_TO_VERIFY.
Optimized for bulk queries.
"""

import logging
import pandas as pd
import snowflake.connector
import sys
from datetime import datetime

# Hardcoded Snowflake credentials
SNOWFLAKE_ACCOUNT = "TWHRMQQ-EIA98922"
SNOWFLAKE_USER = "JEHAN"
SNOWFLAKE_PASSWORD = "qR9#kL8@xP2m"
SNOWFLAKE_WAREHOUSE = "FORAGE_AI_WH"
SNOWFLAKE_ROLE = "FORAGE_AI_USER"

# Output filename with today's date
OUTPUT_FILE = f"crosswalk_entity_mapping_{datetime.now().strftime('%Y%m%d')}.csv"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("entity_mapper.log"),
        logging.StreamHandler()
    ]
)

def connect_to_snowflake():
    try:
        logging.info(f"Connecting to Snowflake with account: {SNOWFLAKE_ACCOUNT}, user: {SNOWFLAKE_USER}, warehouse: {SNOWFLAKE_WAREHOUSE}")
        conn = snowflake.connector.connect(
            account=SNOWFLAKE_ACCOUNT,
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database='PROD',
            role=SNOWFLAKE_ROLE,
            client_session_keep_alive=True,
            application='EntityMapper'
        )
        cursor = conn.cursor()
        cursor.execute("SELECT current_version()")
        version = cursor.fetchone()[0]
        logging.info(f"Connected to Snowflake successfully. Version: {version}")
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to Snowflake: {e}")
        raise

def execute_query(conn, query, description=None):
    try:
        if description:
            logging.info(f"Executing query: {description}")
        logging.debug(f"Full query: {query}")
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)
        logging.info(f"Query returned {len(df)} rows and {len(df.columns)} columns")
        return df
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        logging.error(f"Query: {query}")
        return pd.DataFrame()

def get_crosswalk_pairs(conn):
    query = """
    SELECT * FROM PROD.FORAGE.VOLDEMORT_PITCHBOOK_CROSSWALK_TO_VERIFY
    """
    df = execute_query(conn, query, "Fetching crosswalk pairs")
    if df.empty or df.shape[1] < 2:
        logging.error("Crosswalk table must have at least two columns (Pitchbook and Voldemort IDs)")
        return pd.DataFrame()
    return df

def get_bulk_company_common(conn, pitchbook_ids):
    if not pitchbook_ids:
        return pd.DataFrame()
    # Remove empty and nan
    pitchbook_ids = [str(i) for i in pitchbook_ids if pd.notna(i) and str(i).strip()]
    # Snowflake: max 16,384 expressions in IN clause, but we have ~2,500, so it's fine
    formatted_ids = ','.join([f"'{id}'" for id in pitchbook_ids])
    query = f"""
    SELECT * FROM PROD.PITCHBOOK.COMPANY_COMMON WHERE COMPANY_ID IN ({formatted_ids})
    """
    return execute_query(conn, query, f"Bulk fetching Pitchbook metadata for {len(pitchbook_ids)} IDs")

def get_bulk_voldemort_firmographics(conn, voldemort_ids):
    if not voldemort_ids:
        return pd.DataFrame()
    voldemort_ids = [str(i).replace("'", "") for i in voldemort_ids if pd.notna(i) and str(i).strip()]
    formatted_ids = ','.join([f"'{id}'" for id in voldemort_ids])
    query = f"""
    SELECT * FROM PROD.VOLDEMORT.VOLDEMORT_FIRMOGRAPHICS WHERE BQ_ID IN ({formatted_ids})
    """
    return execute_query(conn, query, f"Bulk fetching Voldemort metadata for {len(voldemort_ids)} IDs")

def create_crosswalk_csv(crosswalk_df, pb_df, vd_df, output_path):
    # Assume first column is Pitchbook ID, second is Voldemort ID
    pb_id_col = crosswalk_df.columns[0]
    vd_id_col = crosswalk_df.columns[1]
    # Prepare base result
    result_df = pd.DataFrame()
    result_df['pitchbook_id'] = crosswalk_df[pb_id_col].astype(str)
    result_df['bq_id'] = crosswalk_df[vd_id_col].astype(str).str.replace("'", "")
    # Prefix columns
    if not pb_df.empty:
        pb_df['COMPANY_ID'] = pb_df['COMPANY_ID'].astype(str)
        pb_prefixed = pb_df.add_prefix('pb_')
    else:
        pb_prefixed = pd.DataFrame()
    if not vd_df.empty:
        vd_df['BQ_ID'] = vd_df['BQ_ID'].astype(str)
        vd_prefixed = vd_df.add_prefix('vd_')
    else:
        vd_prefixed = pd.DataFrame()
    # Merge Pitchbook data
    if not pb_df.empty:
        result_df = result_df.merge(pb_prefixed, how='left', left_on='pitchbook_id', right_on='pb_COMPANY_ID')
    # Merge Voldemort data
    if not vd_df.empty:
        result_df = result_df.merge(vd_prefixed, how='left', left_on='bq_id', right_on='vd_BQ_ID')
    # Drop duplicate key columns if present
    if 'pb_COMPANY_ID' in result_df.columns:
        result_df = result_df.drop(columns=['pb_COMPANY_ID'])
    if 'vd_BQ_ID' in result_df.columns:
        result_df = result_df.drop(columns=['vd_BQ_ID'])
    # Rename vd_ and vd_BQ_ columns to bq_
    vd_cols = [col for col in result_df.columns if col.startswith('vd_')]
    for col in vd_cols:
        new_col = 'bq_' + col[3:] if not col.startswith('vd_BQ_') else 'bq_' + col[6:]
        result_df.rename(columns={col: new_col}, inplace=True)
    # Reorder columns: pitchbook_id, all pb_*, bq_id, all bq_*
    pb_cols = [col for col in result_df.columns if col.startswith('pb_')]
    bq_cols = [col for col in result_df.columns if col.startswith('bq_')]
    ordered_cols = ['pitchbook_id'] + pb_cols + ['bq_id'] + bq_cols
    # Add any remaining columns (shouldn't be any, but just in case)
    for col in result_df.columns:
        if col not in ordered_cols:
            ordered_cols.append(col)
    result_df = result_df[ordered_cols]
    try:
        result_df.to_csv(output_path, index=False)
        logging.info(f"Successfully saved output to: {output_path}")
        logging.info(f"Generated CSV with {len(result_df)} rows and {len(result_df.columns)} columns")
        return True
    except Exception as e:
        logging.error(f"Failed to save output: {e}")
        return False

def main():
    logging.info("Starting Crosswalk Entity Mapper script - BULK QUERY VERSION")
    conn = None
    try:
        conn = connect_to_snowflake()
        crosswalk_df = get_crosswalk_pairs(conn)
        if crosswalk_df.empty:
            logging.error("No crosswalk pairs found. Exiting.")
            return 1
        pitchbook_ids = crosswalk_df.iloc[:, 0].dropna().astype(str).tolist()
        voldemort_ids = crosswalk_df.iloc[:, 1].dropna().astype(str).tolist()
        pb_df = get_bulk_company_common(conn, pitchbook_ids)
        vd_df = get_bulk_voldemort_firmographics(conn, voldemort_ids)
        success = create_crosswalk_csv(crosswalk_df, pb_df, vd_df, OUTPUT_FILE)
        if success:
            logging.info(f"Entity mapping completed successfully. Output saved to {OUTPUT_FILE}")
        else:
            logging.error("Failed to create entity mapping CSV.")
            return 1
    finally:
        if conn:
            conn.close()
            logging.info("Closed Snowflake connection")
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code) 