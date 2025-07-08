#!/usr/bin/env python3
"""
Snowflake Entity Mapper - Automated Version
-----------------------------------------
Extract ALL columns from Pitchbook COMPANY_DATA_FEED and Voldemort VOLDEMORT_FIRMOGRAPHICS tables.
No command-line arguments required; just run the script.
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

# Input and output file paths (edit as needed)
INPUT_FILE = "test1_new.xlsx"  # Change this to your input Excel file if needed
OUTPUT_FILE = f"entity_mapping_{datetime.now().strftime('%Y%m%d')}.csv"

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

def load_input_data(file_path):
    try:
        logging.info(f"Loading input file: {file_path}")
        df = pd.read_excel(file_path)
        logging.info(f"Input file columns: {df.columns.tolist()}")
        logging.info(f"Input file shape: {df.shape}")
        return df
    except Exception as e:
        logging.error(f"Failed to load input file: {e}")
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

def get_pitchbook_data(conn, pitchbook_ids):
    if not pitchbook_ids:
        logging.warning("No Pitchbook IDs provided.")
        return pd.DataFrame()
    formatted_ids = ', '.join([f"'{id.strip()}'" for id in pitchbook_ids if id and str(id).strip()])
    if not formatted_ids:
        logging.warning("No valid Pitchbook IDs after formatting.")
        return pd.DataFrame()
    query = f"""
    SELECT *
    FROM PROD.PITCHBOOK.COMPANY_DATA_FEED 
    WHERE COMPANY_ID IN ({formatted_ids})
    """
    result_df = execute_query(conn, query, "Querying Pitchbook COMPANY_DATA_FEED - ALL columns")
    if not result_df.empty:
        logging.info(f"Retrieved {len(result_df)} rows with {len(result_df.columns)} columns from Pitchbook")
        logging.debug(f"Pitchbook columns: {result_df.columns.tolist()}")
    return result_df

def get_voldemort_data(conn, voldemort_ids):
    if not voldemort_ids:
        logging.warning("No Voldemort IDs provided.")
        return pd.DataFrame()
    formatted_ids = []
    for id in voldemort_ids:
        if id and str(id).strip() and str(id).strip().lower() not in ('nan', 'none', 'null'):
            cleaned_id = str(id).strip().replace("'", "")
            formatted_ids.append(f"'{cleaned_id}'")
    if not formatted_ids:
        logging.warning("No valid Voldemort IDs after formatting.")
        return pd.DataFrame()
    formatted_ids_str = ', '.join(formatted_ids)
    query = f"""
    SELECT *
    FROM PROD.VOLDEMORT.VOLDEMORT_FIRMOGRAPHICS 
    WHERE BQ_ID IN ({formatted_ids_str})
    """
    result_df = execute_query(conn, query, "Querying Voldemort VOLDEMORT_FIRMOGRAPHICS - ALL columns")
    if not result_df.empty:
        logging.info(f"Retrieved {len(result_df)} rows with {len(result_df.columns)} columns from Voldemort")
        logging.debug(f"Voldemort columns: {result_df.columns.tolist()}")
    else:
        test_query = "SELECT COUNT(*) FROM PROD.VOLDEMORT.VOLDEMORT_FIRMOGRAPHICS"
        test_result = execute_query(conn, test_query, "Testing Voldemort table access")
        if not test_result.empty:
            count = test_result.iloc[0, 0]
            logging.info(f"Voldemort table has {count} rows. Table exists and is accessible.")
    if not result_df.empty:
        result_df.columns = [col[3:] if col.startswith('vd_') else col for col in result_df.columns]
        logging.info(f"Removed 'vd_' prefix from Voldemort columns")
    return result_df

def create_complete_csv(input_df, pitchbook_df, voldemort_df, output_path):
    if len(input_df.columns) < 2:
        logging.error(f"Input file doesn't have enough columns. Expected at least 2, got {len(input_df.columns)}")
        return False
    result_df = pd.DataFrame()
    result_df['pitchbook_id'] = input_df.iloc[:, 0].astype(str)
    result_df['bq_id'] = input_df.iloc[:, 1].astype(str).str.replace("'", "")
    logging.info(f"Starting with {len(result_df)} rows")
    if not pitchbook_df.empty:
        logging.info(f"Adding {len(pitchbook_df.columns)} Pitchbook columns")
        pb_dict = {}
        for _, row in pitchbook_df.iterrows():
            company_id = str(row.get('COMPANY_ID', ''))
            pb_dict[company_id] = row.to_dict()
        for col in pitchbook_df.columns:
            new_col_name = f"pb_{col}"
            result_df[new_col_name] = ""
        for idx, row in result_df.iterrows():
            pb_id = str(row['pitchbook_id'])
            if pb_id in pb_dict:
                pb_data = pb_dict[pb_id]
                for col in pitchbook_df.columns:
                    new_col_name = f"pb_{col}"
                    result_df.at[idx, new_col_name] = pb_data.get(col, '')
    if not voldemort_df.empty:
        logging.info(f"Adding {len(voldemort_df.columns)} Voldemort columns")
        vd_dict = {}
        for _, row in voldemort_df.iterrows():
            bq_id = str(row.get('BQ_ID', '')).strip()
            vd_dict[bq_id] = row.to_dict()
        for col in voldemort_df.columns:
            new_col_name = f"vd_{col}"
            result_df[new_col_name] = ""
        for idx, row in result_df.iterrows():
            vd_id = str(row['bq_id']).strip()
            matched_id = None
            if vd_id in vd_dict:
                matched_id = vd_id
            elif vd_id.lstrip('0') in vd_dict:
                matched_id = vd_id.lstrip('0')
            elif vd_id.isdigit() and str(int(vd_id)) in vd_dict:
                matched_id = str(int(vd_id))
            if matched_id:
                vd_data = vd_dict[matched_id]
                for col in voldemort_df.columns:
                    new_col_name = f"vd_{col}"
                    result_df.at[idx, new_col_name] = vd_data.get(col, '')
    try:
        result_df.to_csv(output_path, index=False)
        logging.info(f"Successfully saved output to: {output_path}")
        logging.info(f"Generated CSV with {len(result_df)} rows and {len(result_df.columns)} columns")
        pb_filled = 0
        vd_filled = 0
        if not pitchbook_df.empty:
            pb_filled = result_df[f"pb_{pitchbook_df.columns[0]}"].notna().sum()
        if not voldemort_df.empty:
            vd_filled = result_df[f"vd_{voldemort_df.columns[0]}"].notna().sum()
        logging.info(f"Filled data: {pb_filled} rows with Pitchbook data, {vd_filled} rows with Voldemort data")
        return True
    except Exception as e:
        logging.error(f"Failed to save output: {e}")
        return False

def main():
    logging.info("Starting Entity Mapper script - ALL COLUMNS VERSION (Automated)")
    conn = None
    try:
        input_df = load_input_data(INPUT_FILE)
        if len(input_df.columns) < 2:
            raise ValueError(f"Input file must have at least 2 columns, but has {len(input_df.columns)}")
        pitchbook_ids = input_df.iloc[:, 0].dropna().astype(str).tolist()
        voldemort_ids = input_df.iloc[:, 1].dropna().astype(str).str.replace("'", "").tolist()
        logging.info(f"Extracted {len(pitchbook_ids)} Pitchbook IDs and {len(voldemort_ids)} Voldemort IDs")
        conn = connect_to_snowflake()
        try:
            pitchbook_df = get_pitchbook_data(conn, pitchbook_ids)
            voldemort_df = get_voldemort_data(conn, voldemort_ids)
            success = create_complete_csv(input_df, pitchbook_df, voldemort_df, OUTPUT_FILE)
            if success:
                logging.info(f"Entity mapping completed successfully. Output saved to {OUTPUT_FILE}")
            else:
                logging.error("Failed to create entity mapping CSV.")
                return 1
        finally:
            if conn:
                conn.close()
                logging.info("Closed Snowflake connection")
    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
        return 1
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
