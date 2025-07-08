#!/usr/bin/env python3
"""
Pitchbook Only Mapper - Automated Version
----------------------------------------
Extracts data from the Pitchbook COMPANY_DATA_FEED table for a list of Pitchbook IDs from an Excel file.
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
OUTPUT_FILE = f"pitchbook_only_mapping_{datetime.now().strftime('%Y%m%d')}.csv"

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
            application='PitchbookOnlyMapper'
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

def create_pitchbook_only_csv(input_df, pitchbook_df, output_path):
    if input_df.empty or input_df.shape[1] < 1:
        logging.error(f"Input file doesn't have enough columns. Expected at least 1, got {input_df.shape[1]}")
        return False
    result_df = pd.DataFrame()
    result_df['pitchbook_id'] = input_df.iloc[:, 0].astype(str)
    if not pitchbook_df.empty:
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
    try:
        result_df.to_csv(output_path, index=False)
        logging.info(f"Successfully saved output to: {output_path}")
        logging.info(f"Generated CSV with {len(result_df)} rows and {len(result_df.columns)} columns")
        return True
    except Exception as e:
        logging.error(f"Failed to save output: {e}")
        return False

def main():
    logging.info("Starting Pitchbook Only Mapper script - Automated Version")
    conn = None
    try:
        input_df = load_input_data(INPUT_FILE)
        if input_df.shape[1] < 1:
            raise ValueError(f"Input file must have at least 1 column, but has {input_df.shape[1]}")
        pitchbook_ids = input_df.iloc[:, 0].dropna().astype(str).tolist()
        logging.info(f"Extracted {len(pitchbook_ids)} Pitchbook IDs")
        conn = connect_to_snowflake()
        try:
            pitchbook_df = get_pitchbook_data(conn, pitchbook_ids)
            success = create_pitchbook_only_csv(input_df, pitchbook_df, OUTPUT_FILE)
            if success:
                logging.info(f"Pitchbook mapping completed successfully. Output saved to {OUTPUT_FILE}")
            else:
                logging.error("Failed to create Pitchbook mapping CSV.")
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
