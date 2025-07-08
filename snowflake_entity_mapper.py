#!/usr/bin/env python3
"""
Snowflake Entity Mapper - Updated Version
-----------------------------------------
Extract ALL columns from Pitchbook COMPANY_DATA_FEED and Voldemort VOLDEMORT_FIRMOGRAPHICS tables.
"""

import argparse
import logging
import pandas as pd
import snowflake.connector
import os
import sys
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("entity_mapper.log"),
        logging.StreamHandler()
    ]
)

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Extract and map entity data from Snowflake databases'
    )
    
    parser.add_argument('-i', '--input-file', required=True, help='Path to input Excel file')
    parser.add_argument('-o', '--output-file', default=f'entity_mapping_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv', help='Path to output CSV file')
    parser.add_argument('-a', '--account', required=True, help='Snowflake account identifier')
    parser.add_argument('-u', '--user', required=True, help='Snowflake username')
    parser.add_argument('-p', '--password', required=True, help='Snowflake password')
    parser.add_argument('-w', '--warehouse', default='FORAGE_AI_WH', help='Snowflake warehouse name')
    parser.add_argument('-r', '--role', default='FORAGE_AI_USER', help='Snowflake role name')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose logging')
    
    return parser.parse_args()

def connect_to_snowflake(account, user, password, warehouse, role):
    """Connect to Snowflake using the provided credentials."""
    try:
        logging.info(f"Connecting to Snowflake with account: {account}, user: {user}, warehouse: {warehouse}")
        
        conn = snowflake.connector.connect(
            account=account,
            user=user,
            password=password,
            warehouse=warehouse,
            database='PROD',
            role=role,
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
    """Load the input Excel file containing entity IDs."""
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
    """Execute a query on the Snowflake connection."""
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
    """Get ALL data from Pitchbook COMPANY_DATA_FEED for the specified entity IDs."""
    if not pitchbook_ids:
        logging.warning("No Pitchbook IDs provided.")
        return pd.DataFrame()
    
    formatted_ids = ', '.join([f"'{id.strip()}'" for id in pitchbook_ids if id and str(id).strip()])
    
    if not formatted_ids:
        logging.warning("No valid Pitchbook IDs after formatting.")
        return pd.DataFrame()
    
    # Query ALL columns from COMPANY_DATA_FEED
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
    """Get ALL data from Voldemort VOLDEMORT_FIRMOGRAPHICS for the specified entity IDs."""
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
    
    # Query ALL columns from VOLDEMORT_FIRMOGRAPHICS
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
        # Test table access
        test_query = "SELECT COUNT(*) FROM PROD.VOLDEMORT.VOLDEMORT_FIRMOGRAPHICS"
        test_result = execute_query(conn, test_query, "Testing Voldemort table access")
        if not test_result.empty:
            count = test_result.iloc[0, 0]
            logging.info(f"Voldemort table has {count} rows. Table exists and is accessible.")
    
    # ADD THIS CODE HERE - Remove 'vd_' prefix from all Voldemort columns
    if not result_df.empty:
        result_df.columns = [col[3:] if col.startswith('vd_') else col for col in result_df.columns]
        logging.info(f"Removed 'vd_' prefix from Voldemort columns")
    
    return result_df


def create_complete_csv(input_df, pitchbook_df, voldemort_df, output_path):
    """Create CSV with ALL columns from both datasets."""
    
    if len(input_df.columns) < 2:
        logging.error(f"Input file doesn't have enough columns. Expected at least 2, got {len(input_df.columns)}")
        return False
    
    # Start with the input IDs
    result_df = pd.DataFrame()
    result_df['pitchbook_id'] = input_df.iloc[:, 0].astype(str)
    result_df['bq_id'] = input_df.iloc[:, 1].astype(str).str.replace("'", "")
    
    logging.info(f"Starting with {len(result_df)} rows")
    
    # Add ALL Pitchbook columns with 'pb_' prefix
    if not pitchbook_df.empty:
        logging.info(f"Adding {len(pitchbook_df.columns)} Pitchbook columns")
        
        # Create dictionaries for mapping
        pb_dict = {}
        for _, row in pitchbook_df.iterrows():
            company_id = str(row.get('COMPANY_ID', ''))
            pb_dict[company_id] = row.to_dict()
        
        # Add all Pitchbook columns to result_df
        for col in pitchbook_df.columns:
            new_col_name = f"pb_{col}"
            result_df[new_col_name] = ""
        
        # Fill Pitchbook data
        for idx, row in result_df.iterrows():
            pb_id = str(row['pitchbook_id'])
            if pb_id in pb_dict:
                pb_data = pb_dict[pb_id]
                for col in pitchbook_df.columns:
                    new_col_name = f"pb_{col}"
                    result_df.at[idx, new_col_name] = pb_data.get(col, '')
    
    # Add ALL Voldemort columns with 'vd_' prefix
    if not voldemort_df.empty:
        logging.info(f"Adding {len(voldemort_df.columns)} Voldemort columns")
        
        # Create dictionaries for mapping
        vd_dict = {}
        for _, row in voldemort_df.iterrows():
            bq_id = str(row.get('BQ_ID', '')).strip()
            vd_dict[bq_id] = row.to_dict()
        
        # Add all Voldemort columns to result_df
        for col in voldemort_df.columns:
            new_col_name = f"vd_{col}"
            result_df[new_col_name] = ""
        
        # Fill Voldemort data with improved matching
        for idx, row in result_df.iterrows():
            vd_id = str(row['bq_id']).strip()
            
            # Try multiple matching strategies
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
    
    # Save to CSV
    try:
        result_df.to_csv(output_path, index=False)
        logging.info(f"Successfully saved output to: {output_path}")
        logging.info(f"Generated CSV with {len(result_df)} rows and {len(result_df.columns)} columns")
        
        # Count filled data
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
    """Main function to run the entity mapping process."""
    args = parse_arguments()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    logging.info("Starting Entity Mapper script - ALL COLUMNS VERSION")
    
    conn = None
    
    try:
        # Load input file
        input_df = load_input_data(args.input_file)
        
        # Extract entity IDs
        if len(input_df.columns) < 2:
            raise ValueError(f"Input file must have at least 2 columns, but has {len(input_df.columns)}")
        
        pitchbook_ids = input_df.iloc[:, 0].dropna().astype(str).tolist()
        voldemort_ids = input_df.iloc[:, 1].dropna().astype(str).str.replace("'", "").tolist()
        
        logging.info(f"Extracted {len(pitchbook_ids)} Pitchbook IDs and {len(voldemort_ids)} Voldemort IDs")
        
        # Connect to Snowflake
        conn = connect_to_snowflake(args.account, args.user, args.password, args.warehouse, args.role)
        
        try:
            # Get ALL Pitchbook data from COMPANY_DATA_FEED
            pitchbook_df = get_pitchbook_data(conn, pitchbook_ids)
            
            # Get ALL Voldemort data from VOLDEMORT_FIRMOGRAPHICS
            voldemort_df = get_voldemort_data(conn, voldemort_ids)
            
            # Create complete CSV with all columns
            success = create_complete_csv(input_df, pitchbook_df, voldemort_df, args.output_file)
            
            if success:
                logging.info(f"Entity mapping completed successfully. Output saved to {args.output_file}")
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
