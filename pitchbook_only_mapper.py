import argparse
import logging
import pandas as pd
import snowflake.connector
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("entity_mapper.log"), logging.StreamHandler()]
)

def parse_arguments():
    parser = argparse.ArgumentParser(description='Extract ALL Pitchbook data from Snowflake')
    parser.add_argument('-i', '--input-file', required=True, help='Excel file with Pitchbook IDs')
    parser.add_argument('-o', '--output-file', default=f'pitchbook_entity_mapping_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
    parser.add_argument('-a', '--account', required=True, help='Snowflake account identifier')
    parser.add_argument('-u', '--user', required=True, help='Snowflake username')
    parser.add_argument('-p', '--password', required=True, help='Snowflake password')
    parser.add_argument('-w', '--warehouse', default='FORAGE_AI_WH', help='Snowflake warehouse')
    parser.add_argument('-r', '--role', default='FORAGE_AI_USER', help='Snowflake role')
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose logging')
    return parser.parse_args()

def connect_to_snowflake(account, user, password, warehouse, role):
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
    return conn

def load_input_data(file_path):
    df = pd.read_excel(file_path)
    return df

def get_pitchbook_data(conn, pitchbook_ids):
    formatted_ids = ', '.join([f"'{id.strip()}'" for id in pitchbook_ids if id and str(id).strip()])
    if not formatted_ids:
        return pd.DataFrame()
    query = f"SELECT * FROM PROD.PITCHBOOK.COMPANY_DATA_FEED WHERE COMPANY_ID IN ({formatted_ids})"
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [col[0] for col in cursor.description]
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=columns)
    return df

def create_pitchbook_only_csv(input_df, pitchbook_df, output_path):
    # Merge input IDs with Pitchbook data
    result_df = pd.DataFrame()
    result_df['pitchbook_id'] = input_df.iloc[:, 0].astype(str)
    pb_dict = {str(row['COMPANY_ID']): row for _, row in pitchbook_df.iterrows()}
    for col in pitchbook_df.columns:
        result_df[col] = result_df['pitchbook_id'].map(lambda x: pb_dict.get(x, {}).get(col, ''))
    result_df.to_csv(output_path, index=False)
    logging.info(f"Saved output to {output_path}")

def main():
    args = parse_arguments()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    input_df = load_input_data(args.input_file)
    pitchbook_ids = input_df.iloc[:, 0].dropna().astype(str).tolist()
    conn = connect_to_snowflake(args.account, args.user, args.password, args.warehouse, args.role)
    try:
        pitchbook_df = get_pitchbook_data(conn, pitchbook_ids)
        create_pitchbook_only_csv(input_df, pitchbook_df, args.output_file)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
