# Entity Matching & Mapping Scripts

This folder contains Python scripts to help you extract, match, and merge company data from Snowflake tables, and output the results as CSV files. These tools are designed to automate the process of mapping entities between different data sources (like Pitchbook and Voldemort) and are easy to adapt for new requirements.

---

## Table of Contents
- [Overview](#overview)
- [Scripts in This Folder](#scripts-in-this-folder)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [How to Run the Scripts](#how-to-run-the-scripts)
- [How to Adapt the Scripts for New Tables or Columns](#how-to-adapt-the-scripts-for-new-tables-or-columns)
- [Troubleshooting](#troubleshooting)
- [Contact](#contact)

---

## Overview

These scripts are used to:
- Connect to a Snowflake data warehouse
- Extract company/entity data from one or more tables
- Match and merge data based on IDs (like Pitchbook ID, Voldemort BQ_ID)
- Output the merged data to a CSV file for further analysis

They are especially useful for data mapping, crosswalks, and entity resolution projects.

---

## Scripts in This Folder

### 1. `snowflake_entity_mapper.py`
- **Purpose:**
  - Extracts and merges data from two Snowflake tables (Pitchbook and Voldemort) based on IDs provided in an Excel file.
  - Outputs a CSV with all columns from both tables, matched by the IDs.
- **When to use:**
  - When you have a list of IDs in an Excel file and want to pull all metadata for those IDs from two tables.

### 2. `crosswalk_entity_mapper.py`
- **Purpose:**
  - Extracts and merges data for all ID pairs found in a crosswalk table (no Excel input needed).
  - Runs very fast by querying all data in bulk.
  - Outputs a CSV with all columns from both tables, with clear prefixes (`pb_` for Pitchbook, `bq_` for Voldemort).
- **When to use:**
  - When you want to process all pairs from a crosswalk table in Snowflake, without needing to provide an input file.

### 3. `pitchbook_only_mapper.py`
- **Purpose:**
  - Extracts data only from the Pitchbook table for a list of IDs.
- **When to use:**
  - When you only need Pitchbook data for a set of IDs.

---

## Prerequisites
- Python 3.8 or higher
- Access to your organization's Snowflake account (with credentials)
- Required Python packages:
  - `pandas`
  - `snowflake-connector-python`
- (Optional) A virtual environment for Python

---

## Setup
1. **Clone or download this folder to your computer.**
2. **Install dependencies:**
   ```bash
   pip install pandas snowflake-connector-python
   ```
3. **(Optional) Set up a virtual environment:**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

---

## How to Run the Scripts

### For `crosswalk_entity_mapper.py` (fully automated, recommended)
1. **Open the script in your editor.**
2. **Edit the Snowflake credentials at the top of the script** (account, user, password, warehouse, role) if needed.
3. **Run the script:**
   ```bash
   python crosswalk_entity_mapper.py
   ```
4. **Output:**
   - A CSV file named like `crosswalk_entity_mapping_YYYYMMDD.csv` will be created in the same folder.
   - The file will have columns in this order:
     - `pitchbook_id`, all `pb_` columns, `bq_id`, all `bq_` columns

### For `snowflake_entity_mapper.py` and `pitchbook_only_mapper.py`
- These scripts may require command-line arguments (input file, output file, credentials, etc.).
- Example usage:
  ```bash
  python snowflake_entity_mapper.py -i input.xlsx -o output.csv -a ACCOUNT -u USER -p PASSWORD -w WAREHOUSE -r ROLE
  ```
- See the top of each script for details.

---

## How to Adapt the Scripts for New Tables or Columns

If you want to extract data from different tables or columns, follow these steps:

1. **Find the section in the script where the table names are used.**
   - For example, in `crosswalk_entity_mapper.py`, look for lines like:
     ```python
     SELECT * FROM PROD.PITCHBOOK.COMPANY_COMMON WHERE COMPANY_ID IN (...)
     SELECT * FROM PROD.VOLDEMORT.VOLDEMORT_FIRMOGRAPHICS WHERE BQ_ID IN (...)
     SELECT * FROM PROD.FORAGE.VOLDEMORT_PITCHBOOK_CROSSWALK_TO_VERIFY
     ```
2. **Change the table names** to the new ones you want to use.
   - Example: If you want to use a different crosswalk table, change the table name in the `get_crosswalk_pairs` function.
   - If you want to pull from a different metadata table, change the table name in the relevant query.
3. **If you only want specific columns,** replace `SELECT *` with `SELECT col1, col2, ...` in the query.
4. **If the ID column names are different,** update the merge/join logic to use the new column names.
5. **Save the script and run it as before.**

**Tip:**
- Always check the output CSV to make sure the columns and data look correct.
- If you get errors about merging, make sure the ID columns are the same type (convert to string if needed).

---

## Troubleshooting
- **Script runs but output is empty:**
  - Check that your IDs exist in the source tables.
  - Make sure you have access to the correct Snowflake database and tables.
- **Merge errors:**
  - Make sure the columns you are joining on are both strings (use `.astype(str)` if needed).
- **Snowflake connection errors:**
  - Double-check your credentials and network access.
- **Need to change output format (e.g., Excel):**
  - Replace `to_csv` with `to_excel` in the script and install `openpyxl`.

---



