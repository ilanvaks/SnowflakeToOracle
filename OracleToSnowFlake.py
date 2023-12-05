import os
import csv
import snowflake.connector
import cx_Oracle
 
# Snowflake connection details
SNOWFLAKE_USER = #USER#
SNOWFLAKE_ACCOUNT = #USER_ACCOUNT#
SNOWFLAKE_ROLE = #USER_ROLE#
SNOWFLAKE_DATABASE = #DATABASE#
 
# Oracle connection details
ORACLE_USER = #ORACLE_USER#
ORACLE_PASSWORD = #ORACLE_PASSWORD#
ORACLE_DSN = #ORACLE_DSN#
 
# CSV file network path
CSV_FILE_NETWORK_PATH = #FILE_PATHH#
 
# Function to get data from Snowflake
def get_snowflake_data():
    conn = snowflake.connector.connect(
       user=SNOWFLAKE_USER,
       password=#PASSWORD#,  
       account=SNOWFLAKE_ACCOUNT,
       role=SNOWFLAKE_ROLE,
       database=SNOWFLAKE_DATABASE
    )
    cur = conn.cursor()
    try:
        cur.execute("select
  PNM_AUTO_KEY
, PN
, DESCRIPTION
, PN_TYPE_CODE
, PN_TYPE_CODE_NEW
, CURRENT_TIER
, YEAR_TIER_RECO_OFFSET TIER_RECO
, case when YEAR_TIER_RECO_OFFSET_RULE != 'no offset'
    then '11/14/2023 Ilan V : tier changed from ' || CURRENT_TIER::varchar || ' to ' || YEAR_TIER_RECO_OFFSET::varchar || ', rule ' || YEAR_TIER_RULE || ' with offset: ' || YEAR_TIER_RECO_OFFSET_RULE
    else  '11/14/2023 Ilan V : tier changed from ' || CURRENT_TIER::varchar || ' to ' || YEAR_TIER_RECO_OFFSET::varchar || ', rule ' || YEAR_TIER_RULE
  end NOTES
from dw_prod.dw.SNAP_PN_TIER_RECO
where SNAP_ID = (select max(SNAP_ID) from dw_prod.dw.SNAP_PN_TIER_RECO)
and CURRENT_TIER::varchar != YEAR_TIER_RECO_OFFSET::varchar
")  
        data = cur.fetchall()
        return data
    finally:
        cur.close()
        conn.close()
 
# Function to write data to CSV
def write_to_csv(data, file_path):
    with open(file_path, 'w', newline='') as file:
        writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for row in data:
            formatted_row = [str(item) if item is not None else '' for item in row]
            writer.writerow(formatted_row)
 
# Function to run Oracle procedure
def run_oracle_procedure():
    conn = cx_Oracle.connect(ORACLE_USER, ORACLE_PASSWORD, ORACLE_DSN)
    cur = conn.cursor()
    try:
        cur.callproc('train.GAT_UPD_IC_UDF_001')  # Procedure name
    finally:
        cur.close()
        conn.close()
 
# Main function
def main():
    data = get_snowflake_data()
    write_to_csv(data, CSV_FILE_NETWORK_PATH)
    run_oracle_procedure()
 
if __name__ == "__main__":
    main()