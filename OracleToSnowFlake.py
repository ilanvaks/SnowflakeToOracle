import os
import csv
import snowflake.connector
import cx_Oracle
import connect
import oracledb
from email_send_function import *
import datetime

today = date.today()

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
CSV_EMAIL_FILE_NETWORK_PATH = 'tier_reco.csv'
 
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
        cur.execute(f'''select
  PNM_AUTO_KEY
, PN
, DESCRIPTION
, PN_TYPE_CODE
, PN_TYPE_CODE_NEW
, CURRENT_TIER
, YEAR_TIER_RECO_OFFSET TIER_RECO
, case when YEAR_TIER_RECO_OFFSET_RULE != 'no offset'
    then '{today} Ilan V : tier changed from ' || CURRENT_TIER::varchar || ' to ' || YEAR_TIER_RECO_OFFSET::varchar || ', rule ' || YEAR_TIER_RULE || ' with offset: ' || YEAR_TIER_RECO_OFFSET_RULE
    else  '{today} Ilan V : tier changed from ' || CURRENT_TIER::varchar || ' to ' || YEAR_TIER_RECO_OFFSET::varchar || ', rule ' || YEAR_TIER_RULE
  end NOTES
from dw_prod.dw.SNAP_PN_TIER_RECO
where SNAP_ID = (select max(SNAP_ID) from dw_prod.dw.SNAP_PN_TIER_RECO)
and CURRENT_TIER::varchar != YEAR_TIER_RECO_OFFSET::varchar
''')  
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
oracledb.init_oracle_client(lib_dir=r"C:\oracle\instantclient_21_12") ## Replace with your actual Oracle client path, need this line to run the script.  Uses Oracle 64bit
def run_oracle_procedure():
    conn = cx_Oracle.connect(ORACLE_USER, ORACLE_PASSWORD, ORACLE_DSN)
    cur = conn.cursor()
    try:
        cur.callproc('train.GAT_UPD_IC_UDF_001')  # Procedure name
    finally:
        cur.close()
        conn.close()

# Function to send email with attachment
def send_email_with_attachment(file_path, updated_tier_count):
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if updated_tier_count > 0:
        body = f'''<p>Hello,</p>
        <p>The Oracle procedure was executed successfully on {current_time}.</p>
        <p>Number of PN tiers updated: {updated_tier_count}</p>
        <p>Please find the attached CSV file for more details.</p>
        <p>Regards,</p>'''
    else:
        body = f'''<p>Hello,</p>
        <p>The Oracle procedure was executed on {current_time}, but no PN tiers were updated.</p>
        <p>Regards,</p>'''

    try:
        send_my_email(recipients, subject, body, file_path, body_type='html')
    except Exception as e:
        print(f'Error! Could not send email: {e}')
 

# Main function
def main():
    data = get_snowflake_data()
    # save csv file to be read by the Ora proc
    write_to_csv(data[['PNM_AUTO_KEY','TIER_RECO','NOTES']], CSV_FILE_NETWORK_PATH)
    # save another file to be added to email as attachment
    write_to_csv(data[['PN','DESC','CURRENT_TIER','TIER_RECO,']], CSV_EMAIL_FILE_NETWORK_PATH)
    updated_tier_count = len(data)
    run_oracle_procedure()
    send_email_with_attachment(CSV_EMAIL_FILE_NETWORK_PATH, updated_tier_count)
 
if __name__ == "__main__":
    main()
