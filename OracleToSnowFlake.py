import os
import csv
import snowflake.connector
import cx_Oracle
 
# Snowflake connection details
SNOWFLAKE_USER = 'PYTHON_USER'
SNOWFLAKE_ACCOUNT = 'qr93446.east-us-2.azure'
SNOWFLAKE_ROLE = 'SYSADMIN'
SNOWFLAKE_DATABASE = 'DW_PROD'
 
# Oracle connection details
ORACLE_USER = 'diatrain'
ORACLE_PASSWORD = 'quantum'
ORACLE_DSN = 'GAT-FL-QDB2.gatelesis.com:1521/CCTL'

# Email details
recipients = 'example@example.com'  
subject = 'Oracle Procedure Execution Report'
 
# CSV file network path
CSV_FILE_NETWORK_PATH = r'\\gat-fl-qdb2\dia_import\parts_master_tear.csv'
 
# Function to get data from Snowflake
def get_snowflake_data():
    conn = snowflake.connector.connect(
       user=SNOWFLAKE_USER,
       password=os.getenv('SNOWFLAKE_PYTHON_USER'),  
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
")  # Replace with your actual query
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
    write_to_csv(data, CSV_FILE_NETWORK_PATH)
    updated_tier_count = len(data)
    run_oracle_procedure()
    send_email_with_attachment(CSV_FILE_NETWORK_PATH, updated_tier_count)
 
if __name__ == "__main__":
    main()