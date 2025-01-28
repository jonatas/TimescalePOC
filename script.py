import psycopg2
import os
from datetime import datetime

try:
    device_count = int(os.getenv('DEVICE_COUNT'))
    tag_count = int(os.getenv('TAG_COUNT'))
    history_years = int(os.getenv('HISTORY_YEARS'))
except Exception as e:
    print(f"Parameters error: {e}")

today = datetime.today()
creation_date = today.replace(year=today.year - history_years).strftime('%Y-%m-%d %H:%M:%S')

conn_string = os.environ.get('CONN_STRING', "postgresql://postgres:5432/TEST?user=postgres&password=TEST!timescale")

try:
    db_connection = psycopg2.connect(conn_string)
    cursor = db_connection.cursor()
    print("Succesfully connected to DB", flush=True)
except Exception as e:
    print(f"Error during connection to db {e}")

def execute_query(query):
    try:
        cursor.execute(query)
        db_connection.commit()
    except Exception as e:
        print(f"Errore query: {e}")
        exit(0)

create_table_query = f'''
CREATE TABLE IF NOT EXISTS public."Values"
(
    "Date" timestamp with time zone NOT NULL,
    "Tag" character varying(30) COLLATE pg_catalog."default" NOT NULL,
    "Device" character varying(20) COLLATE pg_catalog."default" NOT NULL,
    "Value" double precision NOT NULL,
    CONSTRAINT "PK_Values" PRIMARY KEY ("Tag", "Device", "Date")
);

SELECT create_hypertable('"Values"', by_range('Date', INTERVAL '1 month')); 
SELECT add_dimension('"Values"', by_hash('Device', 4));
'''
execute_query(create_table_query)

start_year = (today.year - history_years)
print(f"ADDING VALUES", flush=True)
for device in range(device_count):
    query = ''
    print(f"\tDEVICE_{device}", flush=True)

    for tag in range(tag_count):
        print(f"\t\tTAG_{tag}", flush=True)
        query += f'''
            INSERT INTO "Values" ("Date", "Tag", "Device", "Value")
            SELECT
                generate_series AS "Data",
                'TAG_{tag}' AS "Tag",
                'DEVICE_{device}' AS "Device",
                random()*1 AS "Valore" from generate_series('{start_year}-01-01', '{today.year}-01-01', INTERVAL '1 hour');
        '''
    execute_query(query)



    

