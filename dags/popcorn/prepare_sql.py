import csv
import os

csv_file = '/Users/nathan/Documents/Github/kdt-woongjin/dags/popcorn/realtime_subway_data.csv'
sql_file = '/Users/nathan/Documents/Github/kdt-woongjin/dags/popcorn/insert_data.sql'

create_table_sql = """
CREATE TABLE IF NOT EXISTS realtime_subway_positions (
    id SERIAL PRIMARY KEY,
    line_id VARCHAR(50),
    line_name VARCHAR(50),
    station_id VARCHAR(50),
    station_name VARCHAR(50),
    train_number VARCHAR(50),
    last_rec_date VARCHAR(50),
    last_rec_time TIMESTAMPTZ,
    direction_type INT,
    dest_station_id VARCHAR(50),
    dest_station_name VARCHAR(50),
    train_status INT,
    is_express INT DEFAULT 0,
    is_last_train BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

with open(csv_file, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    insert_rows = []
    for row in reader:
        # Handling NULLs and booleans
        def fmt(v):
            if v == '' or v is None or v == 'None': return 'NULL'
            if v == 'True': return 'TRUE'
            if v == 'False': return 'FALSE'
            # Escape single quotes for strings
            return "'" + str(v).replace("'", "''") + "'"

        # last_rec_time needs to be explicitly typed with KST if it's just a timestamp string
        # The CSV has "2026-01-13T14:08:44"
        lrt = row['last_rec_time']
        if lrt and lrt != 'None' and '+' not in lrt:
            lrt = f"{lrt}+09" # Add KST offset

        values = [
            fmt(row['line_id']),
            fmt(row['line_name']),
            fmt(row['station_id']),
            fmt(row['station_name']),
            fmt(row['train_number']),
            fmt(row['last_rec_date']),
            fmt(lrt),
            row['direction_type'] if row['direction_type'].isdigit() else 'NULL',
            fmt(row['dest_station_id']),
            fmt(row['dest_station_name']),
            row['train_status'] if row['train_status'].isdigit() else 'NULL',
            row['is_express'] if row['is_express'].isdigit() else '0',
            'TRUE' if row['is_last_train'] == 'True' else 'FALSE'
        ]
        insert_rows.append(f"({', '.join(values)})")

with open(sql_file, 'w', encoding='utf-8') as f:
    f.write(create_table_sql)
    f.write("\nINSERT INTO realtime_subway_positions \n")
    f.write("(line_id, line_name, station_id, station_name, train_number, last_rec_date, last_rec_time, direction_type, dest_station_id, dest_station_name, train_status, is_express, is_last_train)\n")
    f.write("VALUES\n")
    f.write(",\n".join(insert_rows))
    f.write(";")

print(f"SQL file created: {sql_file}")
