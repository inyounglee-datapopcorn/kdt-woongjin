import csv

csv_file = '/Users/nathan/Documents/Github/kdt-woongjin/dags/popcorn/realtime_subway_data.csv'
sql_file = '/Users/nathan/Documents/Github/kdt-woongjin/dags/popcorn/insert_data_small.sql'

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

def fmt(v):
    if v == '' or v is None or v == 'None': return 'NULL'
    if v == 'True': return 'TRUE'
    if v == 'False': return 'FALSE'
    return "'" + str(v).replace("'", "''") + "'"

with open(csv_file, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    rows = list(reader)

with open(sql_file, 'w', encoding='utf-8') as f:
    f.write(create_table_sql)
    f.write("\n")
    
    # Chunk size of 50
    chunk_size = 50
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i + chunk_size]
        f.write("INSERT INTO realtime_subway_positions \n")
        f.write("(line_id, line_name, station_id, station_name, train_number, last_rec_date, last_rec_time, direction_type, dest_station_id, dest_station_name, train_status, is_express, is_last_train)\n")
        f.write("VALUES\n")
        
        insert_rows = []
        for row in chunk:
            lrt = row['last_rec_time']
            if lrt and lrt != 'None' and '+' not in lrt:
                lrt = f"{lrt}+09"
            
            values = [
                fmt(row['line_id']), fmt(row['line_name']), fmt(row['station_id']), fmt(row['station_name']),
                fmt(row['train_number']), fmt(row['last_rec_date']), fmt(lrt),
                row['direction_type'] if row['direction_type'].isdigit() else 'NULL',
                fmt(row['dest_station_id']), fmt(row['dest_station_name']),
                row['train_status'] if row['train_status'].isdigit() else 'NULL',
                row['is_express'] if row['is_express'].isdigit() else '0',
                'TRUE' if row['is_last_train'] == 'True' else 'FALSE'
            ]
            insert_rows.append(f"({', '.join(values)})")
        
        f.write(",\n".join(insert_rows))
        f.write(";\nCOMMIT;\n")

print(f"Split SQL file created: {sql_file}")
