import os
import pandas as pd
import pyodbc
from datetime import datetime
import re
SERVER = ''
DATABASE = ''
USERNAME = ''
PASSWORD = ''
TABLE_NAME = ''

CSV_FOLDER = r'D:\Bookswagon-Shristi\Write-1\Archive'



# ======================================
# OPTIONAL: CUSTOM NAME MAPPING
# (Use only if some columns are completely different)
# normalized_csv_name : exact_db_column_name
# ======================================
custom_mapping = {
    # Example:
    # "vendorid": "ID_Vendor",
    # "publishername": "Publisher"
}

# ======================================
# NORMALIZE FUNCTION
# ======================================
def normalize(col):
    return re.sub(r'[^a-zA-Z0-9]', '', col).lower()

# ======================================
# CONNECT TO SQL SERVER
# ======================================
conn = pyodbc.connect(
    f'DRIVER={{ODBC Driver 17 for SQL Server}};'
    f'SERVER={SERVER};'
    f'DATABASE={DATABASE};'
    f'UID={USERNAME};'
    f'PWD={PASSWORD}'
)

cursor = conn.cursor()
cursor.fast_executemany = True

# ======================================
# GET DB METADATA
# ======================================
cursor.execute(f"""
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = '{TABLE_NAME}'
""")

db_info = cursor.fetchall()

db_columns = []
int_columns = []
bit_columns = []

for col_name, data_type in db_info:
    if col_name != 'ID_MasterUpload':  # Skip identity column
        db_columns.append(col_name)

        if data_type in ['int', 'bigint', 'smallint', 'tinyint']:
            int_columns.append(col_name)

        elif data_type == 'bit':
            bit_columns.append(col_name)

# Create normalized DB lookup
normalized_db = {normalize(col): col for col in db_columns}

print("DB Columns Found:", db_columns)

# ======================================
# PROCESS ALL CSV FILES
# ======================================
for file_name in os.listdir(CSV_FOLDER):

    if file_name.endswith(".csv"):

        file_path = os.path.join(CSV_FOLDER, file_name)

        try:
            print(f"\nProcessing: {file_name}")
            start_time = datetime.now()

            df = pd.read_csv(file_path, encoding='cp1252')
            df.columns = df.columns.str.strip()
            df = df.where(pd.notnull(df), None)

            # ======================================
            # MATCH CSV WITH DB
            # ======================================
            matched_columns = {}

            for csv_col in df.columns:
                norm_csv = normalize(csv_col)

                # First check custom mapping
                if norm_csv in custom_mapping:
                    matched_columns[csv_col] = custom_mapping[norm_csv]

                # Then check normalized DB match
                elif norm_csv in normalized_db:
                    matched_columns[csv_col] = normalized_db[norm_csv]

            if not matched_columns:
                print("No matching columns found. Skipping file.")
                continue

            df = df[list(matched_columns.keys())]
            df.rename(columns=matched_columns, inplace=True)

            # ======================================
            # HANDLE DATA TYPES (DB Controls Logic)
            # ======================================
            for col in df.columns:

                # BIT columns
                if col in bit_columns:
                    df[col] = df[col].apply(
                        lambda x: 1 if str(x).strip().lower() in ['1','true','yes']
                        else 0 if str(x).strip().lower() in ['0','false','no']
                        else None
                    )

                # INT columns
                elif col in int_columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    df[col] = df[col].where(pd.notnull(df[col]), None)

                # VARCHAR/NVARCHAR
                else:
                    df[col] = df[col].astype(str)
                    df[col] = df[col].replace('nan', None)

            print("Matched Columns:", df.columns.tolist())
            print("Rows:", len(df))

            # ======================================
            # INSERT INTO SQL
            # ======================================
            columns = ",".join([f"[{col}]" for col in df.columns])
            placeholders = ",".join(["?"] * len(df.columns))

            insert_sql = f"""
            INSERT INTO {TABLE_NAME} ({columns})
            VALUES ({placeholders})
            """

            data = [tuple(row) for row in df.itertuples(index=False, name=None)]

            batch_size = 5000

            for i in range(0, len(data), batch_size):
                cursor.executemany(insert_sql, data[i:i+batch_size])

            conn.commit()

            print(f"Inserted {len(df)} rows from {file_name}")
            print("Time:", datetime.now() - start_time)

        except Exception as e:
            print(f"Error in {file_name}:", e)

cursor.close()
conn.close()

print("\nAll files processed successfully.")

