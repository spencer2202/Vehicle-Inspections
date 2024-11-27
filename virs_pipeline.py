import pandas as pd
import pyodbc
import os
import csv


# Replace with your actual SQL Server connection details
server = 'localhost\SQLEXPRESS'
database = 'VIRSpipeline'

# Connection string for SQL Server
conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database}'
conn = pyodbc.connect(conn_str)

# Function to create the data models
def create_data_model():
    with pyodbc.connect(conn_str) as conn:
        with conn.cursor() as cursor:
            # Create the dim_org table
            cursor.execute('''
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_org')
            BEGIN
                CREATE TABLE dim_org (
                    org_id INT PRIMARY KEY,
                    org_name VARCHAR(100) NOT NULL
                )
            END
            ''')

            # Create the fact_inspections table
            cursor.execute('''
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'fact_inspections')
            BEGIN
                CREATE TABLE fact_inspections (
                    vehicle_id INT,
                    inspection_date DATE,
                    org_id INT,
                    inspection_result INT,
                    PRIMARY KEY (vehicle_id, inspection_date),
                    FOREIGN KEY (org_id) REFERENCES dim_org (org_id)
                )
            END
            ''')

            # Commit the changes
            conn.commit()

# Function to read and process the CSV files
def process_dump(dump_file):

    dump_file = "csv_files\\" + dump_file
    with open(dump_file, 'r') as file:
        csv_reader = csv.reader(file, delimiter='|')
        next(csv_reader)  # Skip the header row

        for row in csv_reader:
            print(row)
            vehicle_id, inspection_date, vehicle_org_id, org_name, inspection_period_id, inspection_result = row

            if inspection_result == '':
                inspection_result = None
            else:
                inspection_result = 1 if inspection_result.lower() == 'true' else 0

            # Insert or update organization in the dim_org table
            with conn.cursor() as cursor:
                cursor.execute("""
                    MERGE INTO dim_org AS target
                    USING (VALUES (?, ?)) AS source (org_id, org_name)
                    ON target.org_id = source.org_id
                    WHEN MATCHED THEN
                        UPDATE SET target.org_name = source.org_name
                    WHEN NOT MATCHED THEN
                        INSERT (org_id, org_name) VALUES (source.org_id, source.org_name);
                """, (vehicle_org_id, org_name))

            # Insert or update organization in the fact_inspections table
            # This stores the latest value of inspection for each unique inspection report (vehicle_id + inspection_date).
            with conn.cursor() as cursor:
                cursor.execute("""
                    MERGE INTO fact_inspections AS target
                    USING (VALUES (?, ?, ?, ?)) AS source (vehicle_id, inspection_date, org_id, inspection_result)
                    ON target.vehicle_id = source.vehicle_id AND target.inspection_date = source.inspection_date
                    WHEN MATCHED THEN
                        UPDATE SET target.inspection_result = source.inspection_result
                    WHEN NOT MATCHED THEN
                        INSERT (vehicle_id, inspection_date, org_id, inspection_result) 
                        VALUES (source.vehicle_id, source.inspection_date, source.org_id, source.inspection_result);
                """, (vehicle_id, inspection_date, vehicle_org_id, inspection_result))
    conn.commit()


# Function to generate the report
def generate_report():

    query = '''
    WITH latest_vehicle_fail AS (
      SELECT 
            vehicle_id, 
            org_id, 
            inspection_date,
            inspection_result,
            ROW_NUMBER() OVER (PARTITION BY vehicle_id ORDER BY inspection_date DESC) AS rn
      FROM fact_inspections 
      WHERE inspection_result IS NOT NULL
    ),
    
    org_report AS (
        SELECT latest_vehicle_fail.org_id,
               COUNT(*) AS total_inspected_vehicle,
               SUM(CASE WHEN inspection_result = 0 THEN 1 ELSE 0 END) AS vehicle_fail,
               ROUND(SUM(CASE WHEN inspection_result = 0 THEN 1 ELSE 0 END)*1.0 / COUNT(*), 2) as percent_vehicle_fail
        FROM latest_vehicle_fail 
            INNER JOIN dim_org ON dim_org.org_id = latest_vehicle_fail.org_id
        WHERE rn = 1
        GROUP BY latest_vehicle_fail.org_id
    )
    
    SELECT TOP 3 org_name, total_inspected_vehicle, vehicle_fail
    FROM org_report	
        INNER JOIN dim_org ON org_report.org_id = dim_org.org_id
    ORDER BY percent_vehicle_fail DESC
    '''
    df = pd.read_sql(query, conn)

    # Write the report to the virs_report.tsv file
    df.to_csv('virs_report.tsv', sep='|', index=False)

if __name__ == "__main__":
    # Create the data model in SQL Server
    create_data_model()

    # Process the CSV files and insert data into the tables
    dump_files = [f for f in os.listdir('csv_files') if f.endswith('.csv')]
    for dump_file in sorted(dump_files):
        process_dump(dump_file)

    # Generate the report
    generate_report()
