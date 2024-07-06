{{
    config(
        materialized='table',
        priority=1
    )
}}

WITH csv_data AS (
    SELECT
        *
    FROM
        read_csv(
            '/usr/local/airflow/datasets/Parking_Violations_2023_Sample_Next.csv',
            header = true,
            dateformat = '%m/%d/%Y',
            columns = {
                'Summons Number' : BIGINT,
                'Plate ID' : VARCHAR,
                'Registration State' : VARCHAR,
                'Plate Type' : VARCHAR,
                'Issue Date' : DATE,
                'Violation Code' : SMALLINT,
                'Vehicle Body Type' : VARCHAR,
                'Vehicle Make' : VARCHAR,
                'Issuing Agency' : VARCHAR,
                'Street Code1' : INTEGER,
                'Street Code2' : INTEGER,
                'Street Code3' : INTEGER,
                'Vehicle Expiration Date' : VARCHAR,
                'Violation Location' : FLOAT,
                'Violation Precinct' : SMALLINT,
                'Issuer Precinct' : SMALLINT,
                'Issuer Code' : INTEGER,
                'Issuer Command' : VARCHAR,
                'Issuer Squad' : VARCHAR,
                'Violation Time' : VARCHAR,
                'Time First Observed' : VARCHAR,
                'Violation County' : VARCHAR,
                'Violation In Front Of Or Opposite' : VARCHAR,
                'House Number' : VARCHAR,
                'Street Name' : VARCHAR,
                'Intersecting Street' : VARCHAR,
                'Date First Observed' : VARCHAR,
                'Law Section' : SMALLINT,
                'Sub Division' : VARCHAR,
                'Violation Legal Code' : VARCHAR,
                'Days Parking In Effect' : VARCHAR,
                'From Hours In Effect' : VARCHAR,
                'To Hours In Effect' : VARCHAR,
                'Vehicle Color' : VARCHAR,
                'Unregistered Vehicle?' : VARCHAR,
                'Vehicle Year' : SMALLINT,
                'Meter Number' : VARCHAR,
                'Feet From Curb' : SMALLINT,
                'Violation Post Code' : VARCHAR,
                'Violation Description' : VARCHAR,
                'No Standing or Stopping Violation' : VARCHAR,
                'Hydrant Violation' : VARCHAR,
                'Double Parking Violation' : VARCHAR
            }
        )
)

SELECT *
FROM csv_data
