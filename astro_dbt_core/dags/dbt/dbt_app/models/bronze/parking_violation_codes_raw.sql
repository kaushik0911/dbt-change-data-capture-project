{{
    config(
        materialized='table',
        priority=2
    )
}}

WITH csv_data AS (
    SELECT
        *
    FROM
        read_csv(
            '/usr/local/airflow/datasets/dof_parking_violation_codes.csv',
            header = true
        )
)

SELECT *
FROM csv_data
