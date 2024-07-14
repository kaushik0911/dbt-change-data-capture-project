{{
    config(
        materialized='table'
    )
}}

WITH csv_data AS (
    SELECT
        *
    FROM
        read_csv(
            '../datasets/dof_parking_violation_codes.csv',
            header = true
        )
)

SELECT *
FROM csv_data
