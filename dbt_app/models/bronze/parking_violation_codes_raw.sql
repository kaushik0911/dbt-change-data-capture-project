{{
    config(
        materialized='table'
    )
}}

WITH csv_data AS (
    SELECT
        *
    FROM
        file(
            'dof_parking_violation_codes.csv',
            CSVWithNames
        )
)

SELECT *
FROM csv_data
