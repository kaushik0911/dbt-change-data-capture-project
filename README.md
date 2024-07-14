Change data capture using dbt
=============================

> Change data capture (CDC) is a technique used to track changes in data within a database. Dbt (data build tool) is an open-source command-line tool that helps data analysts and engineers transform data in their data warehouse more effectively.

Installing dbt
--------------

To install dbt developers can refer to official dbt documentation.
[https://docs.getdbt.com/docs/core/pip-install](https://docs.getdbt.com/docs/core/pip-install)

dbt adapter allows dbt to interact with a specific database or data warehouse. In this article, I'm going to use [DuckDB](https://duckdb.org/).

```
pip install duckdb dbt-duckdb
```

Run the dbt init command followed by the name you want to give to your new project.

```
dbt init my_dbt_project
```

Setting Up the Profile

Load and clean raw data
-----------------------

Once you set up the dbt project you must load the data to the database. To do that there are various methods you can follow. The below code snippet is to load data via DuckDB.

Load data using seed
--------------------

*   Create a directory named `data` in your dbt project root directory.

```
my_dbt_project/
├── data/
│   └── my_seed_file.csv
```

*   Run `dbt seed` to load the data into your data warehouse.

```
dbt seed
```

Load data using SQL functions
-----------------------------

*   DuckDB supports its [method](https://duckdb.org/docs/data/csv/overview) to import data directly from files.

```
SELECT * FROM read_csv('test.csv', header = true);
```

*   In dbt you can use this method in a model.

```
my_dbt_project/
├── models/
│   └── import_data.sql
```
```
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
            '<csv-file-path>',
            header = true
        )
)
SELECT *
FROM csv_data
```

*   The above method sniffs the dataset, identifies the data types, and imports data to the database.

Perform CDC using a snapshot
----------------------------

Since this article focuses on CDC, let's see what it is.

> A snapshot is a way to capture and store the state of a table or a set of records over time. Dbt snapshots are particularly useful for tracking changes in slowly changing dimensions (SCDs) or capturing historical changes in data. They allow you to see how data has changed over time and can be useful for auditing, historical analysis, and maintaining a history of changes.

Example Snapshot Configuration
------------------------------

[Snapshot](https://docs.getdbt.com/docs/build/snapshots) files are stored in the `snapshots` directory in the dbt project.

```
my_dbt_project/
├── snapshots/
│   └── change_data_capture.sql
```
```
{% snapshot change_data_capture_method %}
{{
    config(
        target_schema='<schema-in-database>',
        unique_key='<unique-key-column>',
        strategy='check/timestamp',
        check_cols='all'/['col-1', 'col-2'],
        updated_at='<updated-timestamp-col-name>'
    )
}}
select * from {{ ref('<model-name>') }}
{% endsnapshot %}
```

*   target_schema — The schema where the snapshot data is going to be stored.
*   unique_key — The **column**(one column) that uniquely identifies a record.
*   strategy — How to detect changes.
*   strategy=“check” — Data comparison
*   strategy=“timestamp” — `updated_at` field to determine if a row has changed. The`updated_at` attribute is required only if the strategy is timestamp.
*   check_cols=“all” — Compare data in all columns.
*   check_cols=[“col-1”, “col-2”] — Compare data in selected columns.

Below is an example if you want to check all the columns.

```
{% snapshot parking_violation_cdc %}
{{
    config(
        target_schema='main',
        unique_key='summons_number',
        strategy='check',
        check_cols='all'
    )
}}
select * from {{ ref('bronze_parking_violations') }}
{% endsnapshot %}
```

Once you define the snapshot you can run the snapshot using the command.

```
dbt snapshot
```

What is inside the snapshot table
---------------------------------

The snapshot table will contain historical records with metadata columns added by dbt to track the validity of each record over time.

*   dbt_valid_from — when data gets inserted
*   dbt_valid_to — Once an updated record gets inserted for the same unique id, the previous record gets expires.
*   dbt_updated_at — When the record was last updated.

As an example below table shows data changes in columns `registration_state` and `issue_date`.

```
SELECT summons_number, registration_state , issue_date, dbt_updated_at, dbt_valid_from , dbt_valid_to
FROM <schema-name>.<snapshot-name>
WHERE summons_number in (1, 2)
```
```
summons_number|registration_state|issue_date|dbt_updated_at         |dbt_valid_from         |dbt_valid_to           |
--------------+------------------+----------+-----------------------+-----------------------+-----------------------+
             1|NY                |2022-06-29|2024-07-05 13:41:12.259|2024-07-05 13:41:12.259|2024-07-05 13:42:40.603|
             1|CA                |2022-06-29|2024-07-05 13:42:40.603|2024-07-05 13:42:40.603|2024-07-05 13:45:18.712|
             1|CA                |2022-06-20|2024-07-05 13:45:18.712|2024-07-05 13:45:18.712|                       |
             2|NY                |2022-06-29|2024-07-05 13:42:40.603|2024-07-05 13:42:40.603|2024-07-05 13:45:18.712|
             2|NY                |2022-06-12|2024-07-05 13:45:18.712|2024-07-05 13:45:18.712|                       |
```

You can see `summons_number=1` changes in 2 times.

*   First time `registration_state` changed from `NY` to `CA`
*   Second time `issue_date` changed from `2022–06–29` to `2022–06–20`

Snapshots in dbt are a powerful way to track changes to your data over time. By capturing the state of records at different points in time, you can perform historical analysis, audit changes, and maintain a record of how your data has evolved.

References
----------
*   [https://docs.getdbt.com/docs/build/snapshots](https://docs.getdbt.com/docs/build/snapshots)
*   [https://duckdb.org/docs/index](https://duckdb.org/docs/index)