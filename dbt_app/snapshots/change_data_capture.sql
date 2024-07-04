{% snapshot worldcheck_deltalake %}

{{
    config(
        target_schema='',
        unique_key='summons_number',
        strategy='check',
        check_cols=['summons_number']
    )
}}

select * from {{ ref('bronze_parking_violations') }}

{% endsnapshot %}
