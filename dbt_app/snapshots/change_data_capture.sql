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
