{% macro to_utc_date_if_not_null(date_column) %}

    CASE
    WHEN {{date_column}} IS NOT NULL
        AND {{date_column}} <> ''
        AND LENGTH({{date_column}}) = 10

        THEN toDate({{date_column}}, 'UTC')
    ELSE
        NULL
    END

{% endmacro %}