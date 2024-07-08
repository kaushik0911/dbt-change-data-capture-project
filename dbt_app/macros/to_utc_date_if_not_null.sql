{% macro to_utc_date_if_not_null(column_name) %}

    CASE
    WHEN {{column_name}} IS NOT NULL
        AND {{column_name}} <> ''
        AND LENGTH({{column_name}}) = 10

        THEN toDate({{column_name}}, 'UTC')
    ELSE
        NULL
    END

{% endmacro %}