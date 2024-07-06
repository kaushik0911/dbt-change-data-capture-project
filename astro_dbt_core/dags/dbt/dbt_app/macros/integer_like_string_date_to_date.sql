{% macro integer_like_string_date_to_date(date_column) %}

    TRY_CAST(
        SUBSTR(CAST({{ date_column }} AS VARCHAR), 1, 4) || '-' ||
        SUBSTR(CAST({{ date_column }} AS VARCHAR), 5, 2) || '-' ||
        SUBSTR(CAST({{ date_column }} AS VARCHAR), 7, 2)
    AS DATE)

{% endmacro %}