{% macro integer_like_string_date_to_date(column_name) %}
    if(
        match("{{ column_name }}", '^\d{8}$') AND toDateOrNull(concat(substring("{{ column_name }}", 1, 4), '-', substring("{{ column_name }}", 5, 2), '-', substring("{{ column_name }}", 7, 2))) IS NOT NULL,
        toDate(concat(substring("{{ column_name }}", 1, 4), '-', substring("{{ column_name }}", 5, 2), '-', substring("{{ column_name }}", 7, 2))),
        NULL
    )
{% endmacro %}