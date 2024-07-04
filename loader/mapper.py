import pyarrow as pa

# Define a function to map PyArrow types to DuckDB types
def map_pyarrow_to_duckdb(pa_type):
    if pa.types.is_int8(pa_type) or pa.types.is_int16(pa_type) or pa.types.is_int32(pa_type):
        return 'INTEGER'
    elif pa.types.is_int64(pa_type):
        return 'BIGINT'
    elif pa.types.is_float32(pa_type) or pa.types.is_float64(pa_type):
        return 'DOUBLE'
    elif pa.types.is_string(pa_type):
        return 'VARCHAR'
    elif pa.types.is_boolean(pa_type):
        return 'BOOLEAN'
    elif pa.types.is_timestamp(pa_type):
        return 'TIMESTAMP'
    # Add more mappings as needed
    else:
        return 'VARCHAR'  # Default to VARCHAR for unhandled types
