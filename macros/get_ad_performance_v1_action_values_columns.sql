{% macro get_ad_performance_v1_action_values_columns() %}
{% set columns = [
    {"name": "_fivetran_id", "datatype": dbt.type_string()},
    {"name": "_fivetran_synced", "datatype": dbt.type_timestamp()},
    {"name": "action_type", "datatype": dbt.type_string()},
    {"name": "ad_id", "datatype": dbt.type_string()},
    {"name": "date", "datatype": "date"},
    {"name": "index", "datatype": dbt.type_int()},
    {"name": "value", "datatype": dbt.type_float()},
    {"name": "_1_d_view", "datatype": dbt.type_float()},
    {"name": "_7_d_click", "datatype": dbt.type_float()}
] %}


{{ return(columns)}}
{% endmacro %}
