{% macro get_ad_performance_v1_columns() %}


{% set columns = [
{"name": "_fivetran_id", "datatype": dbt.type_string()},

    {"name": "_fivetran_synced", "datatype": dbt.type_timestamp()},
    {"name": "ad_id", "datatype": dbt.type_string()},
    {"name": "ad_name", "datatype": dbt.type_string()},
    {"name": "adset_name", "datatype": dbt.type_string()},
    {"name": "adset_id", "datatype": dbt.type_string()},
    {"name": "date", "datatype": "date"},
    {"name": "account_id", "datatype": dbt.type_int()},
    {"name": "account_name", "datatype": dbt.type_string()},
{"name": "campaign_id", "datatype": dbt.type_string()},
{"name": "campaign_name", "datatype": dbt.type_string()},
{"name": "device_platform", "datatype": dbt.type_string()},
{"name": "objective", "datatype": dbt.type_string()},
{"name": "publisher_platform", "datatype": dbt.type_string()},
{"name": "attribution_setting", "datatype": dbt.type_string()},
{"name": "account_currency", "datatype": dbt.type_string()},
{"name": "canvas_avg_view_percent", "datatype": dbt.type_float()},
{"name": "canvas_avg_view_time", "datatype": dbt.type_float()},
{"name": "frequency", "datatype": dbt.type_float()},
{"name": "clicks", "datatype": dbt.type_int()},
{"name": "impressions", "datatype": dbt.type_int()},
{"name": "inline_post_engagement", "datatype": dbt.type_int()},
{"name": "inline_link_clicks", "datatype": dbt.type_int()},
{"name": "reach", "datatype": dbt.type_int()},
{"name": "spend", "datatype": dbt.type_float()},
]
%}

{{ return(columns)}}



{% endmacro %}
