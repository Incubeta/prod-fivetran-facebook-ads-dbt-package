{% macro fivetran_incremental_filter() %}

{% if is_incremental() %}
{% set columns = dbt_utils.get_filtered_columns_in_relation(from=this) %}
    {% if "_fivetran_synced" in columns %}

    where _fivetran_synced >=(select coalesce(max(_fivetran_synced), '1900-01-01 00:00:00') from {{this}})
    {%endif%}
{% endif %}

{% endmacro %}
