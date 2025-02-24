{% macro generate_action_types_columns(action_or_value, time_window) %}
    {%- set action_types_list = ["offsite_conversion.fb_pixel_add_to_cart", "offsite_conversion.fb_pixel_view_content",
    "offsite_conversion.fb_pixel_purchase", "link_click", "lead", "landing_page_view"] %}

    {%- if var('custom_action_types') is not none and var('custom_action_types') != "None" -%}
    {% set action_types_list = var('custom_action_types').split(',') + action_types_list %}
    {%- endif -%}

    {%- for each_action_type in action_types_list -%}

    {% set each_action_type_cleaned = each_action_type|lower|replace(".", "_") %}
    {%- if action_or_value == "action" -%}
    CAST( {{ each_action_type_cleaned }}_{{ time_window }}_{{ action_or_value }} AS INT64 ) {{ each_action_type_cleaned }}_{{ time_window }}_{{ action_or_value }},
    {% else %}
    CAST( {{ each_action_type_cleaned }}_{{ time_window }}_{{ action_or_value }} AS FLOAT64 ) {{ each_action_type_cleaned }}_{{ time_window }}_{{ action_or_value }},
    {%- endif -%}
    {%- endfor -%}
{% endmacro %}
