{% macro generate_facebook_action_types_metrics(action_or_value, metric_field) %}

    {%- set action_types_list = ["offsite_conversion.fb_pixel_add_to_cart", "offsite_conversion.fb_pixel_view_content",
    "offsite_conversion.fb_pixel_purchase", "link_click", "lead", "landing_page_view"] -%}

    {%- if var('custom_action_types', 'None') != "None" -%}
        {%- set action_types_list = var('custom_action_types').split(',') + action_types_list -%}
    {%- endif -%}

    {%- if action_or_value == "action" -%}
    {%- for each_action_type in action_types_list %}
    SUM(IF(action_type = '{{ each_action_type }}', IFNULL(_1_d_view, 0), 0))
        AS {{ each_action_type|lower|replace(".", "_") }}_1d_view_action,
    SUM(IF(action_type = '{{ each_action_type }}', IFNULL(_7_d_click, 0), 0))
        AS {{ each_action_type|lower|replace(".", "_") }}_7d_click_action,
    SUM(IF(action_type = '{{ each_action_type }}', IFNULL(value, 0), 0))
        AS {{ each_action_type|lower|replace(".", "_") }}_default_action,
    {%- endfor -%}

    {% else %}

    {%- for each_action_type in action_types_list %}
    SUM(IF(action_type = '{{ each_action_type }}', IFNULL(_1_d_view, 0), 0))
        AS {{ each_action_type|lower|replace(".", "_") }}_1d_view_value,
    SUM(IF(action_type = '{{ each_action_type }}', IFNULL(_7_d_click, 0), 0))
        AS {{ each_action_type|lower|replace(".", "_") }}_7d_click_value,
    SUM(IF(action_type = '{{ each_action_type }}', IFNULL(value, 0), 0))
        AS {{ each_action_type|lower|replace(".", "_") }}_default_value,
    {%- endfor -%}

    {%- endif -%}

{% endmacro %}
