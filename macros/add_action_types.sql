{% macro add_action_types() %}

    {% set action_types_list = ["offsite_conversion.fb_pixel_add_to_cart", "offsite_conversion.fb_pixel_view_content",
        "offsite_conversion.fb_pixel_purchase", "link_click", "lead", "landing_page_view"] %}
    {% set action_attribution_windows_list = ["1d_view", "7d_click", "default"] %}


    {%- for each_action_type in action_types_list %}
        {%- for each_action_attribution_window in action_attribution_windows_list %}
            CASE WHEN action_type = "{{ each_action_type }}" AND action_attribution_window = "{{ each_action_attribution_window }}" THEN IFNULL(SAFE_CAST(actions as INT64), 0) ELSE 0 END AS {{ each_action_type|lower|replace(".", "_") }}_{{ each_action_attribution_window }}_action,
            CASE WHEN action_type = "{{ each_action_type }}" AND action_attribution_window = "{{ each_action_attribution_window }}" THEN IFNULL(SAFE_CAST(action_values as FLOAT64), 0) ELSE 0 END AS {{ each_action_type|lower|replace(".", "_") }}_{{ each_action_attribution_window }}_value,
        {%- endfor -%}
    {%- endfor -%}

    {% if var('custom_action_types') is not none and var('custom_action_types') != "None"  %}
        {% set custom_action_types = var('custom_action_types').split(',') %}  

        {% for custom_action in custom_action_types%}
            {%- for each_action_attribution_window in action_attribution_windows_list %}
            CASE WHEN action_type = "{{ custom_action }}" AND action_attribution_window = "{{ each_action_attribution_window }}" THEN IFNULL(SAFE_CAST(actions as INT64), 0) ELSE 0 END AS {{ custom_action|lower|replace(".", "_") }}_{{ each_action_attribution_window }}_action,
            CASE WHEN action_type = "{{ custom_action }}" AND action_attribution_window = "{{ each_action_attribution_window }}" THEN IFNULL(SAFE_CAST(action_values as FLOAT64), 0) ELSE 0 END AS {{ custom_action|lower|replace(".", "_") }}_{{ each_action_attribution_window }}_value
            {%- if not loop.last -%},{%- endif -%}
        {%- endfor -%}
        {% if not loop.last %},{% endif %}
        {% endfor %}
    {%- endif -%}

{% endmacro %}
