{% macro add_adname_split(ad_name) %}

    {% if ad_name != "null" %}

        {% if  var('campaign_segment_separator') is not none and var('campaign_segment_separator') != "None" %}

            {% set def = var('campaign_segment_separator') %}
            {% for i in range(15) %}
                {{ split_part(string_text=ad_name, delimiter_text="'"+def+"'", part_number=i+1) }} as ad_name_sub_{{i+1}}
                {% if not loop.last %},{% endif %}
            {% endfor %}

        {% elif var('campaign_segment_separator') is defined and var('campaign_segment_separator') is none %}

            {% set def = "-" %}
            {% for i in range(15) %}
                {{ split_part(string_text=ad_name, delimiter_text="'"+def+"'", part_number=i+1) }} as ad_name_sub_{{i+1}}
                {% if not loop.last %},{% endif %}
            {% endfor %}

        {% elif var('campaign_segment_separator') is not none and var('campaign_segment_separator') == "None" %}

            {% set def = "-" %}
            {% for i in range(15) %}
                {{ split_part(string_text=ad_name, delimiter_text="'"+def+"'", part_number=i+1) }} as ad_name_sub_{{i+1}}
                {% if not loop.last %},{% endif %}
            {% endfor %}

        {% endif %}

    {% endif %}

{% endmacro %}
