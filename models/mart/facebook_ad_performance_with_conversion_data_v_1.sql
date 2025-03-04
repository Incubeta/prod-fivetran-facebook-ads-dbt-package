with performance_report as (
    select *
    from {{ ref('int_fivetran_facebook_ads__ad_performance_v_1') }}
{% if is_incremental() %}
WHERE day >= {{ dbt_date.n_days_ago(var('days_ago', 1), date="day") }}
{% endif %}
),

combined_actions as (
    select *
    from {{ ref('int_fivetran_facebook_ads_combined_actions') }}
{% if is_incremental() %}
WHERE date >= {{ dbt_date.n_days_ago(var('days_ago', 1), date="date") }}
{% endif %}
),

report as (
    select
        performance_report._fivetran_synced,
        null as action_type,
        null as actrion_attribution_window,
        campaign_id,
        day,
        objective,
        publisher_platform,
        attribution_setting,
        action_total_attributions as actions,
        account_id,
        null as unique_actions,
        action_value_total_attributions as action_values,
        performance_report.ad_id,
        null as unique_clicks,
        adset_name,
        cost,
        ad_name,
        account_name,
        inline_link_clicks,
        null as date_stop,
        frequency,
        outbound_clicks,
        adset_id,
        impressions,
        reach,
        inline_post_engagement,
        account_currency,
        clicks,
        device_platform,
        campaign_name,
        null as ad_creative_url_tags,
        null as ad_creative_image_url,
        video_p25_watched_actions,
        video_p100_watched_actions,
        canvas_avg_view_percent,
        video_p75_watched_actions,
        video_avg_time_watched_actions as video_time_watched_actions,
        video_30_sec_watched_actions,
        null as video_15_sec_watched_actions,
        conversions,
        video_p95_watched_actions,
        video_p50_watched_actions,
        video_play_actions,
        canvas_avg_view_time,
        {{ generate_action_types_columns("action", "7d_click") }}
        {{ generate_action_types_columns("action", "1d_view") }}
        {{ generate_action_types_columns("action", "default") }}
        {{ generate_action_types_columns("value", "7d_click") }}
        {{ generate_action_types_columns("value", "1d_view") }}
        {{ generate_action_types_columns("value", "default") }}
        {{ add_fields("campaign_name") }},
        {{ add_adname_split("ad_name") }}

    -- cost/ exchange_source.ex_rate _gbp_cost,
    -- (offsite_conversion_fb_pixel_purchase_default_value) / exchange_source.ex_rate _gb_revenue



    from performance_report
    left join combined_actions
        on
            performance_report.day = combined_actions.date
            and performance_report.ad_id = combined_actions.ad_id

/*
left join exchange_source
on performance_report.day = exchange_source.date
and performance_report.account_currency = exchange_source.currency_code
*/

)

select * from report
