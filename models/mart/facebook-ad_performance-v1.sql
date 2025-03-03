
with performance_report as (
    select *
    from {{ ref('int_fivetran_facebook_ads__ad_performance_v_1') }}
	{{ fivetran_incremental_filter() }}
),

combined_actions as (
    select *
    from {{ ref('int_fivetran_facebook_ads_combined_actions') }}
	{{ fivetran_incremental_filter() }}
),

report as (
    select
        unique_id,
        SAFE_CAST(null as STRING) as action_type,
        SAFE_CAST(null as STRING) as action_attribution_window,
        campaign_id,
        day,
        objective,
        publisher_platform,
        attribution_setting,
        action_total_attributions as actions,
        account_id,
        0 as unique_actions,
        action_value_total_attributions as action_values,
        performance_report.ad_id,
        0 as unique_clicks,
        adset_name,
        cost,
        ad_name,
        account_name,
        inline_link_clicks,
        SAFE_CAST(null as DATE) as date_stop,
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
        SAFE_CAST(null as STRING) as ad_creative_url_tags,
        SAFE_CAST(null as STRING) as ad_creative_image_url,
        video_p25_watched_actions,
        video_p100_watched_actions,
        canvas_avg_view_percent,
        video_p75_watched_actions,
        video_avg_time_watched_actions as video_time_watched_actions,
        video_30_sec_watched_actions,
        0 as video_15_sec_watched_actions,
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
        {{ add_adname_split("ad_name") }},

    -- cost/ exchange_source.ex_rate _gbp_cost,
    -- (offsite_conversion_fb_pixel_purchase_default_value) / exchange_source.ex_rate _gb_revenue



    from performance_report
    left join combined_actions
        on
            performance_report.day = combined_actions.date
            and performance_report.ad_id = combined_actions.ad_id
            and performance_report.fivetran_id = combined_actions.fivetran_id

/*
left join exchange_source
on performance_report.day = exchange_source.date
and performance_report.account_currency = exchange_source.currency_code
*/

)

select * from report
