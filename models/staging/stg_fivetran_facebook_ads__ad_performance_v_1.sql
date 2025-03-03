with base as (
    select *
    from {{ ref('stg_fivetran_facebook_ads__ad_performance_v_1_tmp') }}
),

fields as (
    select
    {{
        fivetran_utils.fill_staging_columns(
            source_columns=adapter.get_columns_in_relation(ref('stg_fivetran_facebook_ads__ad_performance_v_1_tmp')),
            staging_columns=get_ad_performance_v1_columns()
        )
        }}

        {{
            fivetran_utils.source_relation()
            }}

    from base
)

select
    _fivetran_synced,
    source_relation,
    _fivetran_id as fivetran_id,
    date as day,
    ad_id,
    ad_name,
    SAFE_CAST(adset_id as STRING) adset_id,
    adset_name,
    SAFE_CAST(account_id as STRING) as account_id,
    account_name,
    SAFE_CAST(campaign_id as STRING) as campaign_id,
    campaign_name,
    device_platform,
    objective,
    publisher_platform,
    attribution_setting,
    TRIM(account_currency) as account_currency,
    SAFE_CAST(PARSE_NUMERIC(canvas_avg_view_percent) as INT64) as canvas_avg_view_percent,
    SAFE_CAST(PARSE_NUMERIC(canvas_avg_view_time) as INT64) as canvas_avg_view_time,
    SAFE_CAST(frequency as FLOAT64) as frequency,
    SAFE_CAST(clicks as INT64) as clicks,
    SAFE_CAST(impressions as INT64) as impressions,
    SAFE_CAST(inline_post_engagement as INT64) as inline_post_engagement,
    SAFE_CAST(inline_link_clicks as INT64) as inline_link_clicks,
    SAFE_CAST(reach as INT64) as reach,
    SAFE_CAST(spend as FLOAT64) as cost

from fields
