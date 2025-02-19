with base as (
    select *
    from {{ ref('stg_fivetran_facebook_ads__ad_performance_v_1_tmp') }}
),

fields as (
    select
        _fivetran_synced,
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
    adset_id,
    adset_name,
    account_id,
    account_name,
    campaign_id,
    campaign_name,
    device_platform,
    objective,
    publisher_platform,
    attribution_setting,
    TRIM(account_currency) as account_currency,
    CAST(canvas_avg_view_percent as FLOAT) as canvas_avg_view_percent,
    CAST(canvas_avg_view_time as FLOAT) as canvas_avg_view_time,
    CAST(frequency as FLOAT) as frequency,
    CAST(clicks as INT64) as clicks,
    CAST(impressions as INT64) as impressions,
    CAST(inline_post_engagement as INT64) as inline_post_engagement,
    CAST(inline_link_clicks as INT64) as inline_link_clicks,
    CAST(reach as INT64) as reach,
    CAST(spend as FLOAT) as cost

from fields
