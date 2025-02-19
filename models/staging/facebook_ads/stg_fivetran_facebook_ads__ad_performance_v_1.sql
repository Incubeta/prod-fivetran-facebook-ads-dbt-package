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
SELECT
    _fivetran_synced,
source_relation,
    _fivetran_id AS  fivetran_id,
    date AS  day,
    ad_id AS  ad_id,
    ad_name AS  ad_name,
    adset_id AS  adset_id,
    adset_name AS  adset_name,
    account_id AS  account_id,
    account_name AS  account_name,
    campaign_id AS  campaign_id,
    campaign_name AS  campaign_name,
    device_platform AS  device_platform,
    objective AS  objective,
    publisher_platform AS  publisher_platform,
    attribution_setting AS  attribution_setting,
    TRIM(account_currency) AS  account_currency,
    CAST(canvas_avg_view_percent AS FLOAT) AS  canvas_avg_view_percent,
    CAST(canvas_avg_view_time AS FLOAT) AS  canvas_avg_view_time,
    CAST(frequency AS FLOAT) AS  frequency,
    CAST(clicks AS INT64) AS  clicks,
    CAST(impressions AS INT64) AS impressions,
    CAST(inline_post_engagement AS INT64) AS  inline_post_engagement,
    CAST(inline_link_clicks AS INT64) AS inline_link_clicks,
    CAST(reach AS INT64) AS reach,
    CAST(spend as FLOAT) AS cost

from fields
