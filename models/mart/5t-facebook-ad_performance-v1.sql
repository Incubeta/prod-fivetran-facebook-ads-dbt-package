
{{
  config(
    alias= '5t-facebook-ad_performance-v1'
  )
}}


WITH
  actions AS (
  SELECT
    SAFE_CAST(_fivetran_id AS STRING) fivetran_id,
    index AS idx,
    SAFE_CAST(date AS DATE) AS day,
    ad_id,
    action_type,
    SUM(value) AS action_total_attributions,
    {{ generate_facebook_action_types_metrics("action") }}
  FROM
      {{ source('facebook_ads',  'facebook_ad_performance_v_1_actions') }}
  GROUP BY 1,2,3,4,5),
  actions_values AS (
  SELECT
    SAFE_CAST(_fivetran_id AS STRING) fivetran_id,
    index AS idx,
    SAFE_CAST(date AS DATE) AS day,
    ad_id,
    action_type,
    SUM(value) AS action_value_total_attributions,
    {{ generate_facebook_action_types_metrics("value") }}
  FROM
    {{source('facebook_ads', 'facebook_ad_performance_v_1_action_values')}}
  GROUP BY 1,2,3,4,5),
  p25 AS (
  SELECT
    SAFE_CAST(_fivetran_id AS STRING) fivetran_id,
    index AS idx,
    SAFE_CAST(date AS DATE) AS day,
    ad_id,
    action_type,
    SUM(value) AS action_video_p25_watched
  FROM
    {{source('facebook_ads', 'facebook_ad_performance_v_1_video_p_25_watched_actions') }}
  GROUP BY 1,2,3,4,5),
  p50 AS (
  SELECT
    SAFE_CAST(_fivetran_id AS STRING) fivetran_id,
    index AS idx,
    SAFE_CAST(date AS DATE) AS day,
    ad_id,
    action_type,
    SUM(value) AS action_video_p50_watched
  FROM
    {{ source('facebook_ads', 'facebook_ad_performance_v_1_video_p_50_watched_actions') }}
  GROUP BY 1,2,3,4,5),
  p75 AS (
  SELECT
    SAFE_CAST(_fivetran_id AS STRING) fivetran_id,
    index AS idx,
    SAFE_CAST(date AS DATE) AS day,
    ad_id,
    action_type,
    SUM(value) AS action_video_p75_watched
  FROM
    {{ source('facebook_ads', 'facebook_ad_performance_v_1_video_p_75_watched_actions') }}
  GROUP BY 1,2,3,4,5),
  p95 AS (
  SELECT
    SAFE_CAST(_fivetran_id AS STRING) fivetran_id,
    index AS idx,
    SAFE_CAST(date AS DATE) AS day,
    ad_id,
    action_type,
    SUM(value) AS action_video_p95_watched
  FROM
    {{ source('facebook_ads', 'facebook_ad_performance_v_1_video_p_95_watched_actions') }}
  GROUP BY 1,2,3,4,5),
  p100 AS (
  SELECT
    SAFE_CAST(_fivetran_id AS STRING) fivetran_id,
    index AS idx,
    SAFE_CAST(date AS DATE) AS day,
    ad_id,
    action_type,
    SUM(value) AS action_video_p100_watched
  FROM
    {{ source('facebook_ads', 'facebook_ad_performance_v_1_video_p_100_watched_actions') }}
  GROUP BY 1,2,3,4,5),
  video_30_sec AS (
  SELECT
    SAFE_CAST(_fivetran_id AS STRING) fivetran_id,
    index AS idx,
    SAFE_CAST(date AS DATE) AS day,
    ad_id,
    action_type,
    SUM(value) AS action_video_30_sec_watched,
  FROM
    {{ source('facebook_ads', 'facebook_ad_performance_v_1_video_30_sec_watched_actions') }}
  GROUP BY 1,2,3,4,5),
  video_avg_time AS (
  SELECT
    SAFE_CAST(_fivetran_id AS STRING) fivetran_id,
    index AS idx,
    SAFE_CAST(date AS DATE) AS day,
    ad_id,
    action_type,
    SUM(value) AS action_video_avg_time,
  FROM
    {{ source('facebook_ads', 'facebook_ad_performance_v_1_video_avg_time_watched_actions') }}
  GROUP BY 1,2,3,4,5),
  video_play AS (
  SELECT
    SAFE_CAST(_fivetran_id AS STRING) fivetran_id,
    index AS idx,
    SAFE_CAST(date AS DATE) AS day,
    ad_id,
    action_type,
    SUM(value) AS action_video_play,
  FROM
    {{ source('facebook_ads', 'facebook_ad_performance_v_1_video_play_actions') }}
  GROUP BY 1,2,3,4,5),
  outbound_clicks AS (
  SELECT
    SAFE_CAST(_fivetran_id AS STRING) fivetran_id,
    index AS idx,
    SAFE_CAST(date AS DATE) AS day,
    ad_id,
    action_type,
    SUM(value) AS action_outbound_clicks,
  FROM
    {{ source('facebook_ads', 'facebook_ad_performance_v_1_outbound_clicks') }}
  GROUP BY 1,2,3,4,5),
  actions_combined AS (
  SELECT
    COALESCE(A.fivetran_id, B.fivetran_id, C.fivetran_id, D.fivetran_id, E.fivetran_id, F.fivetran_id, G.fivetran_id, H.fivetran_id, I.fivetran_id, J.fivetran_id, K.fivetran_id) fivetran_id,
    COALESCE(A.day, B.day, C.day, D.day, E.day, F.day, G.day, H.day, I.day, J.day, K.day) day,
    COALESCE(A.ad_id, B.ad_id, C.ad_id, D.ad_id, E.ad_id, F.ad_id, G.ad_id, H.ad_id, I.ad_id, J.ad_id, K.ad_id) ad_id,
    SUM(action_video_p25_watched) video_p25_watched_actions,
    SUM(action_video_p50_watched) video_p50_watched_actions,
    SUM(action_video_p75_watched) video_p75_watched_actions,
    SUM(action_video_p95_watched) video_p95_watched_actions,
    SUM(action_video_p100_watched) video_p100_watched_actions,
    SUM(action_video_30_sec_watched) video_30_sec_watched_actions,
    SUM(action_video_avg_time) video_avg_time_watched_actions,
    SUM(action_video_play) video_play_actions,
    SUM(action_outbound_clicks) outbound_clicks,
    SUM(action_total_attributions) action_total_attributions,
    SUM(action_value_total_attributions) action_value_total_attributions,
    {{ generate_action_types_sum("action", "7d_click") }}
    {{ generate_action_types_sum("action", "1d_view") }}
    {{ generate_action_types_sum("action", "default") }}
    {{ generate_action_types_sum("value", "7d_click") }}
    {{ generate_action_types_sum("value", "1d_view") }}
    {{ generate_action_types_sum("value", "default") }}
  FROM
    actions A
  FULL JOIN
    actions_values B
  ON
    A.day=B.day
    AND A.ad_id=B.ad_id
    AND A.action_type=B.action_type
    AND A.fivetran_id=B.fivetran_id
    AND A.idx=B.idx
  FULL JOIN
    p25 C
  ON
    A.day=C.day
    AND A.ad_id=C.ad_id
    AND A.action_type=C.action_type
    AND A.fivetran_id=C.fivetran_id
    AND A.idx=C.idx
  FULL JOIN
    p50 D
  ON
    A.day=D.day
    AND A.ad_id=D.ad_id
    AND A.action_type=D.action_type
    AND A.fivetran_id=D.fivetran_id
    AND A.idx=D.idx
  FULL JOIN
    p75 E
  ON
    A.day=E.day
    AND A.ad_id=E.ad_id
    AND A.action_type=E.action_type
    AND A.fivetran_id=E.fivetran_id
    AND A.idx=E.idx
  FULL JOIN
    p95 F
  ON
    A.day=F.day
    AND A.ad_id=F.ad_id
    AND A.action_type=F.action_type
    AND A.fivetran_id=F.fivetran_id
    AND A.idx=F.idx
  FULL JOIN
    p100 G
  ON
    A.day=G.day
    AND A.ad_id=G.ad_id
    AND A.action_type=G.action_type
    AND A.fivetran_id=G.fivetran_id
    AND A.idx=G.idx
  FULL JOIN
    video_30_sec H
  ON
    A.day=H.day
    AND A.ad_id=H.ad_id
    AND A.action_type=H.action_type
    AND A.fivetran_id=H.fivetran_id
    AND A.idx=H.idx
  FULL JOIN
    video_avg_time I
  ON
    A.day=I.day
    AND A.ad_id=I.ad_id
    AND A.action_type=I.action_type
    AND A.fivetran_id=I.fivetran_id
    AND A.idx=I.idx
  FULL JOIN
    video_play J
  ON
    A.day=J.day
    AND A.ad_id=J.ad_id
    AND A.action_type=J.action_type
    AND A.fivetran_id=J.fivetran_id
    AND A.idx=J.idx
  FULL JOIN
    outbound_clicks K
  ON
    A.day=K.day
    AND A.ad_id=K.ad_id
    AND A.action_type=K.action_type
    AND A.fivetran_id=K.fivetran_id
    AND A.idx=K.idx
  GROUP BY 1,2,3),

facebook_ads AS (
SELECT
    SAFE_CAST(_fivetran_id AS STRING) fivetran_id,
    SAFE_CAST(date AS DATE) day,
    SAFE_CAST(ad_id AS STRING) ad_id,
    SAFE_CAST(ad_name AS STRING) ad_name,
    SAFE_CAST(adset_id AS STRING) adset_id,
    SAFE_CAST(adset_name AS STRING) adset_name,
    SAFE_CAST(account_id AS string) account_id,
    SAFE_CAST(account_name AS STRING) account_name,
    SAFE_CAST(campaign_id AS STRING) campaign_id,
    SAFE_CAST(campaign_name AS STRING) campaign_name,
    SAFE_CAST(device_platform AS STRING) device_platform,
    SAFE_CAST(objective AS STRING) objective,
    SAFE_CAST(publisher_platform AS STRING) publisher_platform,
    SAFE_CAST(attribution_setting AS STRING) attribution_setting,
    SAFE_CAST(TRIM(account_currency) AS STRING) account_currency,
    SUM(SAFE_CAST(canvas_avg_view_percent AS FLOAT64)) canvas_avg_view_percent,
    SUM(SAFE_CAST(canvas_avg_view_time AS FLOAT64)) canvas_avg_view_time,
    SUM(SAFE_CAST(frequency AS FLOAT64)) frequency,
    SUM(SAFE_CAST(clicks AS INT64)) clicks,
    SUM(SAFE_CAST(impressions AS INT64) )impressions,
    SUM(SAFE_CAST(inline_post_engagement AS INT64)) inline_post_engagement,
    SUM(SAFE_CAST(inline_link_clicks AS INT64)) inline_link_clicks,
    SUM(SAFE_CAST(reach AS INT64)) reach,
    SUM(SAFE_CAST(spend AS FLOAT64)) cost
FROM
    {{ source('facebook_ads', 'facebook_ad_performance_v_1') }}
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15 ),
exchange_source AS (
    select 
        date,
        currency as currency_code,
        rate as ex_rate
    from {{ref('openexchange_rates','stg_openexchange_rates__openexchange_report_v1')}}
)

SELECT
    SAFE_CAST(NULL AS STRING) action_type,
    SAFE_CAST(NULL AS STRING) action_attribution_window,
    SAFE_CAST(campaign_id AS STRING) campaign_id,
    SAFE_CAST(source_a.day AS DATE) day,
    SAFE_CAST(objective AS STRING) objective,
    SAFE_CAST(publisher_platform AS STRING)	publisher_platform,
    SAFE_CAST(attribution_setting AS STRING) attribution_setting,
    SAFE_CAST(action_total_attributions AS INT64) actions,
    SAFE_CAST(account_id AS STRING) account_id,
    SAFE_CAST("0" AS INT64) unique_actions,
    SAFE_CAST(action_value_total_attributions AS FLOAT64) action_values,
    SAFE_CAST(source_a.ad_id AS STRING)	ad_id,
    SAFE_CAST("0" AS INT64) unique_clicks,
    SAFE_CAST(adset_name AS STRING)	adset_name,
    SAFE_CAST(cost AS FLOAT64) cost,
    SAFE_CAST(ad_name AS STRING) ad_name,
    SAFE_CAST(account_name AS STRING) account_name,
    SAFE_CAST(inline_link_clicks AS INT64) inline_link_clicks,
    SAFE_CAST(NULL AS DATE) date_stop,
    SAFE_CAST(frequency AS FLOAT64)	frequency,
    SAFE_CAST(outbound_clicks AS INT64)	outbound_clicks,
    SAFE_CAST(adset_id AS STRING) adset_id,
    SAFE_CAST(impressions AS INT64)	impressions,
    SAFE_CAST(reach AS INT64) reach,
    SAFE_CAST(inline_post_engagement AS INT64) inline_post_engagement,
    SAFE_CAST(TRIM(account_currency) AS STRING)	account_currency,
    SAFE_CAST(clicks AS INT64) clicks,
    SAFE_CAST(device_platform AS STRING) device_platform,
    SAFE_CAST(campaign_name AS STRING) campaign_name,
    SAFE_CAST(NULL AS STRING) ad_creative_url_tags,
    SAFE_CAST(NULL AS STRING) ad_creative_image_url,
    SAFE_CAST(video_p25_watched_actions AS INT64) video_p25_watched_actions,
    SAFE_CAST(video_p100_watched_actions AS INT64) video_p100_watched_actions,
    SAFE_CAST(canvas_avg_view_percent AS INT64) canvas_avg_view_percent,
    SAFE_CAST(video_p75_watched_actions AS INT64) video_p75_watched_actions,
    SAFE_CAST(video_avg_time_watched_actions AS INT64) video_time_watched_actions,
    SAFE_CAST(video_30_sec_watched_actions AS INT64) video_30_sec_watched_actions,
    SAFE_CAST("0" AS INT64) video_15_sec_watched_actions,
    SAFE_CAST("0" AS INT64)	conversions,
    SAFE_CAST(video_p95_watched_actions AS INT64) video_p95_watched_actions,
    SAFE_CAST(video_p50_watched_actions AS INT64) video_p50_watched_actions,
    SAFE_CAST(video_play_actions AS INT64) video_play_actions,
    SAFE_CAST(canvas_avg_view_time AS INT64) canvas_avg_view_time,
    {{ generate_action_types_columns("action", "7d_click") }}
    {{ generate_action_types_columns("action", "1d_view") }}
    {{ generate_action_types_columns("action", "default") }}
    {{ generate_action_types_columns("value", "7d_click") }}
    {{ generate_action_types_columns("value", "1d_view") }}
    {{ generate_action_types_columns("value", "default") }}
    {{ add_fields("campaign_name") }}, /* Replace with the report's campaign name field */
    {{ add_adname_split("ad_name") }}, /* Replace with the report's ad name field */
    "facebook_ad_performance_v_1" AS raw_origin,
    cost / exchange_source.ex_rate _gbp_cost,
    (offsite_conversion_fb_pixel_purchase_default_value) / exchange_source.ex_rate _gbp_revenue
FROM
  facebook_ads source_a
LEFT JOIN
  actions_combined B
ON
  source_a.day=B.day
  AND source_a.ad_id=B.ad_id
  AND source_a.fivetran_id=B.fivetran_id

LEFT JOIN exchange_source
    on source_a.day = exchange_source.date
    and source_a.account_currency = exchange_source.currency_code



