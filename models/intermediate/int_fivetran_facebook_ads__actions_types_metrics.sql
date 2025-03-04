with actions_report as (
    select *
    from {{ ref('stg_fivetran_facebook_ads__ad_performance_v_1_actions') }}

),

action_values_report as (
    select *
    from
        {{ ref('stg_fivetran_facebook_ads__ad_performance_v_1_action_values') }}
),

conversions_report as (
    select *
    from {{ ref('stg_fivetran_facebook_ads__ad_performance_v_1_conversions') }}
),

outbound_clicks_report as (
    select *
    from
        {{ ref('stg_fivetran_facebook_ads__ad_performance_v_1_outbound_clicks') }}
),

actions_metrics as (
    select
        fivetran_id,
        ad_id,
        date,
        sum(value) as action_total_attributions,

        {{ generate_facebook_action_types_metrics("action") }}
    from actions_report
    group by all
),

action_value_metrics as (
    select
        fivetran_id,
        ad_id,
        date,
        sum(value) as action_value_total_attributions,
        {{ generate_facebook_action_types_metrics("value") }}
    from action_values_report

    group by all
),

conversions_metrics as (
    select
        fivetran_id,
        ad_id,
        date,
        sum(value) as conversions
    from conversions_report
    group by all
),

outbound_clicks_metrics as (
    select
        fivetran_id,
        ad_id,
        date,
        sum(value) as outbound_clicks
    from outbound_clicks_report
    group by all
),

metrics_join as (
    select

        actions_metrics.fivetran_id,
        actions_metrics.ad_id,
        actions_metrics.date,
        sum(action_total_attributions) as action_total_attributions,
        ROUND(sum(action_value_total_attributions), 2) as action_value_total_attributions,
        sum(outbound_clicks) as outbound_clicks,
        sum(coalesce(conversions, 0)) as conversions,
        {{ generate_action_types_sum("action", "7d_click") }}
        {{ generate_action_types_sum("action", "1d_view") }}
        {{ generate_action_types_sum("action", "default") }}
        {{ generate_action_types_sum("value", "7d_click") }}
        {{ generate_action_types_sum("value", "1d_view") }}
        {{ generate_action_types_sum("value", "default") }}
    from actions_metrics
    left join action_value_metrics
        on
            actions_metrics.ad_id = action_value_metrics.ad_id
            and actions_metrics.date = action_value_metrics.date
            and actions_metrics.fivetran_id = action_value_metrics.fivetran_id



    left join conversions_metrics
        on
            actions_metrics.ad_id = conversions_metrics.ad_id
            and actions_metrics.date = conversions_metrics.date
            and actions_metrics.fivetran_id = conversions_metrics.fivetran_id


    left join outbound_clicks_metrics
        on
            actions_metrics.ad_id = outbound_clicks_metrics.ad_id
            and actions_metrics.date = outbound_clicks_metrics.date
            and actions_metrics.fivetran_id = outbound_clicks_metrics.fivetran_id
    group by all

)

select * from metrics_join
