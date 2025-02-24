with actions_report as (
    select *
    from {{ ref('stg_fivetran_facebook_ads__ad_performance_v_1_actions') }}

),

action_values_report as (
    select *
    from
        {{ ref('stg_fivetran_facebooks_ads__ad_performance_v_1_action_values') }}
),

conversions_report as (
    select *
    from {{ ref('stg_fivetran_facebooks_ads__ad_performance_v_1_conversions') }}
),

outbound_clicks_report as (
    select *
    from
        {{ ref('stg_fivetran_facebooks_ads__ad_performance_v_1_outbound_clicks') }}
),

actions_metrics as (
    select
        _fivetran_synced,
        source_relation,
        action_type,
        idx,
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
        _fivetran_synced,
        source_relation,
        action_type,
        idx,
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
        _fivetran_synced,
        source_relation,
        action_type,
        idx,
        fivetran_id,
        ad_id,
        date,
        sum(value) as conversions
    from conversions_report
    group by all
),

outbound_clicks_metrics as (
    select
        _fivetran_synced,
        source_relation,
        action_type,
        idx,
        fivetran_id,
        ad_id,
        date,
        sum(value) as outbound_clicks
    from outbound_clicks_report
    group by all
),

metrics_join as (
    select
        actions_metrics._fivetran_synced,
        actions_metrics.source_relation,

        actions_metrics.fivetran_id,
        actions_metrics.ad_id,
        actions_metrics.date,
        sum(action_total_attributions) as action_total_attributions,
        sum(action_value_total_attributions) as action_value_total_attributions,
        sum(outbound_clicks) as outbound_clicks,
        sum(conversions) as conversions,
        {{ generate_action_types_sum("action", "7d_click") }}
        {{ generate_action_types_sum("action", "1d_view") }}
        {{ generate_action_types_sum("action", "default") }}
        {{ generate_action_types_sum("value", "7d_click") }}
        {{ generate_action_types_sum("value", "1d_view") }}
        {{ generate_action_types_sum("value", "default") }}
    from actions_metrics
    left join action_value_metrics
        on
            actions_metrics.source_relation
            = action_value_metrics.source_relation
            and actions_metrics.action_type = action_value_metrics.action_type
            and actions_metrics.idx = action_value_metrics.idx
            and actions_metrics.ad_id = action_value_metrics.ad_id
            and actions_metrics.date = action_value_metrics.date



    left join conversions_metrics
        on
            actions_metrics.source_relation
            = conversions_metrics.source_relation
            and actions_metrics.action_type = conversions_metrics.action_type
            and actions_metrics.idx = conversions_metrics.idx
            and actions_metrics.ad_id = conversions_metrics.ad_id
            and actions_metrics.date = conversions_metrics.date


    left join outbound_clicks_metrics
        on
            actions_metrics.source_relation
            = outbound_clicks_metrics.source_relation
            and actions_metrics.action_type
            = outbound_clicks_metrics.action_type
            and actions_metrics.idx = outbound_clicks_metrics.idx
            and actions_metrics.ad_id = outbound_clicks_metrics.ad_id
            and actions_metrics.date = outbound_clicks_metrics.date
    group by all

)

select * from metrics_join
