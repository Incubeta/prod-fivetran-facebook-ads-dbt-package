with p25_report as (
    select *
    from
        {{ ref('stg_fivetran_facebooks_ads__ad_performance_v_1_video_p_25_watched_actions') }}

),

p50_report as (
    select *
    from
        {{ ref('stg_fivetran_facebooks_ads__ad_performance_v_1_video_p_50_watched_actions') }}

),

p75_report as (
    select *
    from
        {{ ref('stg_fivetran_facebooks_ads__ad_performance_v_1_video_p_75_watched_actions') }}

),

p95_report as (
    select *
    from
        {{ ref('stg_fivetran_facebooks_ads__ad_performance_v_1_video_p_95_watched_actions') }}

),

p100_report as (
    select *
    from
        {{ ref('stg_fivetran_facebooks_ads__ad_performance_v_1_video_p_100_watched_actions') }}

),

_30_sec_report as (
    select *
    from
        {{ ref('stg_fivetran_facebooks_ads__ad_performance_v_1_video_30_sec_watched_actions') }}

),

avg_time_report as (
    select *
    from
        {{ ref('stg_fivetran_facebooks_ads__ad_performance_v_1_video_avg_time_watched_actions') }}

),

play_report as (
    select *
    from
        {{ ref('stg_fivetran_facebooks_ads__ad_performance_v_1_video_play_actions') }}

),

p25_metrics as (
    select
        source_relation,
        action_type,
        idx,
        fivetran_id,
        ad_id,
        date,
        sum(value) as action_video_p25_watched
    from p25_report
    group by all
),

p50_metrics as (
    select
        source_relation,
        action_type,
        idx,
        fivetran_id,
        ad_id,
        date,
        sum(value) as action_video_p50_watched
    from p50_report
    group by all
),

p75_metrics as (
    select
        source_relation,
        action_type,
        idx,
        fivetran_id,
        ad_id,
        date,
        sum(value) as action_video_p75_watched
    from p75_report
    group by all
),

p95_metrics as (
    select
        source_relation,
        action_type,
        idx,
        fivetran_id,
        ad_id,
        date,
        sum(value) as action_video_p95_watched
    from p95_report
    group by all
),

p100_metrics as (
    select
        source_relation,
        action_type,
        idx,
        fivetran_id,
        ad_id,
        date,
        sum(value) as action_video_p100_watched
    from p100_report
    group by all
),

_30_sec_metrics as (
    select
        source_relation,
        action_type,
        idx,
        fivetran_id,
        ad_id,
        date,
        sum(value) as action_video_30_sec_watched
    from _30_sec_report
    group by all
),

avg_time_metrics as (
    select
        source_relation,
        action_type,
        idx,
        fivetran_id,
        ad_id,
        date,
        sum(value) as action_video_avg_time
    from avg_time_report
    group by all
),

play_metrics as (
    select
        source_relation,
        action_type,
        idx,
        fivetran_id,
        ad_id,
        date,
        sum(value) as action_video_play
    from play_report
    group by all
),

metrics_join as (
    select
        p25_metrics.source_relation,
        p25_metrics.fivetran_id,
        p25_metrics.ad_id,
        p25_metrics.date,
        sum(action_video_p25_watched) as video_p25_watched_actions,
        sum(action_video_p50_watched) as video_p50_watched_actions,
        sum(action_video_p75_watched) as video_p75_watched_actions,
        sum(action_video_p95_watched) as video_p95_watched_actions,
        sum(action_video_p100_watched) as video_p100_watched_actions,
        sum(action_video_30_sec_watched) as video_30_sec_watched_actions,
        sum(action_video_avg_time) as video_avg_time_watched_actions,
        sum(action_video_play) as video_play_actions
    from p25_metrics
    left join p50_metrics
        on
            p25_metrics.source_relation = p50_metrics.source_relation
            and p25_metrics.action_type = p50_metrics.action_type
            and p25_metrics.idx = p50_metrics.idx
            and p25_metrics.ad_id = p50_metrics.ad_id
            and p25_metrics.date = p50_metrics.date


    left join p75_metrics
        on
            p25_metrics.source_relation = p75_metrics.source_relation
            and p25_metrics.action_type = p75_metrics.action_type
            and p25_metrics.idx = p75_metrics.idx
            and p25_metrics.ad_id = p75_metrics.ad_id
            and p25_metrics.date = p75_metrics.date


    left join p95_metrics
        on
            p25_metrics.source_relation = p95_metrics.source_relation
            and p25_metrics.action_type = p95_metrics.action_type
            and p25_metrics.idx = p95_metrics.idx
            and p25_metrics.ad_id = p95_metrics.ad_id
            and p25_metrics.date = p95_metrics.date


    left join p100_metrics
        on
            p25_metrics.source_relation = p100_metrics.source_relation
            and p25_metrics.action_type = p100_metrics.action_type
            and p25_metrics.idx = p100_metrics.idx
            and p25_metrics.ad_id = p100_metrics.ad_id
            and p25_metrics.date = p100_metrics.date

    left join _30_sec_metrics
        on
            p25_metrics.source_relation = _30_sec_metrics.source_relation
            and p25_metrics.action_type = _30_sec_metrics.action_type
            and p25_metrics.idx = _30_sec_metrics.idx
            and p25_metrics.ad_id = _30_sec_metrics.ad_id
            and p25_metrics.date = _30_sec_metrics.date

    left join play_metrics
        on
            p25_metrics.source_relation = play_metrics.source_relation
            and p25_metrics.action_type = play_metrics.action_type
            and p25_metrics.idx = play_metrics.idx
            and p25_metrics.ad_id = play_metrics.ad_id
            and p25_metrics.date = play_metrics.date

    left join avg_time_metrics
        on
            p25_metrics.source_relation = avg_time_metrics.source_relation
            and p25_metrics.action_type = avg_time_metrics.action_type
            and p25_metrics.idx = avg_time_metrics.idx
            and p25_metrics.ad_id = avg_time_metrics.ad_id
            and p25_metrics.date = avg_time_metrics.date







    group by all

)

select * from metrics_join
