select
    {{ dbt_utils.star(from=ref('int_fivetran_facebook_ads__actions_types_metrics'), relation_alias='action_types_metrics', except=['source_relation']) }},
    {{ dbt_utils.star(from=ref('int_fivetran_facebook_ads_video_watch_actions_metrics'), relation_alias='video_watch_actions_metrics', except=['source_relation', 'ad_id', 'date','fivetran_id']) }} 
from
    {{ ref('int_fivetran_facebook_ads__actions_types_metrics') }}
        as action_types_metrics
left join
    {{ ref('int_fivetran_facebook_ads_video_watch_actions_metrics') }}
        as video_watch_actions_metrics
    on
        action_types_metrics.ad_id = video_watch_actions_metrics.ad_id
        and action_types_metrics.date = video_watch_actions_metrics.date
        and action_types_metrics.fivetran_id = video_watch_actions_metrics.fivetran_id
