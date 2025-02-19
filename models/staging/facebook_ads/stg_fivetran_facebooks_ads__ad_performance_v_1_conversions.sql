
 with base as (
   select * 
   from {{ ref('stg_fivetran_facebook_ads__ad_performance_v_1_conversions_tmp') }}
), 
fields as (
    select
    _fivetran_synced,
    {{
        fivetran_utils.fill_staging_columns(
            source_columns=adapter.get_columns_in_relation(ref('stg_fivetran_facebook_ads__ad_performance_v_1_conversions_tmp')),
            staging_columns=get_ad_performance_v1_action_columns()
        )
        }}

        {{
            fivetran_utils.source_relation()
            }}

            from base
        ),
final as (
   select 
     _fivetran_synced,
      source_relation,
      lower(action_type) as action_type,
      index as idx,
      _fivetran_id as fivetran_id,
      ad_id,
      date,
      cast(coalesce(_1_d_view, 0) as {{dbt.type_float()}}) as _1_d_view,
      cast(coalesce(_7_d_click, 0) as {{dbt.type_float()}}) as _7_d_click,
      cast(coalesce(value, 0) as {{dbt.type_float()}}) as value
      from fields

)

select * from final
