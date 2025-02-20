
{{
    fivetran_utils.union_data(
        table_identifier='facebook_ad_performance_v_1_conversions',
        schema_variable='facebook_ads_schema',
        default_database=target.database,
        default_schema='facebook_ads',
    )
     
}}
