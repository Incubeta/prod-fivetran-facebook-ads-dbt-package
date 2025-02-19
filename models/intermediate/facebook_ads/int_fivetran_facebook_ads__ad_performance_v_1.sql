with performance_report as (
	select 
		_fivetran_synced,
		day,
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
		account_currency,
		sum(canvas_avg_view_percent) as canvas_avg_view_percent,
		sum(canvas_avg_view_time) as canvas_avg_view_time,
		sum(frequency) as frequency,
		sum(clicks) as clicks,
		sum(impressions) as impressions,
		sum(inline_post_engagement) as inline_post_engagement,
		sum(inline_link_clicks) as inline_link_clicks,
		sum(reach) as reach,
		sum(cost) as cost,
		from {{ref('stg_fivetran_facebook_ads__ad_performance_v_1')}}
		group by all


)
select * from performance_report
