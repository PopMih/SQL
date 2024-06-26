with CombinedFiles as (
select
		fabd.ad_date,
		fabd.url_parameters,
		fcam.campaign_name,
		coalesce(fabd.spend,0) as spend,
		coalesce(fabd.impressions,0) as impressions,
		coalesce(fabd.reach,0) as reach,
		coalesce(fabd.clicks,0) as clicks,
		coalesce(fabd.leads,0) as leads,
		coalesce(fabd.value,0) as value
from
		facebook_ads_basic_daily as fabd
left join
	facebook_campaign as fcam on
	fabd.campaign_id = fcam.campaign_id
union all
select
		gabd.ad_date,
		gabd.url_parameters,
		gabd.campaign_name,
		coalesce(gabd.spend,0) as spend,
		coalesce(gabd.impressions,0) as impressions,
		coalesce(gabd.reach,0) as reach,
		coalesce(gabd.clicks,0) as clicks,
		coalesce(gabd.leads,0),
		coalesce(gabd.value,0)
from
		google_ads_basic_daily as gabd
),
ExtractData as (
select
		strftime('%m', ad_date) as ad_month,
		case 
			when substr(url_parameters, instr(url_parameters, 'utm_campaign=') + length('utm_campaign=')) = 'nan' 
    		then null
		else substr(url_parameters, instr(url_parameters, 'utm_campaign=') + length('utm_campaign='))
	end as utm_campaign,
		sum(spend) as total_spend,
		sum(impressions) as total_impressions,
		sum(reach) as total_reach,
		sum(clicks) as total_clicks,
		sum(leads) as total_leads,
		sum(value) as total_value,
		case
		when sum(clicks) = 0 then 0
		else sum(spend)/ sum(clicks)
	end as CPC,
		case
		when sum(impressions) = 0 then 0
		else sum(spend)/(sum(impressions)/ 1000)
	end as CPM, 
		case
		when sum(impressions) = 0 then 0
		else cast(sum(clicks) as real)/ cast(sum(impressions) as real)* 100
	end as CTR,
		case
		when sum(spend) = 0 then 0
		else cast(sum(value)-sum(spend) as real)/ cast(sum(spend) as real)* 100
	end as ROMI
from
		CombinedFiles
group by
	ad_month,
	utm_campaign
)
	select
	    ad_month,
	    utm_campaign,
	    total_spend,
	    total_impressions,
	    total_reach,
	    total_clicks,
	    total_leads,
	    total_value,
	    CPC,
	    CTR,
	    CPM,
	    ROMI,
	    case
		when previous_CPC is null then 0
		else round(previous_CPC, 2)
	end as previous_CPC,
	    case
		when previous_CPM is null then 0
		else round(previous_CPM, 2)
	end as previous_CPM,
	    case
		when previous_CTR is null then 0
		else round(previous_CTR, 2)
	end as previous_CTR,
	    case
		when previous_ROMI is null then 0
		else round(previous_ROMI, 2)
	end as previous_ROMI,
	    round(coalesce(cast((CPC - previous_CPC) as real) / nullif(previous_CPC, 0) * 100, 0), 2) as CPC_monthly_diff,
	    round(coalesce(cast((CPM - previous_CPM) as real) / nullif(previous_CPM, 0) * 100, 0), 2) as CPM_monthly_diff,
	    round(coalesce(cast((CTR - previous_CTR) as real) / nullif(previous_CTR, 0) * 100, 0), 2) as CTR_monthly_diff,
	    round(coalesce(cast((ROMI - previous_ROMI) as real) / nullif(previous_ROMI, 0) * 100, 0), 2) as ROMI_monthly_diff
from
	(
	select
		ad_month,
		utm_campaign,
		total_spend,
		total_impressions,
		total_reach,
		total_clicks,
		total_leads,
		total_value,
		CPC,
		round(CTR, 2) as CTR,
		CPM,
		round(ROMI, 2) as ROMI,
		lag(CPC) over (partition by utm_campaign
	order by
		ad_month) as previous_CPC,
		lag(CPM) over (partition by utm_campaign
	order by
		ad_month) as previous_CPM,
		lag(CTR) over (partition by utm_campaign
	order by
		ad_month) as previous_CTR,
		lag(ROMI) over (partition by utm_campaign
	order by
		ad_month) as previous_ROMI
	from
		ExtractData
  )
group by
	ad_month,
	utm_campaign
