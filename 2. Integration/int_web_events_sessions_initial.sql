

  create  table
    "heap"."analytics_dev_staging"."int_web_events_sessions_initial__dbt_tmp"
    
    
  as (
    











with events_sessionized as (

    select * from "heap"."analytics_dev_staging"."int_web_events_sessionized"

    

),

referrer_mapping as (

    select * from "heap"."analytics_dev_seed"."referrer_mapping"

),

additional_referrer_mapping as (

    select * from "heap"."analytics_dev_seed"."additional_referrer_mapping"

),
marketing_channel_mapping as (

    select * from "heap"."analytics_dev_seed"."marketing_channel_mapping"

),

channel_mapping as (

    select * from "heap"."analytics_dev_seed"."marketing_channel_mapping"

),
agg as (

    select distinct
        session_id,
        visitor_id,
        user_id,
        site,
        min(event_ts) over ( partition by session_id ) as session_start_ts,
        max(event_ts) over ( partition by session_id ) as session_end_ts,
        count(*) over ( partition by session_id ) as events,

        
        first_value(utm_source) over (
    partition by session_id
    order by event_number
    rows between unbounded preceding and unbounded following
    ) as utm_source,
        
        first_value(utm_content) over (
    partition by session_id
    order by event_number
    rows between unbounded preceding and unbounded following
    ) as utm_content,
        
        first_value(utm_medium) over (
    partition by session_id
    order by event_number
    rows between unbounded preceding and unbounded following
    ) as utm_medium,
        
        first_value(utm_campaign) over (
    partition by session_id
    order by event_number
    rows between unbounded preceding and unbounded following
    ) as utm_campaign,
        
        first_value(utm_term) over (
    partition by session_id
    order by event_number
    rows between unbounded preceding and unbounded following
    ) as utm_term,
        
        first_value(search) over (
    partition by session_id
    order by event_number
    rows between unbounded preceding and unbounded following
    ) as search,
        
        first_value(gclid) over (
    partition by session_id
    order by event_number
    rows between unbounded preceding and unbounded following
    ) as gclid,
        
        first_value(email) over (
    partition by session_id
    order by event_number
    rows between unbounded preceding and unbounded following
    ) as email,
        
        first_value(page_url) over (
    partition by session_id
    order by event_number
    rows between unbounded preceding and unbounded following
    ) as first_page_url,
        
        first_value(page_url_host) over (
    partition by session_id
    order by event_number
    rows between unbounded preceding and unbounded following
    ) as first_page_url_host,
        
        first_value(page_url_path) over (
    partition by session_id
    order by event_number
    rows between unbounded preceding and unbounded following
    ) as first_page_url_path,
        
        first_value(referrer_host) over (
    partition by session_id
    order by event_number
    rows between unbounded preceding and unbounded following
    ) as referrer_host,
        
        first_value(device) over (
    partition by session_id
    order by event_number
    rows between unbounded preceding and unbounded following
    ) as device,
        
        first_value(device_category) over (
    partition by session_id
    order by event_number
    rows between unbounded preceding and unbounded following
    ) as device_category,
        
        first_value(browser) over (
    partition by session_id
    order by event_number
    rows between unbounded preceding and unbounded following
    ) as browser,
        
        first_value(browser_type) over (
    partition by session_id
    order by event_number
    rows between unbounded preceding and unbounded following
    ) as browser_type,
        
        first_value(search_engine) over (
    partition by session_id
    order by event_number
    rows between unbounded preceding and unbounded following
    ) as search_engine,
        
        first_value(social_network) over (
    partition by session_id
    order by event_number
    rows between unbounded preceding and unbounded following
    ) as social_network,
        
        first_value(marketing_channel) over (
    partition by session_id
    order by event_number
    rows between unbounded preceding and unbounded following
    ) as marketing_channel,
        
        first_value(country) over (
    partition by session_id
    order by event_number
    rows between unbounded preceding and unbounded following
    ) as country,
        
        first_value(region) over (
    partition by session_id
    order by event_number
    rows between unbounded preceding and unbounded following
    ) as region,
        
        first_value(city) over (
    partition by session_id
    order by event_number
    rows between unbounded preceding and unbounded following
    ) as city,
        
        first_value(continent) over (
    partition by session_id
    order by event_number
    rows between unbounded preceding and unbounded following
    ) as continent,
        

        
        last_value(page_url) over (
    partition by session_id
    order by event_number
    rows between unbounded preceding and unbounded following
    ) as last_page_url,
        
        last_value(page_url_host) over (
    partition by session_id
    order by event_number
    rows between unbounded preceding and unbounded following
    ) as last_page_url_host,
        
        last_value(page_url_path) over (
    partition by session_id
    order by event_number
    rows between unbounded preceding and unbounded following
    ) as last_page_url_path
        

    from events_sessionized

),

diffs as (

    select

        *,

        

    datediff(
        SECOND,
        session_start_ts,
        session_end_ts
        )



 as duration_in_s

    from agg

),

tiers as (

    select

        *,

        case
            when duration_in_s between 0 and 9 then '0s to 9s'
            when duration_in_s between 10 and 29 then '10s to 29s'
            when duration_in_s between 30 and 59 then '30s to 59s'
            when duration_in_s > 59 then '60s or more'
            else null
        end as duration_in_s_tier

    from diffs

),

mapped as (

    select
        tiers.*,
        referrer_mapping.medium as referrer_medium,
        referrer_mapping.source as referrer_source

    from tiers

    left join referrer_mapping on tiers.referrer_host = referrer_mapping.host

),

channel_mapped as (

    select
      mapped.*,
      case when coalesce(marketing_channel_mapping.channel,additional_referrer_mapping.channel) is not null then coalesce(marketing_channel_mapping.channel,additional_referrer_mapping.channel)
           when coalesce(marketing_channel_mapping.channel,additional_referrer_mapping.channel) is null and mapped.referrer_host is not null then 'Referral'
           else 'Direct' end as channel
      from mapped
      left join additional_referrer_mapping
      on mapped.referrer_host = additional_referrer_mapping.domain
      left join marketing_channel_mapping
      on mapped.utm_medium = marketing_channel_mapping.medium

)

select * from channel_mapped


  );