

  create  table
    "heap"."analytics_dev_staging"."stg_heap_events_page_events__dbt_tmp"
    
    
  as (
    



with
   recursive migrated_users(from_user_id, to_user_id, level) as
( select from_user_id, to_user_id, 1 as level
  from "heap"."intostudy_production"."user_migrations"
  union all
  select u.from_user_id, u.to_user_id, level + 1
  from "heap"."intostudy_production"."user_migrations" u, migrated_users m
  where u.to_user_id = m.from_user_id and level < 4
  ),
 mapped_user_ids as (
 select from_user_id, to_user_id from migrated_users order by to_user_id)
 ,
 source as (

   select * from "heap"."intostudy_production"."pageviews_vw_sitewide"


 ),
    users as (

   select * from "heap"."intostudy_production"."users"
    )
 ,
renamed as (

  SELECT
    event_id::varchar AS event_id,
    'Page View' AS event_type,
    time AS event_ts,
    title AS event_details,
    title AS page_title,
    path AS page_url_path,
    replace(
        
    
    cast(

    split_part(
        

    split_part(
        

    replace(
        

    replace(
        referrer,
        'http://',
        ''
    )
    

,
        'https://',
        ''
    )
    

,
        '/',
        1
        )

,
        '?',
        1
        )

 as varchar)
,
        'www.',
        ''
    )                           as referrer_host,
    query AS search,
    concat(DOMAIN, path) AS page_url,
    domain as page_url_host,
    cast(NULL AS varchar) AS gclid,
    utm_term AS utm_term,
    utm_content AS utm_content,
    utm_medium AS utm_medium,
    utm_campaign AS utm_campaign,
    utm_source AS utm_source,
    marketing_channel,
    ip AS ip,
    p.user_id::varchar AS visitor_id,
    u.email as email,
    u."identity" AS user_id,
    platform AS device,
    device_type as device_category,
    'intostudy.com' AS site,
    browser,
    browser_type,
    search_engine,
    social_network,
    country,
    region,
    city,
    continent
FROM
    source p
LEFT JOIN mapped_user_ids m on p.user_id = m.from_user_id
JOIN users u ON coalesce(m.to_user_id,p.user_id) = u.user_id

)
select * from renamed



  );