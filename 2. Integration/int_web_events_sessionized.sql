

  create view "heap"."analytics_dev_staging"."int_web_events_sessionized__dbt_tmp" as (
    

with events as (select * from "heap"."analytics_dev_staging"."int_web_events"
/*  
*/
),

numbered as (

    select

        *,

        row_number() over (
            partition by visitor_id
            order by event_ts
          ) as event_number

    from events

),

lagged as (

    select

        *,

        lag(event_ts) over (
            partition by visitor_id
            order by event_number
          ) as previous_event_ts

    from numbered

),

diffed as (

    select
        *,
        

    datediff(
        second,
        event_ts,
        previous_event_ts
        )

 as period_of_inactivity

    from lagged

),

new_sessions as (


    select
        *,
        case
            when period_of_inactivity <= 30 * 60 then 0
            else 1
        end as new_session
    from diffed

),

session_numbers as (


    select

        *,

        sum(new_session) over (
            partition by visitor_id
            order by event_number
            rows between unbounded preceding and current row
            ) as session_number

    from new_sessions

),

session_ids AS (

  SELECT
    event_id,
    event_type,
    event_ts,
    event_details,
    page_title,
    page_url_path,
    referrer_host,
    search,
    page_url,
    page_url_host,
    gclid,
    utm_term,
    utm_content,
    utm_medium,
    utm_campaign,
    utm_source,
    ip,
    visitor_id,
    user_id,
    email,
    device,
    device_category,
    event_number,
    md5(CONCAT(CONCAT(visitor_id::varchar,'-'),coalesce(session_number::varchar,''::varchar))) as session_id,
    site,
    marketing_channel,
    browser,
    browser_type,
    search_engine,
    social_network,
    country,
    region,
    city,
    continent
  FROM
    session_numbers ),
id_stitching as (

    select * from "heap"."analytics_dev_staging"."int_web_events_user_stitching"

),

joined as (

    select

        session_ids.*,

        coalesce(id_stitching.user_id, session_ids.visitor_id)
            as blended_user_id

    from session_ids
    left join id_stitching on id_stitching.visitor_id = session_ids.visitor_id

),
ordered as (
  select *,
         row_number() over (partition by blended_user_id order by event_ts) as event_seq,
         row_number() over (partition by blended_user_id, session_id order by event_ts) as event_in_session_seq
         ,

         case when event_type = 'Page View'
         and session_id = lead(session_id,1) over (partition by visitor_id order by event_number)
         then 

    datediff(
        SECOND,
        lead(event_ts,1) over (partition by visitor_id order by event_number),
        event_ts
        )

 end time_on_page_secs
  from joined

)
,
ordered_conversion_tagged as (
  SELECT o.*
  FROM ordered o)
select *
from ordered_conversion_tagged



  ) ;
