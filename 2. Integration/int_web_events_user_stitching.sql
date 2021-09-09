

  create view "heap"."analytics_dev_staging"."int_web_events_user_stitching__dbt_tmp" as (
    

with events as (

    select * from "heap"."analytics_dev_staging"."int_web_events"

),

mapping as (

    select distinct

        visitor_id as visitor_id,

        last_value(user_id ignore nulls) over (
            partition by visitor_id
            order by event_ts
            rows between unbounded preceding and unbounded following
        ) as user_id,

        min(event_ts) over (
            partition by visitor_id
        ) as first_seen_at,

        max(event_ts) over (
            partition by visitor_id
        ) as last_seen_at

    from events

)

select * from mapping


  ) ;
