

  create  table
    "heap"."analytics_dev"."wh_web_events_fact__dbt_tmp"
    
    
  as (
    



with events as
  (
    SELECT *
    FROM   "heap"."analytics_dev_staging"."int_web_events_sessionized"
  )



,
events_with_prev_ts_event_type as
(
SELECT

    md5(cast(
    
    coalesce(cast(event_id as varchar), '')

 as varchar)) as web_event_pk,
    e.*,

    lag(e.event_ts,1) over (partition by e.blended_user_id order by event_seq) as prev_event_ts,
    lag(e.event_type,1)  over (partition by e.blended_user_id order by event_seq) as prev_event_type
FROM
   events e
)
,
joined as
(
  SELECT
      e.*
      
      
  FROM
     events_with_prev_ts_event_type e
  
  
)
select * from joined


  );