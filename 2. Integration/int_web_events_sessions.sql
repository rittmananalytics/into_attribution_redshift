

  create view "heap"."analytics_dev_staging"."int_web_events_sessions__dbt_tmp" as (
    





with sessions as (

    select * from "heap"."analytics_dev_staging"."int_web_events_sessions_stitched"

    

),



windowed as (

    select

        *,

        row_number() over (
            partition by blended_user_id
            order by sessions.session_start_ts
            )
            
            as session_number

    from sessions

    


)

select * from windowed


  ) ;
