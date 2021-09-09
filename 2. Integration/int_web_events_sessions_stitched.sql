

  create view "heap"."analytics_dev_staging"."int_web_events_sessions_stitched__dbt_tmp" as (
    



with sessions as (

    select * from "heap"."analytics_dev_staging"."int_web_events_sessions_initial"

    

),

id_stitching as (

    select * from "heap"."analytics_dev_staging"."int_web_events_user_stitching"

),

joined as (

    select

        sessions.*,

        coalesce(id_stitching.user_id, sessions.visitor_id)
            as blended_user_id

    from sessions
    left join id_stitching using (visitor_id)

)

select *,
       

    datediff(
        MINUTE,
        lead(session_start_ts, 1) OVER (PARTITION BY blended_user_id ORDER BY session_start_ts DESC),
        session_start_ts
        )

 AS mins_between_sessions,
       case when events = 1 then true else false end as is_bounced_session



        from joined


  ) ;
