

  create view "heap"."analytics_dev_staging"."int_web_events__dbt_tmp" as (
    

with events_merge_list as
  (
    

      

      select
        'heap_events_page' as source,
        *
        from "heap"."analytics_dev_staging"."stg_heap_events_page_events"

        union all
      

      

      select
        'heap_events_track' as source,
        *
        from "heap"."analytics_dev_staging"."stg_heap_events_track_events"

        
      
  )


select
  e.*


from events_merge_list e


left outer join
  "heap"."analytics_dev_seed"."event_mapping_list" m
on
  e.event_type = m.event_type_original



  ) ;
