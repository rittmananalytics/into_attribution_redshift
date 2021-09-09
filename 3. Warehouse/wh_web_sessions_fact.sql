

  create  table
    "heap"."analytics_dev"."wh_web_sessions_fact__dbt_tmp"
    
    
  as (
    



with sessions as
  (
    SELECT
      *
    FROM (
      SELECT
        session_id,
        session_start_ts,
        session_end_ts,
        events,
        utm_source,
        utm_content,
        utm_medium,
        utm_campaign,
        utm_term,
        search_engine,
	      social_network,
	      marketing_channel,
	      country,
	      region,
	      city,
	      continent,
        search,
        gclid,
        first_page_url,
        first_page_url_host,
        first_page_url_path,
        referrer_host,
        device,
        device_category,
        browser,
        browser_type,
        last_page_url,
        last_page_url_host,
        last_page_url_path,
        duration_in_s,
        duration_in_s_tier,
        referrer_medium,
        referrer_source,
        channel,
        blended_user_id,
        email,
        is_bounced_session,
        sum(mins_between_sessions) over (partition by session_id) as mins_between_sessions

      FROM
        "heap"."analytics_dev_staging"."int_web_events_sessions_stitched"
      )
      group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38
    )
    
    
      ,
      joined as (
        SELECT
            s.*
            
            
        FROM
           sessions s
    
        
      ),
      ordered as (
        
        SELECT
          md5(cast(
    
    coalesce(cast(session_id as varchar), '')

 as varchar)) as web_sessions_pk,
      
          * ,
          row_number() over (partition by blended_user_id order by session_start_ts) as user_session_number
        FROM
          joined)
  SELECT
    *
  FROM
    ordered


  );