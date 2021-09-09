

  create  table
    "heap"."analytics_dev"."wh_attribution_fact__dbt_tmp"
    
    
  as (
    



WITH
converting_events as
    (
      SELECT
        e.blended_user_id,
        first_value(CASE WHEN event_type = 'post_submission_pages_vw_post_submission_uk_us_apply_thank_you}' or event_type = 'application_form_submitted' THEN session_id END) over (PARTITION BY e.blended_user_id order by e.event_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as session_id,
        1 as count_conversions,
        event_type,
        MIN(CASE WHEN event_type = 'post_submission_pages_vw_post_submission_uk_us_apply_thank_you'
           or event_type = 'application_form_submitted' THEN event_ts END ) OVER (PARTITION BY e.blended_user_id) AS converted_ts,
        cast(null as timestamp) AS created_account_ts
      FROM
        "heap"."analytics_dev"."web_events_fact" e
      WHERE
        event_type = 'post_submission_pages_vw_post_submission_uk_us_apply_thank_you'
        OR event_type = 'application_form_submitted'),
converting_sessions as (
    SELECT
      *
    FROM
      converting_events
    group by 1,2,3,4,5,6
  ),
converting_sessions_deduped as (
    SELECT
      blended_user_id AS blended_user_id,
      MAX(CASE WHEN event_type = 'post_submission_pages_vw_post_submission_uk_us_apply_thank_you' or event_type = 'application_form_submitted' THEN session_id END ) AS session_id,
      max(count_conversions) as count_conversions,
      MIN(converted_ts) AS converted_ts,
      MIN(created_account_ts) AS created_account_ts
    FROM
      converting_sessions
    GROUP BY
     1
  ),
converting_sessions_deduped_labelled as
    (
      SELECT
        c.blended_user_id,
        s.session_start_ts,
        s.session_end_ts,
        c.converted_ts,
        c.created_account_ts,
        s.session_id AS session_id,
        ROW_NUMBER() OVER (PARTITION BY c.blended_user_id ORDER BY s.session_start_ts) AS session_seq,
        count_conversions,
        CASE WHEN c.created_account_ts BETWEEN s.session_start_ts AND s.session_end_ts THEN TRUE ELSE FALSE END AS account_opening_session,
        CASE WHEN (c.converted_ts BETWEEN s.session_start_ts AND s.session_end_ts)  THEN TRUE ELSE FALSE END AS conversion_session,
        CASE WHEN (c.converted_ts BETWEEN s.session_start_ts AND s.session_end_ts)  THEN 1 ELSE 0 END AS event,
        CASE WHEN s.session_start_ts BETWEEN c.created_account_ts AND coalesce(c.converted_ts, s.session_end_ts) THEN TRUE ELSE FALSE END AS trialing_session,
        utm_source,
        utm_content,
        utm_medium,
        utm_campaign,
        email,
        marketing_channel,
        referrer_host,
        browser,
        browser_type,
        search_engine,
        social_network,
        country,
        region,
        city,
        continent,
        first_page_url_host,
        first_page_url_path,
        REPLACE(REGEXP_SUBSTR(referrer_host,'//[^/\\\,=@\\+]+\\.[^/:;,\\\\\(\\)]+'),'//','') as referrer_domain,
        channel,
        events
      FROM
        "heap"."analytics_dev"."web_sessions_fact" s
      JOIN
        converting_sessions_deduped c
      ON
        c.blended_user_id = s.blended_user_id
      WHERE
        c.converted_ts >= s.session_start_ts
      ORDER BY
        c.blended_user_id,
        s.session_start_ts),
        session_attrib_pct as (
            SELECT
              *,
              CASE
                WHEN session_id = LAST_VALUE(session_id) OVER (PARTITION BY blended_user_id ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) THEN 1
              ELSE
              0
            END
              AS LAST_click_attrib_pct,
              CASE
                WHEN session_id = FIRST_VALUE(session_id) OVER (PARTITION BY blended_user_id ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) THEN 1
              ELSE
              0
            END
              AS first_click_attrib_pct,
              1/COUNT(session_id) OVER (PARTITION BY blended_user_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS even_click_attrib_pct,
              CASE
                WHEN session_start_ts = FIRST_VALUE(session_start_ts) OVER (PARTITION BY blended_user_id ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AND MAX(event) OVER (PARTITION BY blended_user_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) = 1 THEN 1.1-ROW_NUMBER() OVER (PARTITION BY blended_user_id)
                WHEN session_start_ts > LAG(session_start_ts) OVER (PARTITION BY blended_user_id ORDER BY session_start_ts)
              AND MAX(event) OVER (PARTITION BY blended_user_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) = 1 THEN ROUND(1.1-1/ROW_NUMBER() OVER (PARTITION BY blended_user_id), 2)
              ELSE
              null
            END
              AS weights
            FROM  converting_sessions_deduped_labelled),
        session_attrib_pct_with_time_decay AS (
            SELECT
              *,
              ROUND(case when (weights::FLOAT=0 OR SUM(weights::FLOAT) OVER (PARTITION BY blended_user_id)=0) then 0
                    else weights::float/SUM(weights::FLOAT) OVER (PARTITION BY blended_user_id) end, 2) AS time_decay_attrib_pct
            FROM
              session_attrib_pct),
        final as (
            SELECT
              *,
              round(MAX(count_conversions * first_click_attrib_pct),2) AS first_click_attrib_conversions,
              round(MAX(count_conversions * last_click_attrib_pct),2) AS last_click_attrib_conversions,
              round(MAX(count_conversions * even_click_attrib_pct),2) AS even_click_attrib_conversions,
              round(MAX(count_conversions * time_decay_attrib_pct),2) AS time_decay_attrib_conversions
            FROM
              session_attrib_pct_with_time_decay
              group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37 )
          select
            *
          from
            final
  );