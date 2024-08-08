-- Engagement Depth Analysis
SELECT
  CASE 
    WHEN totals.pageviews = 1 THEN '1 page'
    WHEN totals.pageviews BETWEEN 2 AND 5 THEN '2-5 pages'
    WHEN totals.pageviews BETWEEN 6 AND 10 THEN '6-10 pages'
    ELSE '10+ pages'
  END AS pageview_bucket,
  COUNT(*) AS sessions,
  AVG(totals.timeOnSite) AS avg_session_duration,
  ROUND(100 * COUNTIF(totals.transactions > 0) / COUNT(*), 2) AS conversion_rate
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
  _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
GROUP BY
  pageview_bucket
ORDER BY
  sessions DESC;

-- Content Engagement Analysis
SELECT
  hits.page.pagePath,
  COUNT(*) AS pageviews,
  AVG(hits.time) AS avg_time_on_page,
  SUM(CAST(hits.isExit AS INT64)) AS exits,
  ROUND(100 * SUM(CAST(hits.isExit AS INT64)) / COUNT(*), 2) AS exit_rate
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
  UNNEST(hits) AS hits
WHERE
  _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
  AND hits.type = 'PAGE'
GROUP BY
  hits.page.pagePath
ORDER BY
  pageviews DESC
LIMIT 20;

-- User Flow Analysis
WITH page_sequence AS (
  SELECT
    fullVisitorId,
    visitId,
    hits.page.pagePath,
    hits.hitNumber,
    LEAD(hits.page.pagePath) OVER (PARTITION BY fullVisitorId, visitId ORDER BY hits.hitNumber) AS next_page
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
    UNNEST(hits) AS hits
  WHERE
    _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
    AND hits.type = 'PAGE'
)
SELECT
  pagePath AS current_page,
  next_page,
  COUNT(*) AS frequency
FROM
  page_sequence
WHERE
  next_page IS NOT NULL
GROUP BY
  current_page, next_page
ORDER BY
  frequency DESC
LIMIT 20;


-- Engagement by User Loyalty
SELECT
  CASE
    WHEN totals.visits = 1 THEN 'First Visit'
    WHEN totals.visits BETWEEN 2 AND 5 THEN '2-5 Visits'
    WHEN totals.visits BETWEEN 6 AND 10 THEN '6-10 Visits'
    ELSE '10+ Visits'
  END AS visit_frequency,
  COUNT(*) AS sessions,
  AVG(totals.timeOnSite) AS avg_session_duration,
  AVG(totals.pageviews) AS avg_pageviews,
  ROUND(100 * SUM(totals.transactions) / COUNT(*), 2) AS conversion_rate
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
  _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
GROUP BY
  visit_frequency
ORDER BY
  sessions DESC;

  -- Event-Based Engagement Analysis
  SELECT
  hits.eventInfo.eventCategory,
  hits.eventInfo.eventAction,
  COUNT(*) AS event_count
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
  UNNEST(hits) AS hits
WHERE
  _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
  AND hits.type = 'EVENT'
GROUP BY
  hits.eventInfo.eventCategory,
  hits.eventInfo.eventAction
ORDER BY
  event_count DESC
LIMIT 20;

-- Time to Purchase Analysis
WITH purchase_sessions AS (
 SELECT
 fullVisitorId,
 visitId,
 MAX(hits.time) / 1000 AS time_to_purchase
 FROM
 `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
 UNNEST(hits) AS hits
 WHERE
 _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
 AND totals.transactions > 0
 GROUP BY
 fullVisitorId, visitId
)
SELECT
 CASE
 WHEN time_to_purchase < 60 THEN '< 1 min'
 WHEN time_to_purchase < 300 THEN '1-5 mins'
 WHEN time_to_purchase < 900 THEN '5-15 mins'
 ELSE '15+ mins'
 END AS time_bucket,
 COUNT(*) AS purchase_count,
 AVG(time_to_purchase) AS avg_time_to_purchase
FROM
 purchase_sessions
GROUP BY
 time_bucket
ORDER BY
 purchase_count DESC;

 -- Cross-Device Engagement
 WITH user_devices AS (
  SELECT
    fullVisitorId,
    COUNT(DISTINCT device.deviceCategory) AS device_count
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE
    _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
  GROUP BY
    fullVisitorId
)
SELECT
  CASE
    WHEN device_count = 1 THEN 'Single Device'
    WHEN device_count = 2 THEN 'Two Devices'
    ELSE 'Three or More Devices'
  END AS device_usage,
  COUNT(*) AS user_count,
  AVG(device_count) AS avg_devices_used
FROM
  user_devices
GROUP BY
  device_usage
ORDER BY
  user_count DESC;
  