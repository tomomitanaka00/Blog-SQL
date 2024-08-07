-- Total Visits
SELECT COUNT(DISTINCT visitId) AS total_visits
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`;

-- Average page views per visit
SELECT AVG(totals.pageviews) AS avg_pageviews_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`;

-- Traffic Source Distribution
SELECT trafficSource.source, COUNT(*) AS visit_count
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`
GROUP BY trafficSource.source
ORDER BY visit_count DESC;

-- Bounce Rate
SELECT
  COUNTIF(totals.pageviews = 1) / COUNT(*) AS bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`;

-- Average Session Duration
SELECT AVG(totals.timeOnSite) AS avg_session_duration
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`;

-- User Engagement by Device Type
SELECT
  device.deviceCategory,
  AVG(totals.pageviews) AS avg_pageviews,
  AVG(totals.timeOnSite) AS avg_time_on_site
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`
GROUP BY device.deviceCategory;

-- User Engagement by Geographical Location
SELECT
  geoNetwork.country,
  AVG(totals.pageviews) AS avg_pageviews,
  AVG(totals.timeOnSite) AS avg_time_on_site
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`
GROUP BY geoNetwork.country
ORDER BY avg_pageviews DESC
LIMIT 10;