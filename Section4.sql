 -- Data Preparation
CREATE OR REPLACE TABLE `predictive-behavior-analytics.Section4.revenue_prediction_data` AS
SELECT
  CONCAT(fullVisitorId, CAST(visitId AS STRING)) AS session_id,
  trafficSource.source AS traffic_source,
  trafficSource.medium AS traffic_medium,
  device.deviceCategory AS device_type,
  geoNetwork.country AS country,
  totals.pageviews AS pageviews,
  totals.timeOnSite AS time_on_site,
  IFNULL(totals.transactionRevenue, 0) / 1000000 AS revenue
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
  _TABLE_SUFFIX BETWEEN '20160801' AND '20170731';

  -- Traffic Source Analysis
SELECT
  traffic_source,
  COUNT(*) AS total_visits,
  SUM(revenue) AS total_revenue,
  AVG(revenue) AS avg_revenue_per_visit
FROM
  `predictive-behavior-analytics.Section4.revenue_prediction_data`
GROUP BY
  traffic_source
ORDER BY
  total_revenue DESC;

-- User Demographics Analysis
SELECT
  device_type,
  COUNT(*) AS total_visits,
  SUM(revenue) AS total_revenue,
  AVG(revenue) AS avg_revenue_per_visit
FROM
  `predictive-behavior-analytics.Section4.revenue_prediction_data`
GROUP BY
  device_type
ORDER BY
  total_revenue DESC;

-- Behavioral Patterns Analysis
SELECT
  pageviews,
  AVG(revenue) AS avg_revenue_per_pageview
FROM
  `predictive-behavior-analytics.Section4.revenue_prediction_data`
GROUP BY
  pageviews
ORDER BY
  pageviews DESC;

  -- Feature Engineering: Categorical Variables
CREATE OR REPLACE TABLE `predictive-behavior-analytics.Section4.revenue_prediction_data_with_features` AS
SELECT
  *,
  CAST(traffic_source AS STRING) AS traffic_source_cat,
  CAST(device_type AS STRING) AS device_type_cat,
  CAST(country AS STRING) AS country_cat,
  (pageviews - (SELECT AVG(pageviews) FROM `predictive-behavior-analytics.Section4.revenue_prediction_data`)) / (SELECT STDDEV(pageviews) FROM `predictive-behavior-analytics.Section4.revenue_prediction_data`) AS normalized_pageviews,
  (time_on_site - (SELECT AVG(time_on_site) FROM `predictive-behavior-analytics.Section4.revenue_prediction_data`)) / (SELECT STDDEV(time_on_site) FROM `predictive-behavior-analytics.Section4.revenue_prediction_data`) AS normalized_time_on_site
FROM
  `predictive-behavior-analytics.Section4.revenue_prediction_data`;

  -- Build and Train Linear Regression Model
CREATE OR REPLACE MODEL `predictive-behavior-analytics.Section4.revenue_prediction_model`
OPTIONS(model_type='linear_reg', input_label_cols=['revenue']) AS
SELECT
  traffic_source_cat,
  device_type_cat,
  country_cat,
  normalized_pageviews,
  normalized_time_on_site,
  revenue,  
FROM
  `predictive-behavior-analytics.Section4.revenue_prediction_data_with_features`;

  -- Build and Train Ridge Regression Model
CREATE OR REPLACE MODEL `predictive-behavior-analytics.Section4.ridge_reg_sales_model`
OPTIONS(model_type='linear_reg', l2_reg=0.1, input_label_cols=['revenue']) AS
SELECT
  traffic_source_cat,
  device_type_cat,
  country_cat,
  normalized_pageviews,
  normalized_time_on_site,
  revenue,
FROM
  `predictive-behavior-analytics.Section4.revenue_prediction_data_with_features`;

  -- Build and Train Lasso Regression Model
CREATE OR REPLACE MODEL `predictive-behavior-analytics.Section4.lasso_reg_sales_model`
OPTIONS(model_type='linear_reg', l1_reg=0.1, input_label_cols=['revenue']) AS
SELECT
  traffic_source_cat,
  device_type_cat,
  country_cat,
  normalized_pageviews,
  normalized_time_on_site,
  revenue,
FROM
  `predictive-behavior-analytics.Section4.revenue_prediction_data_with_features`;

-- Evaluate the Models and calculate RMSE
CREATE OR REPLACE TABLE `predictive-behavior-analytics.Section4.revenue_prediction_evaluation` AS
SELECT
  'Linear Regression' AS model,
  mean_absolute_error,
  mean_squared_error,
  SQRT(mean_squared_error) AS root_mean_squared_error,  -- Calculate RMSE
  mean_squared_log_error,
  median_absolute_error,
  r2_score,
  explained_variance,
  mean_absolute_error AS mae
FROM
  ML.EVALUATE(
    MODEL `predictive-behavior-analytics.Section4.revenue_prediction_model`,
    (
      SELECT
        traffic_source_cat,
        device_type_cat,
        country_cat,
        normalized_pageviews,
        normalized_time_on_site,
        revenue
      FROM
        `predictive-behavior-analytics.Section4.revenue_prediction_data_with_features`
    )
  )
UNION ALL
SELECT
  'Ridge Regression' AS model,
  mean_absolute_error,
  mean_squared_error,
  SQRT(mean_squared_error) AS root_mean_squared_error,  -- Calculate RMSE
  mean_squared_log_error,
  median_absolute_error,
  r2_score,
  explained_variance,
  mean_absolute_error AS mae
FROM
  ML.EVALUATE(
    MODEL `predictive-behavior-analytics.Section4.ridge_reg_sales_model`,
    (
      SELECT
        traffic_source_cat,
        device_type_cat,
        country_cat,
        normalized_pageviews,
        normalized_time_on_site,
        revenue
      FROM
        `predictive-behavior-analytics.Section4.revenue_prediction_data_with_features`
    )
  )
UNION ALL
SELECT
  'Lasso Regression' AS model,
  mean_absolute_error,
  mean_squared_error,
  SQRT(mean_squared_error) AS root_mean_squared_error,  -- Calculate RMSE
  mean_squared_log_error,
  median_absolute_error,
  r2_score,
  explained_variance,
  mean_absolute_error AS mae
FROM
  ML.EVALUATE(
    MODEL `predictive-behavior-analytics.Section4.lasso_reg_sales_model`,
    (
      SELECT
        traffic_source_cat,
        device_type_cat,
        country_cat,
        normalized_pageviews,
        normalized_time_on_site,
        revenue
      FROM
        `predictive-behavior-analytics.Section4.revenue_prediction_data_with_features`
    )
  );
