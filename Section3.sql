-- Data Preparation
CREATE OR REPLACE TABLE `predictive-behavior-analytics.Section3.sales_prediction_data` AS
SELECT
  CONCAT(fullVisitorId, CAST(visitId AS STRING)) AS session_id,
  trafficSource.source AS traffic_source,
  trafficSource.medium AS traffic_medium,
  device.deviceCategory AS device_type,
  geoNetwork.country AS country,
  totals.pageviews AS pageviews,
  totals.timeOnSite AS time_on_site,
  totals.transactions AS transactions,
  IFNULL(totals.transactionRevenue, 0) / 1000000 AS revenue
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
  _TABLE_SUFFIX BETWEEN '20160801' AND '20170731';

-- Traffic Sources and Sales Impact
SELECT
  traffic_source,
  COUNT(*) AS total_visits,
  SUM(transactions) AS total_transactions,
  SUM(transactions) / COUNT(*) AS conversion_rate,
  SUM(revenue) AS total_revenue
FROM
  `predictive-behavior-analytics.Section3.sales_prediction_data`
GROUP BY
  traffic_source
ORDER BY
  total_visits DESC;

-- Device Type Analysis
SELECT
  device_type,
  COUNT(*) AS total_visits,
  SUM(transactions) AS total_transactions,
  SUM(transactions) / COUNT(*) AS conversion_rate,
  SUM(revenue) AS total_revenue
FROM
  `predictive-behavior-analytics.Section3.sales_prediction_data`
GROUP BY
  device_type
ORDER BY
  total_visits DESC;

-- Geographical Analysis
SELECT
  country,
  COUNT(*) AS total_visits,
  SUM(transactions) AS total_transactions,
  SUM(transactions) / COUNT(*) AS conversion_rate,
  SUM(revenue) AS total_revenue
FROM
  `predictive-behavior-analytics.Section3.sales_prediction_data`
GROUP BY
  country
ORDER BY
  total_revenue DESC
LIMIT 10;

-- Feature Engineering: Categorical Variables
CREATE OR REPLACE TABLE `predictive-behavior-analytics.Section3.sales_prediction_data_with_categorical` AS
SELECT
  *,
  CAST(traffic_source AS STRING) AS traffic_source_cat,
  CAST(device_type AS STRING) AS device_type_cat,
  CAST(country AS STRING) AS country_cat
FROM
   `predictive-behavior-analytics.Section3.sales_prediction_data`;

-- Feature Engineering: Normalized Variables
CREATE OR REPLACE TABLE `predictive-behavior-analytics.Section3.sales_prediction_data_normalized` AS
SELECT
  *,
  (pageviews - (SELECT AVG(pageviews) FROM `predictive-behavior-analytics.Section3.sales_prediction_data`)) / (SELECT STDDEV(pageviews) FROM `predictive-behavior-analytics.Section3.sales_prediction_data`) AS normalized_pageviews,
  (time_on_site - (SELECT AVG(time_on_site) FROM `predictive-behavior-analytics.Section3.sales_prediction_data`)) / (SELECT STDDEV(time_on_site) FROM `predictive-behavior-analytics.Section3.sales_prediction_data`) AS normalized_time_on_site
FROM
  `predictive-behavior-analytics.Section3.sales_prediction_data`;

-- Combine all features into the final dataset
CREATE OR REPLACE TABLE `predictive-behavior-analytics.Section3.sales_prediction_data_final` AS
SELECT
  a.*,
  b.traffic_source_cat,
  b.device_type_cat,
  b.country_cat,
  c.normalized_pageviews,
  c.normalized_time_on_site
FROM
  `predictive-behavior-analytics.Section3.sales_prediction_data` a
JOIN
  `predictive-behavior-analytics.Section3.sales_prediction_data_with_categorical` b
ON
  a.session_id = b.session_id
JOIN
  `predictive-behavior-analytics.Section3.sales_prediction_data_normalized` c
ON
  a.session_id = c.session_id;

-- Logistic Regression Model
CREATE OR REPLACE MODEL `predictive-behavior-analytics.Section3.log_reg_sales_model`
OPTIONS(model_type='logistic_reg', input_label_cols=['made_purchase']) AS
SELECT
  traffic_source_cat,
  device_type_cat,
  country_cat,
  normalized_pageviews,
  normalized_time_on_site,
  IF(transactions > 0, 1, 0) AS made_purchase
FROM
  `predictive-behavior-analytics.Section3.sales_prediction_data_final`;

-- Boosted Tree Classifier Model (Random Forest)
CREATE OR REPLACE MODEL `predictive-behavior-analytics.Section3.boosted_tree_sales_model`
OPTIONS(model_type='boosted_tree_classifier', booster_type='gbtree',
  num_parallel_tree=100, input_label_cols=['made_purchase']) AS
SELECT
  traffic_source_cat,
  device_type_cat,
  country_cat,
  normalized_pageviews,
  normalized_time_on_site,
  IF(transactions > 0, 1, 0) AS made_purchase
FROM
  `predictive-behavior-analytics.Section3.sales_prediction_data_final`;

-- XGBoost Model
CREATE OR REPLACE MODEL `predictive-behavior-analytics.Section3.xgboost_sales_model`
OPTIONS(model_type='boosted_tree_classifier', input_label_cols=['made_purchase']) AS
SELECT
  traffic_source_cat,
  device_type_cat,
  country_cat,
  normalized_pageviews,
  normalized_time_on_site,
  IF(transactions > 0, 1, 0) AS made_purchase
FROM
  `predictive-behavior-analytics.Section3.sales_prediction_data_final`;

-- Deep Neural Network Model
CREATE OR REPLACE MODEL `predictive-behavior-analytics.Section3.dnn_sales_model`
OPTIONS(model_type='dnn_classifier', hidden_units=[128, 64, 32], input_label_cols=['made_purchase']) AS
SELECT
  traffic_source_cat,
  device_type_cat,
  country_cat,
  normalized_pageviews,
  normalized_time_on_site,
  IF(transactions > 0, 1, 0) AS made_purchase
FROM
  `predictive-behavior-analytics.Section3.sales_prediction_data_final`;


-- Evaluate the Logistic Regression Model and save the results
CREATE OR REPLACE TABLE `predictive-behavior-analytics.Section3.log_reg_sales_model_evaluation` AS
SELECT
  *
FROM
  ML.EVALUATE(
    MODEL `predictive-behavior-analytics.Section3.log_reg_sales_model`,
    (
      SELECT
        traffic_source_cat,
        device_type_cat,
        country_cat,
        normalized_pageviews,
        normalized_time_on_site,
        IF(transactions > 0, 1, 0) AS made_purchase
      FROM
        `predictive-behavior-analytics.Section3.sales_prediction_data_final`
    )
  );

-- Evaluate the Boosted Tree Classifier Model and save the results
CREATE OR REPLACE TABLE `predictive-behavior-analytics.Section3.boosted_tree_sales_model_evaluation` AS
SELECT
  *
FROM
  ML.EVALUATE(
    MODEL `predictive-behavior-analytics.Section3.boosted_tree_sales_model`,
    (
      SELECT
        traffic_source_cat,
        device_type_cat,
        country_cat,
        normalized_pageviews,
        normalized_time_on_site,
        IF(transactions > 0, 1, 0) AS made_purchase
      FROM
        `predictive-behavior-analytics.Section3.sales_prediction_data_final`
    )
  );

-- Evaluate the XGBoost Model and save the results
CREATE OR REPLACE TABLE `predictive-behavior-analytics.Section3.xgboost_sales_model_evaluation` AS
SELECT
  *
FROM
  ML.EVALUATE(
    MODEL `predictive-behavior-analytics.Section3.xgboost_sales_model`,
    (
      SELECT
        traffic_source_cat,
        device_type_cat,
        country_cat,
        normalized_pageviews,
        normalized_time_on_site,
        IF(transactions > 0, 1, 0) AS made_purchase
      FROM
        `predictive-behavior-analytics.Section3.sales_prediction_data_final`
    )
  );

-- Evaluate the Deep Neural Network Model and save the results
CREATE OR REPLACE TABLE `predictive-behavior-analytics.Section3.dnn_sales_model_evaluation` AS
SELECT
  *
FROM
  ML.EVALUATE(
    MODEL `predictive-behavior-analytics.Section3.dnn_sales_model`,
    (
      SELECT
        traffic_source_cat,
        device_type_cat,
        country_cat,
        normalized_pageviews,
        normalized_time_on_site,
        IF(transactions > 0, 1, 0) AS made_purchase
      FROM
        `predictive-behavior-analytics.Section3.sales_prediction_data_final`
    )
  );


-- Combine Model Evaluation Results for Comparison
CREATE OR REPLACE TABLE `predictive-behavior-analytics.Section3.model_comparison` AS
SELECT
  'Logistic Regression' AS model,
  *
FROM
  `predictive-behavior-analytics.Section3.log_reg_sales_model_evaluation`
UNION ALL
SELECT
  'Boosted Tree Classifier' AS model,
  *
FROM
  `predictive-behavior-analytics.Section3.boosted_tree_sales_model_evaluation`
UNION ALL
SELECT
  'XGBoost' AS model,
  *
FROM
  `predictive-behavior-analytics.Section3.xgboost_sales_model_evaluation`
UNION ALL
SELECT
  'Deep Neural Network' AS model,
  *
FROM
  `predictive-behavior-analytics.Section3.dnn_sales_model_evaluation`;

-- Query the Model Comparison Table to Compare Performance Metrics
SELECT
  model,
  roc_auc AS AUC,
  precision,
  recall,
  accuracy,
  f1_score
FROM
  `predictive-behavior-analytics.Section3.model_comparison`
ORDER BY
  model;
