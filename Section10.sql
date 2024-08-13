-- Preparing the Data
CREATE OR REPLACE TABLE `predictive-behavior-analytics.Section10.normalized_campaign_performance` AS
WITH raw_data AS (
  SELECT
    DATE(PARSE_DATE('%Y%m%d', date)) AS date,
    trafficSource.source AS source,
    trafficSource.medium AS medium,
    trafficSource.campaign AS campaign,
    COUNT(DISTINCT fullVisitorId) AS users,
    SUM(totals.transactions) AS transactions,
    SUM(totals.transactionRevenue) / 1000000 AS revenue,
    SUM(totals.pageviews) AS pageviews,
    IF(SUM(totals.transactions) > 0, 1, 0) AS made_purchase
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE
    _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
  GROUP BY
    date, source, medium, campaign
),
stats AS (
  SELECT
    AVG(users) AS avg_users, STDDEV(users) AS stddev_users,
    AVG(pageviews) AS avg_pageviews, STDDEV(pageviews) AS stddev_pageviews,
    AVG(revenue) AS avg_revenue, STDDEV(revenue) AS stddev_revenue
  FROM raw_data
)
SELECT
  date,
  source,
  medium,
  campaign,
  (users - avg_users) / stddev_users AS normalized_users,
  (pageviews - avg_pageviews) / stddev_pageviews AS normalized_pageviews,
  (revenue - avg_revenue) / stddev_revenue AS normalized_revenue,
  made_purchase,
  EXTRACT(DAYOFWEEK FROM date) AS day_of_week,
  EXTRACT(MONTH FROM date) AS month
FROM raw_data, stats
WHERE campaign IS NOT NULL;

-- Building Predictive Models
-- Linear 
CREATE OR REPLACE MODEL `predictive-behavior-analytics.Section10.campaign_purchase_lr`
OPTIONS(model_type='LOGISTIC_REG', input_label_cols=['made_purchase']) AS
SELECT
  day_of_week,
  month,
  source,
  medium,
  campaign,
  normalized_users,
  normalized_pageviews,
  made_purchase
FROM
  `predictive-behavior-analytics.Section10.normalized_campaign_performance`;

-- Random Forest
CREATE OR REPLACE MODEL `predictive-behavior-analytics.Section10.campaign_purchase_rf`
OPTIONS(model_type='RANDOM_FOREST_CLASSIFIER', input_label_cols=['made_purchase']) AS
SELECT
  day_of_week,
  month,
  source,
  medium,
  campaign,
  normalized_users,
  normalized_pageviews,
  made_purchase
FROM
  `predictive-behavior-analytics.Section10.normalized_campaign_performance`;

  -- XGBoost
  CREATE OR REPLACE MODEL `predictive-behavior-analytics.Section10.campaign_purchase_xgb`
OPTIONS(model_type='BOOSTED_TREE_CLASSIFIER', input_label_cols=['made_purchase']) AS
SELECT
  day_of_week,
  month,
  source,
  medium,
  campaign,
  normalized_users,
  normalized_pageviews,
  made_purchase
FROM
  `predictive-behavior-analytics.Section10.normalized_campaign_performance`;

  -- Deep Neural Network (DNN)
  CREATE OR REPLACE MODEL `predictive-behavior-analytics.Section10.campaign_purchase_dnn`
OPTIONS(model_type='DNN_CLASSIFIER', input_label_cols=['made_purchase']) AS
SELECT
  day_of_week,
  month,
  source,
  medium,
  campaign,
  normalized_users,
  normalized_pageviews,
  made_purchase
FROM
  `predictive-behavior-analytics.Section10.normalized_campaign_performance`;

-- Model Comparison (Corrected)
WITH model_evaluation AS (
  SELECT 'Logistic Regression' AS model, *
  FROM ML.EVALUATE(MODEL `predictive-behavior-analytics.Section10.campaign_purchase_lr`)
  UNION ALL
  SELECT 'Random Forest' AS model, *
  FROM ML.EVALUATE(MODEL `predictive-behavior-analytics.Section10.campaign_purchase_rf`)
  UNION ALL
  SELECT 'XGBoost' AS model, *
  FROM ML.EVALUATE(MODEL `predictive-behavior-analytics.Section10.campaign_purchase_xgb`)
  UNION ALL
  SELECT 'Deep Neural Network' AS model, *
  FROM ML.EVALUATE(MODEL `predictive-behavior-analytics.Section10.campaign_purchase_dnn`)
)
SELECT
  model,
  precision,
  recall,
  f1_score,
  accuracy,
  roc_auc
FROM
  model_evaluation
ORDER BY
  f1_score DESC;

-- Using the Best Model for Optimization (Corrected)
-- This query uses the normalized data statistics for prediction
WITH stats AS (
  SELECT
    AVG(normalized_users) AS avg_normalized_users, 
    STDDEV(normalized_users) AS stddev_normalized_users,
    AVG(normalized_pageviews) AS avg_normalized_pageviews, 
    STDDEV(normalized_pageviews) AS stddev_normalized_pageviews
  FROM `predictive-behavior-analytics.Section10.normalized_campaign_performance`
),
input_data AS (
  SELECT
    5 AS day_of_week,  -- Friday
    4 AS month,        -- April
    'google' AS source,
    'cpc' AS medium,
    'spring_sale' AS campaign,
    -- Note: We're not re-normalizing here, just using placeholder values
    1.0 AS normalized_users,
    1.0 AS normalized_pageviews
  FROM stats
)
SELECT
  *
FROM
  ML.PREDICT(MODEL `predictive-behavior-analytics.Section10.campaign_purchase_rf`,
    (SELECT * FROM input_data));