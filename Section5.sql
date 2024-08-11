-- Data Preparation
CREATE OR REPLACE TABLE `predictive-behavior-analytics.Section5.customer_transaction_data` AS
SELECT
  CONCAT(fullVisitorId, CAST(visitId AS STRING)) AS customer_id,
  IFNULL(totals.transactionRevenue, 0) / 1000000 AS revenue,
  PARSE_DATE('%Y%m%d', date) AS transaction_date,  -- Ensure the date is parsed correctly for time series analysis
  device.deviceCategory AS device_type,
  geoNetwork.country AS country
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
  _TABLE_SUFFIX BETWEEN '20160801' AND '20170731';

-- Define High-Value Customers
CREATE OR REPLACE TABLE `predictive-behavior-analytics.Section5.high_value_customers` AS
WITH customer_revenue AS (
  SELECT
    customer_id,
    SUM(revenue) AS total_revenue,
    COUNT(*) AS transaction_count,
    AVG(revenue) AS avg_revenue_per_transaction
  FROM
    `predictive-behavior-analytics.Section5.customer_transaction_data`
  GROUP BY
    customer_id
),
percentile_80th AS (
  SELECT
    APPROX_QUANTILES(total_revenue, 100)[OFFSET(80)] AS p80_revenue
  FROM
    customer_revenue
)
SELECT
  cr.customer_id,
  cr.total_revenue,
  cr.transaction_count,
  cr.avg_revenue_per_transaction,
  IF(cr.total_revenue > p.p80_revenue, 1, 0) AS high_value_status  -- Binary label for Logistic Regression and Random Forest
FROM
  customer_revenue cr,
  percentile_80th p;

-- Feature Engineering: Categorical and Numerical Features
CREATE OR REPLACE TABLE `predictive-behavior-analytics.Section5.customer_features` AS
SELECT
  hvc.customer_id,
  hvc.total_revenue,
  hvc.transaction_count,
  hvc.avg_revenue_per_transaction,
  hvc.high_value_status,  -- Include the high-value status for classification models
  ct.device_type,
  ct.country,
  CAST(ct.device_type AS STRING) AS device_type_cat,
  CAST(ct.country AS STRING) AS country_cat,
  (hvc.transaction_count - (SELECT AVG(transaction_count) FROM `predictive-behavior-analytics.Section5.high_value_customers`)) / (SELECT STDDEV(transaction_count) FROM `predictive-behavior-analytics.Section5.high_value_customers`) AS normalized_transaction_count,
  (hvc.avg_revenue_per_transaction - (SELECT AVG(avg_revenue_per_transaction) FROM `predictive-behavior-analytics.Section5.high_value_customers`)) / (SELECT STDDEV(avg_revenue_per_transaction) FROM `predictive-behavior-analytics.Section5.high_value_customers`) AS normalized_avg_revenue,
  ct.transaction_date  -- Keep the transaction date for time series forecasting
FROM
  `predictive-behavior-analytics.Section5.high_value_customers` hvc
JOIN
  `predictive-behavior-analytics.Section5.customer_transaction_data` ct
ON
  hvc.customer_id = ct.customer_id;

-- Logistic Regression
CREATE OR REPLACE MODEL `predictive-behavior-analytics.Section5.logistic_regression_model`
OPTIONS(model_type='logistic_reg', input_label_cols=['high_value_status']) AS
SELECT
  normalized_transaction_count,
  normalized_avg_revenue,
  device_type_cat,
  country_cat,
  high_value_status  -- Include the target column
FROM
  `predictive-behavior-analytics.Section5.customer_features`;

-- K-Means Clustering
CREATE OR REPLACE MODEL `predictive-behavior-analytics.Section5.kmeans_model`
OPTIONS(model_type='kmeans', num_clusters=5) AS
SELECT
  normalized_transaction_count,
  normalized_avg_revenue,
  device_type_cat,
  country_cat
FROM
  `predictive-behavior-analytics.Section5.customer_features`;

-- Random Forest Classifier
CREATE OR REPLACE MODEL `predictive-behavior-analytics.Section5.random_forest_model`
OPTIONS(model_type='random_forest_classifier', input_label_cols=['high_value_status']) AS
SELECT
  normalized_transaction_count,
  normalized_avg_revenue,
  device_type_cat,
  country_cat,
  high_value_status  -- Include the target column
FROM
  `predictive-behavior-analytics.Section5.customer_features`;

-- Time Series Forecasting
CREATE OR REPLACE MODEL `predictive-behavior-analytics.Section5.time_series_model`
OPTIONS(model_type='arima', time_series_timestamp_col='transaction_date', time_series_data_col='total_revenue') AS  -- Changed to total_revenue
SELECT
  transaction_date,
  SUM(revenue) AS total_revenue  -- Ensure 'revenue' is aggregated correctly for time series forecasting
FROM
  `predictive-behavior-analytics.Section5.customer_transaction_data`  -- Use the original transaction data for accurate revenue aggregation
GROUP BY
  transaction_date;

-- Evaluate the Logistic Regression Model
CREATE OR REPLACE TABLE `predictive-behavior-analytics.Section5.logistic_regression_evaluation` AS
SELECT
  *
FROM
  ML.EVALUATE(
    MODEL `predictive-behavior-analytics.Section5.logistic_regression_model`,
    (
      SELECT
        normalized_transaction_count,
        normalized_avg_revenue,
        device_type_cat,
        country_cat,
        high_value_status
      FROM
        `predictive-behavior-analytics.Section5.customer_features`
    )
  );

-- ROC Curve for Logistic Regression
CREATE OR REPLACE TABLE `predictive-behavior-analytics.Section5.logistic_regression_roc` AS
SELECT
  *
FROM
  ML.ROC_CURVE(
    MODEL `predictive-behavior-analytics.Section5.logistic_regression_model`,
    (
      SELECT
        normalized_transaction_count,
        normalized_avg_revenue,
        device_type_cat,
        country_cat,
        high_value_status
      FROM
        `predictive-behavior-analytics.Section5.customer_features`
    )
  );

-- Evaluate the K-Means Clustering Model
CREATE OR REPLACE TABLE `predictive-behavior-analytics.Section5.kmeans_evaluation` AS
SELECT
  *
FROM
  ML.EVALUATE(
    MODEL `predictive-behavior-analytics.Section5.kmeans_model`,
    (
      SELECT
        normalized_transaction_count,
        normalized_avg_revenue,
        device_type_cat,
        country_cat
      FROM
        `predictive-behavior-analytics.Section5.customer_features`
    )
  );
  
-- Get the cluster centroids for interpretation
CREATE OR REPLACE TABLE `predictive-behavior-analytics.Section5.kmeans_centroids` AS
SELECT
  *
FROM
  ML.CENTROIDS(MODEL `predictive-behavior-analytics.Section5.kmeans_model`);


-- Evaluate the Random Forest Classifier Model
CREATE OR REPLACE TABLE `predictive-behavior-analytics.Section5.random_forest_evaluation` AS
SELECT
  *
FROM
  ML.EVALUATE(
    MODEL `predictive-behavior-analytics.Section5.random_forest_model`,
    (
      SELECT
        normalized_transaction_count,
        normalized_avg_revenue,
        device_type_cat,
        country_cat,
        high_value_status
      FROM
        `predictive-behavior-analytics.Section5.customer_features`
    )
  );

-- ROC Curve for Random Forest
CREATE OR REPLACE TABLE `predictive-behavior-analytics.Section5.random_forest_roc` AS
SELECT
  *
FROM
  ML.ROC_CURVE(
    MODEL `predictive-behavior-analytics.Section5.random_forest_model`,
    (
      SELECT
        normalized_transaction_count,
        normalized_avg_revenue,
        device_type_cat,
        country_cat,
        high_value_status
      FROM
        `predictive-behavior-analytics.Section5.customer_features`
    )
  );

-- Feature Importance from Random Forest
CREATE OR REPLACE TABLE `predictive-behavior-analytics.Section5.random_forest_feature_importance` AS
SELECT
  *
FROM
  ML.FEATURE_IMPORTANCE(MODEL `predictive-behavior-analytics.Section5.random_forest_model`);


-- Evaluate the Time Series ARIMA Model
CREATE OR REPLACE TABLE `predictive-behavior-analytics.Section5.arima_evaluation` AS
SELECT
  *
FROM
  ML.EVALUATE(
    MODEL `predictive-behavior-analytics.Section5.time_series_model`,
    (
      SELECT
        transaction_date,
        SUM(revenue) AS total_revenue
      FROM
        `predictive-behavior-analytics.Section5.customer_transaction_data`
      GROUP BY
        transaction_date
    )
  );

-- Forecast future revenue using the ARIMA model
CREATE OR REPLACE TABLE `predictive-behavior-analytics.Section5.arima_forecast` AS
SELECT
  *
FROM
  ML.FORECAST(
    MODEL `predictive-behavior-analytics.Section5.time_series_model`,
    STRUCT(30 AS horizon, 0.8 AS confidence_level)  -- Example: forecast for the next 30 days
  );
