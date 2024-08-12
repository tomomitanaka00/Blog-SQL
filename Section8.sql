-- Examine the structure of the dataset
SELECT
  *
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`
LIMIT 1;

-- Developing Critical Features for Churn Prediction
CREATE OR REPLACE TABLE `predictive-behavior-analytics.Section8.user_churn_features` AS
WITH user_sessions AS (
  SELECT
    fullVisitorId,
    PARSE_DATE('%Y%m%d', date) AS visit_date,
    totals.transactions,
    totals.timeOnSite,
    totals.pageviews,
    device.deviceCategory,
    geoNetwork.country,
    trafficSource.medium
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE
    _TABLE_SUFFIX BETWEEN '20170701' AND '20170801'
),
last_visit AS (
  SELECT
    fullVisitorId,
    MAX(visit_date) AS last_visit_date
  FROM
    user_sessions
  GROUP BY
    fullVisitorId
)
SELECT
  us.fullVisitorId,
  CASE
    WHEN DATE_DIFF(DATE('2017-08-01'), lv.last_visit_date, DAY) > 30 THEN 1
    ELSE 0
  END AS churned,
  COUNT(DISTINCT us.visit_date) AS num_visits,
  AVG(us.timeOnSite) AS avg_time_on_site,
  AVG(us.pageviews) AS avg_pageviews,
  MAX(us.deviceCategory) AS device_category,
  MAX(us.country) AS country,
  MAX(us.medium) AS traffic_medium,
  SUM(us.pageviews) AS total_pageviews,
  SUM(us.timeOnSite) AS total_time_on_site,
  DATE_DIFF(MAX(us.visit_date), MIN(us.visit_date), DAY) AS days_as_customer,
  SUM(us.transactions) AS total_transactions
FROM
  user_sessions us
JOIN
  last_visit lv ON us.fullVisitorId = lv.fullVisitorId
GROUP BY
  us.fullVisitorId, lv.last_visit_date;

-- Logistic Regression Model for Churn
CREATE OR REPLACE MODEL `predictive-behavior-analytics.Section8.churn_logistic`
OPTIONS(model_type='logistic_reg', input_label_cols=['churned']) AS
SELECT
  * EXCEPT(fullVisitorId)
FROM
  `predictive-behavior-analytics.Section8.user_churn_features`;

-- Random Forest Model for Churn
CREATE OR REPLACE MODEL `predictive-behavior-analytics.Section8.churn_random_forest`
OPTIONS(model_type='random_forest_classifier', input_label_cols=['churned']) AS
SELECT
  * EXCEPT(fullVisitorId)
FROM
  `predictive-behavior-analytics.Section8.user_churn_features`;

-- XGBoost Model for Churn
CREATE OR REPLACE MODEL `predictive-behavior-analytics.Section8.churn_xgboost`
OPTIONS(model_type='boosted_tree_classifier', input_label_cols=['churned']) AS
SELECT
  * EXCEPT(fullVisitorId)
FROM
  `predictive-behavior-analytics.Section8.user_churn_features`;

-- Evaluation
-- Evaluate Logistic Regression Model
SELECT
  'Logistic Regression' AS model,
  *
FROM
  ML.EVALUATE(MODEL `predictive-behavior-analytics.Section8.churn_logistic`,
    (
    SELECT
      * EXCEPT(fullVisitorId)
    FROM
      `predictive-behavior-analytics.Section8.user_churn_features`
    )
  );

-- Evaluate Random Forest Model
SELECT
  'Random Forest' AS model,
  *
FROM
  ML.EVALUATE(MODEL `predictive-behavior-analytics.Section8.churn_random_forest`,
    (
    SELECT
      * EXCEPT(fullVisitorId)
    FROM
      `predictive-behavior-analytics.Section8.user_churn_features`
    )
  );

-- Evaluate XGBoost Model
SELECT
  'XGBoost' AS model,
  *
FROM
  ML.EVALUATE(MODEL `predictive-behavior-analytics.Section8.churn_xgboost`,
    (
    SELECT
      * EXCEPT(fullVisitorId)
    FROM
      `predictive-behavior-analytics.Section8.user_churn_features`
    )
  );

-- Confusion Matrices
-- Confusion Matrix for Logistic Regression
SELECT
  'Logistic Regression' AS model,
  churned AS actual,
  predicted_churned AS predicted,
  COUNT(*) AS count
FROM
  ML.PREDICT(MODEL `predictive-behavior-analytics.Section8.churn_logistic`,
    (
    SELECT
      * EXCEPT(fullVisitorId)
    FROM
      `predictive-behavior-analytics.Section8.user_churn_features`
    )
  )
GROUP BY 1, 2, 3
ORDER BY 2, 3;

-- Confusion Matrix for Random Forest
SELECT
  'Random Forest' AS model,
  churned AS actual,
  predicted_churned AS predicted,
  COUNT(*) AS count
FROM
  ML.PREDICT(MODEL `predictive-behavior-analytics.Section8.churn_random_forest`,
    (
    SELECT
      * EXCEPT(fullVisitorId)
    FROM
      `predictive-behavior-analytics.Section8.user_churn_features`
    )
  )
GROUP BY 1, 2, 3
ORDER BY 2, 3;

-- Confusion Matrix for XGBoost
SELECT
  'XGBoost' AS model,
  churned AS actual,
  predicted_churned AS predicted,
  COUNT(*) AS count
FROM
  ML.PREDICT(MODEL `predictive-behavior-analytics.Section8.churn_xgboost`,
    (
    SELECT
      * EXCEPT(fullVisitorId)
    FROM
      `predictive-behavior-analytics.Section8.user_churn_features`
    )
  )
GROUP BY 1, 2, 3
ORDER BY 2, 3;

-- Feature Importance
WITH logistic_weights AS (
  SELECT
    'Logistic Regression' AS model,
    processed_input AS feature,
    ABS(weight) AS importance
  FROM
    ML.WEIGHTS(MODEL `predictive-behavior-analytics.Section8.churn_logistic`)
  WHERE
    processed_input != 'Intercept'
    AND weight IS NOT NULL
),
random_forest_importance AS (
  SELECT
    'Random Forest' AS model,
    feature,
    importance_weight AS importance
  FROM
    ML.FEATURE_IMPORTANCE(MODEL `predictive-behavior-analytics.Section8.churn_random_forest`)
),
xgboost_importance AS (
  SELECT
    'XGBoost' AS model,
    feature,
    importance_weight AS importance
  FROM
    ML.FEATURE_IMPORTANCE(MODEL `predictive-behavior-analytics.Section8.churn_xgboost`)
)
SELECT * FROM logistic_weights
UNION ALL
SELECT * FROM random_forest_importance
UNION ALL
SELECT * FROM xgboost_importance
ORDER BY model, importance DESC;