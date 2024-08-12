-- Examine the structure of the dataset
SELECT
  *
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`
LIMIT 1;
-- Developing Critical Features
CREATE OR REPLACE TABLE `predictive-behavior-analytics.Section7.user_features` AS
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
)
SELECT
  fullVisitorId,
  MAX(CASE WHEN transactions > 0 THEN 1 ELSE 0 END) AS has_converted,
  COUNT(DISTINCT visit_date) AS num_visits,
  AVG(timeOnSite) AS avg_time_on_site,
  AVG(pageviews) AS avg_pageviews,
  MAX(deviceCategory) AS device_category,
  MAX(country) AS country,
  MAX(medium) AS traffic_medium,
  SUM(pageviews) AS total_pageviews,
  SUM(timeOnSite) AS total_time_on_site,
  DATE_DIFF(MAX(visit_date), MIN(visit_date), DAY) AS days_since_first_visit
FROM
  user_sessions
GROUP BY
  fullVisitorId;

  CREATE OR REPLACE MODEL `predictive-behavior-analytics.Section7.user_conversion_logistic`
OPTIONS(model_type='logistic_reg', input_label_cols=['has_converted']) AS
SELECT
  * EXCEPT(fullVisitorId)
FROM
  `predictive-behavior-analytics.Section7.user_features`;

  -- Logistic Regression Model
  CREATE OR REPLACE MODEL `predictive-behavior-analytics.Section7.user_conversion_logistic`
OPTIONS(model_type='logistic_reg', input_label_cols=['has_converted']) AS
SELECT
  * EXCEPT(fullVisitorId)
FROM
  `predictive-behavior-analytics.Section7.user_features`;

  -- Random Forest Model
  CREATE OR REPLACE MODEL `predictive-behavior-analytics.Section7.user_conversion_random_forest`
OPTIONS(model_type='random_forest_classifier', input_label_cols=['has_converted']) AS
SELECT
  * EXCEPT(fullVisitorId)
FROM
  `predictive-behavior-analytics.Section7.user_features`;

  -- XGBoost Model
  CREATE OR REPLACE MODEL `predictive-behavior-analytics.Section7.user_conversion_xgboost`
OPTIONS(model_type='boosted_tree_classifier', input_label_cols=['has_converted']) AS
SELECT
  * EXCEPT(fullVisitorId)
FROM
  `predictive-behavior-analytics.Section7.user_features`;

  -- Evaluation
-- Evaluate Logistic Regression Model
SELECT
  'Logistic Regression' AS model,
  *
FROM
  ML.EVALUATE(MODEL `predictive-behavior-analytics.Section7.user_conversion_logistic`,
    (
    SELECT
      * EXCEPT(fullVisitorId)
    FROM
      `predictive-behavior-analytics.Section7.user_features`
    )
  );

-- Evaluate Random Forest Model
SELECT
  'Random Forest' AS model,
  *
FROM
  ML.EVALUATE(MODEL `predictive-behavior-analytics.Section7.user_conversion_random_forest`,
    (
    SELECT
      * EXCEPT(fullVisitorId)
    FROM
      `predictive-behavior-analytics.Section7.user_features`
    )
  );

-- Evaluate XGBoost Model
SELECT
  'XGBoost' AS model,
  *
FROM
  ML.EVALUATE(MODEL `predictive-behavior-analytics.Section7.user_conversion_xgboost`,
    (
    SELECT
      * EXCEPT(fullVisitorId)
    FROM
      `predictive-behavior-analytics.Section7.user_features`
    )
  );

-- Confusion Matrices
-- Confusion Matrix for Logistic Regression
SELECT
  'Logistic Regression' AS model,
  has_converted AS actual,
  predicted_has_converted AS predicted,
  COUNT(*) AS count
FROM
  ML.PREDICT(MODEL `predictive-behavior-analytics.Section7.user_conversion_logistic`,
    (
    SELECT
      * EXCEPT(fullVisitorId)
    FROM
      `predictive-behavior-analytics.Section7.user_features`
    )
  )
GROUP BY 1, 2, 3
ORDER BY 2, 3;

-- Confusion Matrix for Random Forest
SELECT
  'Random Forest' AS model,
  has_converted AS actual,
  predicted_has_converted AS predicted,
  COUNT(*) AS count
FROM
  ML.PREDICT(MODEL `predictive-behavior-analytics.Section7.user_conversion_random_forest`,
    (
    SELECT
      * EXCEPT(fullVisitorId)
    FROM
      `predictive-behavior-analytics.Section7.user_features`
    )
  )
GROUP BY 1, 2, 3
ORDER BY 2, 3;

-- Confusion Matrix for XGBoost
SELECT
  'XGBoost' AS model,
  has_converted AS actual,
  predicted_has_converted AS predicted,
  COUNT(*) AS count
FROM
  ML.PREDICT(MODEL `predictive-behavior-analytics.Section7.user_conversion_xgboost`,
    (
    SELECT
      * EXCEPT(fullVisitorId)
    FROM
      `predictive-behavior-analytics.Section7.user_features`
    )
  )
GROUP BY 1, 2, 3
ORDER BY 2, 3;

WITH logistic_weights AS (
  SELECT
    'Logistic Regression' AS model,
    processed_input AS feature,
    ABS(weight) AS importance
  FROM
    ML.WEIGHTS(MODEL `predictive-behavior-analytics.Section7.user_conversion_logistic`)
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
    ML.FEATURE_IMPORTANCE(MODEL `predictive-behavior-analytics.Section7.user_conversion_random_forest`)
),
xgboost_importance AS (
  SELECT
    'XGBoost' AS model,
    feature,
    importance_weight AS importance
  FROM
    ML.FEATURE_IMPORTANCE(MODEL `predictive-behavior-analytics.Section7.user_conversion_xgboost`)
)
SELECT * FROM logistic_weights
UNION ALL
SELECT * FROM random_forest_importance
UNION ALL
SELECT * FROM xgboost_importance
ORDER BY model, importance DESC;