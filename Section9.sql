-- Step 1: Preparing the Data
CREATE OR REPLACE TABLE `predictive-behavior-analytics.Section9.user_item_interactions` AS
SELECT
  fullVisitorId,
  CONCAT(product.productSKU, '_', product.v2ProductName) AS item_id,
  SUM(product.productQuantity) AS interaction_count,
  SUM(product.productPrice * product.productQuantity) / 1000000 AS total_revenue
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
  UNNEST(hits) AS hits,
  UNNEST(hits.product) AS product
WHERE
  _TABLE_SUFFIX BETWEEN '20170701' AND '20170731'
  AND hits.eCommerceAction.action_type = '6'  -- Completed purchase
GROUP BY
  fullVisitorId, item_id
HAVING
  interaction_count > 0;

-- Step 2: Building the Matrix Factorization Model
CREATE OR REPLACE MODEL `predictive-behavior-analytics.Section9.item_recommendation_model`
OPTIONS(
  model_type='MATRIX_FACTORIZATION',
  user_col='fullVisitorId',
  item_col='item_id',
  rating_col='interaction_count',
  feedback_type='implicit'
) AS
SELECT
  fullVisitorId,
  item_id,
  interaction_count
FROM
  `predictive-behavior-analytics.Section9.user_item_interactions`;

-- Step 3: Generating Recommendations
WITH user_item_pairs AS (
  SELECT DISTINCT
    ui1.fullVisitorId,
    ui2.item_id
  FROM
    `predictive-behavior-analytics.Section9.user_item_interactions` ui1
  CROSS JOIN
    (SELECT DISTINCT item_id FROM `predictive-behavior-analytics.Section9.user_item_interactions`) ui2
),
predictions AS (
  SELECT *
  FROM ML.PREDICT(MODEL `predictive-behavior-analytics.Section9.item_recommendation_model`,
    (SELECT * FROM user_item_pairs))
)
SELECT
  fullVisitorId AS user_id,
  ARRAY_AGG(STRUCT(item_id, predicted_interaction_count_confidence)
            ORDER BY predicted_interaction_count_confidence DESC
            LIMIT 5) AS top_5_recommendations
FROM predictions
GROUP BY fullVisitorId
LIMIT 10;

-- Step 4: Evaluating the Model
SELECT
  *
FROM
  ML.EVALUATE(MODEL `predictive-behavior-analytics.Section9.item_recommendation_model`,
    (SELECT
       fullVisitorId,
       item_id,
       interaction_count
     FROM
       `predictive-behavior-analytics.Section9.user_item_interactions`));

-- Optional: Inspecting ML.PREDICT Output (for debugging)
SELECT *
FROM ML.PREDICT(MODEL `predictive-behavior-analytics.Section9.item_recommendation_model`,
  (SELECT fullVisitorId, item_id 
   FROM `predictive-behavior-analytics.Section9.user_item_interactions`
   LIMIT 10));