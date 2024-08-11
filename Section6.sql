-- Create Customer Features Table
CREATE OR REPLACE TABLE `predictive-behavior-analytics.Section6.customer_features` AS
SELECT
  CONCAT(fullVisitorId, CAST(visitId AS STRING)) AS customer_id,
  SUM(IFNULL(totals.transactionRevenue, 0)) / 1000000 AS total_revenue, 
  COUNT(totals.transactionRevenue) AS transaction_count,
  AVG(IFNULL(totals.transactionRevenue, 0)) / 1000000 AS avg_transaction_value, 
  MAX(totals.timeOnSite) AS max_session_duration,
  MIN(totals.timeOnSite) AS min_session_duration,
  AVG(totals.timeOnSite) AS avg_session_duration,
  device.deviceCategory AS device_type,
  geoNetwork.country AS country
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
  _TABLE_SUFFIX BETWEEN '20160801' AND '20170731'
GROUP BY
  customer_id, device_type, country;

-- Model
-- K-Means Clustering on Original Features
CREATE OR REPLACE MODEL `predictive-behavior-analytics.Section6.kmeans_customer_segmentation_revised`
OPTIONS(model_type='kmeans', num_clusters=5) AS
SELECT
  total_revenue,
  transaction_count,
  avg_transaction_value,
  avg_session_duration
FROM
  `predictive-behavior-analytics.Section6.customer_features`;

-- Retrieving Centroid Values
SELECT
  centroid_id,
  MAX(CASE WHEN feature = 'total_revenue' THEN numerical_value END) AS avg_total_revenue,
  MAX(CASE WHEN feature = 'transaction_count' THEN numerical_value END) AS avg_transaction_count,
  MAX(CASE WHEN feature = 'avg_transaction_value' THEN numerical_value END) AS avg_transaction_value,
  MAX(CASE WHEN feature = 'avg_session_duration' THEN numerical_value END) AS avg_session_duration
FROM
  ML.CENTROIDS(MODEL `predictive-behavior-analytics.Section6.kmeans_customer_segmentation_revised`)
GROUP BY
  centroid_id
ORDER BY
  avg_total_revenue DESC;

-- Assign Clusters to Customers for K-Means on Original Features
CREATE OR REPLACE TABLE `predictive-behavior-analytics.Section6.customer_clusters` AS
SELECT
  customer_id,
  CENTROID_ID AS cluster_id
FROM
  ML.PREDICT(MODEL `predictive-behavior-analytics.Section6.kmeans_customer_segmentation_revised`,
    (
      SELECT
        customer_id,
        total_revenue,
        transaction_count,
        avg_transaction_value,
        avg_session_duration
      FROM
        `predictive-behavior-analytics.Section6.customer_features`
    )
  );

-- Evaluate Customer Segments using approximate Silhouette Score for K-Means on Original Features
WITH point_distances AS (
  SELECT
    f.customer_id,
    c.cluster_id AS assigned_cluster,
    cent.centroid_id,
    SQRT(POW(f.total_revenue - cent.avg_total_revenue, 2) +
         POW(f.transaction_count - cent.avg_transaction_count, 2) +
         POW(f.avg_transaction_value - cent.avg_transaction_value, 2) +
         POW(f.avg_session_duration - cent.avg_session_duration, 2)) AS distance
  FROM
    `predictive-behavior-analytics.Section6.customer_features` f
  CROSS JOIN
    (SELECT 
       centroid_id,
       MAX(CASE WHEN feature = 'total_revenue' THEN numerical_value END) AS avg_total_revenue,
       MAX(CASE WHEN feature = 'transaction_count' THEN numerical_value END) AS avg_transaction_count,
       MAX(CASE WHEN feature = 'avg_transaction_value' THEN numerical_value END) AS avg_transaction_value,
       MAX(CASE WHEN feature = 'avg_session_duration' THEN numerical_value END) AS avg_session_duration
     FROM
       ML.CENTROIDS(MODEL `predictive-behavior-analytics.Section6.kmeans_customer_segmentation_revised`)
     GROUP BY
       centroid_id) cent
  JOIN
    `predictive-behavior-analytics.Section6.customer_clusters` c
  ON
    f.customer_id = c.customer_id
),
silhouette_data AS (
  SELECT
    customer_id,
    assigned_cluster,
    MIN(CASE WHEN centroid_id = assigned_cluster THEN distance END) AS a,
    MIN(CASE WHEN centroid_id != assigned_cluster THEN distance END) AS b
  FROM
    point_distances
  GROUP BY
    customer_id, assigned_cluster
)
SELECT
  AVG((b - a) / GREATEST(a, b)) AS silhouette_score
FROM
  silhouette_data;

-- Apply PCA to Customer Features
CREATE OR REPLACE MODEL `predictive-behavior-analytics.Section6.pca_customer_features`
OPTIONS(model_type='pca', num_principal_components=3) AS
SELECT
  total_revenue,
  transaction_count,
  avg_transaction_value,
  avg_session_duration
FROM
  `predictive-behavior-analytics.Section6.customer_features`;




-- Retrieve the Principal Components 

CREATE OR REPLACE TABLE `predictive-behavior-analytics.Section6.pca_transformed_features` AS
SELECT
  customer_id,
  principal_component_1,
  principal_component_2,
  principal_component_3
FROM
  ML.PREDICT(MODEL `predictive-behavior-analytics.Section6.pca_customer_features`,
    (
      SELECT
        customer_id,
        total_revenue,
        transaction_count,
        avg_transaction_value,
        avg_session_duration
      FROM
        `predictive-behavior-analytics.Section6.customer_features`
    )
  );


  -- K-Means Clustering on PCA-Transformed Features
CREATE OR REPLACE MODEL `predictive-behavior-analytics.Section6.kmeans_pca_customer_segmentation`
OPTIONS(model_type='kmeans', num_clusters=5) AS
SELECT
  principal_component_1,
  principal_component_2,
  principal_component_3
FROM
  `predictive-behavior-analytics.Section6.pca_transformed_features`;

  -- Retrieving Centroid Values for PCA + K-Means
SELECT
  centroid_id,
  MAX(CASE WHEN feature = 'principal_component_1' THEN numerical_value END) AS avg_principal_component_1,
  MAX(CASE WHEN feature = 'principal_component_2' THEN numerical_value END) AS avg_principal_component_2,
  MAX(CASE WHEN feature = 'principal_component_3' THEN numerical_value END) AS avg_principal_component_3
FROM
  ML.CENTROIDS(MODEL `predictive-behavior-analytics.Section6.kmeans_pca_customer_segmentation`)
GROUP BY
  centroid_id
ORDER BY
  avg_principal_component_1 DESC;

-- Assign Clusters to Customers for PCA + K-Means
CREATE OR REPLACE TABLE `predictive-behavior-analytics.Section6.customer_clusters_pca` AS
SELECT
  customer_id,
  CENTROID_ID AS cluster_id
FROM
  ML.PREDICT(MODEL `predictive-behavior-analytics.Section6.kmeans_pca_customer_segmentation`,
    (
      SELECT
        customer_id,
        principal_component_1,
        principal_component_2,
        principal_component_3
      FROM
        `predictive-behavior-analytics.Section6.pca_transformed_features`
    )
  );
-- Evaluate Customer Segments using approximate Silhouette Score for PCA + K-Means
WITH point_distances AS (
  SELECT
    f.customer_id,
    c.cluster_id AS assigned_cluster,
    cent.centroid_id,
    SQRT(POW(f.principal_component_1 - cent.avg_principal_component_1, 2) +
         POW(f.principal_component_2 - cent.avg_principal_component_2, 2) +
         POW(f.principal_component_3 - cent.avg_principal_component_3, 2)) AS distance
  FROM
    `predictive-behavior-analytics.Section6.pca_transformed_features` f
  CROSS JOIN
    (SELECT 
       centroid_id,
       MAX(CASE WHEN feature = 'principal_component_1' THEN numerical_value END) AS avg_principal_component_1,
       MAX(CASE WHEN feature = 'principal_component_2' THEN numerical_value END) AS avg_principal_component_2,
       MAX(CASE WHEN feature = 'principal_component_3' THEN numerical_value END) AS avg_principal_component_3
     FROM
       ML.CENTROIDS(MODEL `predictive-behavior-analytics.Section6.kmeans_pca_customer_segmentation`)
     GROUP BY
       centroid_id) cent
  JOIN
    `predictive-behavior-analytics.Section6.customer_clusters_pca` c
  ON
    f.customer_id = c.customer_id
),
silhouette_data AS (
  SELECT
    customer_id,
    assigned_cluster,
    MIN(CASE WHEN centroid_id = assigned_cluster THEN distance END) AS a,
    MIN(CASE WHEN centroid_id != assigned_cluster THEN distance END) AS b
  FROM
    point_distances
  GROUP BY
    customer_id, assigned_cluster
)
SELECT
  AVG((b - a) / GREATEST(a, b)) AS silhouette_score
FROM
  silhouette_data;

