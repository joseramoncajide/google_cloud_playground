function runQuery() {
  var query = " \
CREATE TEMPORARY FUNCTION \
  URL_DECODE(enc STRING) \
  RETURNS STRING \
  LANGUAGE js AS ''' \
  try { \
    return decodeURIComponent(enc);; \
  } catch (e) { return null } \
  return null; \
'''; \
WITH \
  LatestTable AS ( \
  SELECT \
    MAX(_table_Suffix) AS TableName \
  FROM \
    `eam-poc-pixel.gcs_pixel_tracking_analytics_eam.*` \
  WHERE \
    REGEXP_CONTAINS(_Table_Suffix, 'requests') \
    ) \
SELECT \
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', receiveTimestamp, 'Europe/Madrid') as date, \
  JSON_EXTRACT_SCALAR(data, '$.client_id') client_id, \
  JSON_EXTRACT_SCALAR(data, '$.hostname') hostname, \
  JSON_EXTRACT_SCALAR(data, '$.pageURL') page_path, \
  JSON_EXTRACT_SCALAR(data, '$.Referrer') referrer, \
  browser, \
  CASE \
    WHEN browser = 'Firefox' THEN SUBSTR(user_agent, STRPOS(user_agent, 'Firefox') + 8, 100) \
    WHEN browser = 'Safari' THEN SUBSTR(user_agent, STRPOS(user_agent, 'Safari') + 7, 100) \
    WHEN browser = 'Chrome' THEN REGEXP_EXTRACT(SUBSTR(user_agent, STRPOS(user_agent, 'Chrome') + 7, 100), r'(^[^\s]+)') \
    WHEN browser = 'IE' THEN SUBSTR(user_agent, STRPOS(user_agent, 'MSIE') + 5, 3) \
    WHEN browser = 'iPhone Safari' THEN SUBSTR(user_agent, STRPOS(user_agent, 'Safari') + 7, 100) \
    WHEN browser = 'iPad Safari' THEN SUBSTR(user_agent, STRPOS(user_agent, 'Safari') + 7, 100) \
    ELSE 'Unknown' \
  END AS browser_version \
FROM ( \
  SELECT \
    receiveTimestamp, \
    URL_DECODE(REGEXP_EXTRACT(httpRequest.requestUrl, r'&bq=([^&]*)')) AS data, \
    httpRequest.userAgent AS user_agent, \
    CASE \
      WHEN httpRequest.userAgent LIKE '%Firefox/%' THEN 'Firefox' \
      WHEN httpRequest.userAgent LIKE '%Chrome/%' \
    OR httpRequest.userAgent LIKE '%CriOS%' THEN 'Chrome' \
      WHEN httpRequest.userAgent LIKE '%MSIE %' THEN 'IE' \
      WHEN httpRequest.userAgent LIKE '%MSIE+%' THEN 'IE' \
      WHEN httpRequest.userAgent LIKE '%Trident%' THEN 'IE' \
      WHEN httpRequest.userAgent LIKE '%iPhone%' THEN 'iPhone Safari' \
      WHEN httpRequest.userAgent LIKE '%iPad%' THEN 'iPad Safari' \
      WHEN httpRequest.userAgent LIKE '%Opera%' THEN 'Opera' \
      WHEN httpRequest.userAgent LIKE '%BlackBerry%' AND httpRequest.userAgent LIKE '%Version/%' THEN 'BlackBerry WebKit' \
      WHEN httpRequest.userAgent LIKE '%BlackBerry%' THEN 'BlackBerry' \
      WHEN httpRequest.userAgent LIKE '%Android%' THEN 'Android' \
      WHEN httpRequest.userAgent LIKE '%Safari%' THEN 'Safari' \
      WHEN httpRequest.userAgent LIKE '%bot%' THEN 'Bot' \
      WHEN httpRequest.userAgent LIKE '%http://%' THEN 'Bot' \
      WHEN httpRequest.userAgent LIKE '%www.%' THEN 'Bot' \
      WHEN httpRequest.userAgent LIKE '%Wget%' THEN 'Bot' \
      WHEN httpRequest.userAgent LIKE '%curl%' THEN 'Bot' \
      WHEN httpRequest.userAgent LIKE '%urllib%' THEN 'Bot' \
      ELSE 'Unknown' \
    END AS browser \
  FROM \
   `eam-poc-pixel.gcs_pixel_tracking_analytics_eam.*` WHERE _Table_Suffix = (SELECT TableName FROM LatestTable)) \
  "
  var configuration = {
    "query": {
    "useQueryCache": false,
    "destinationTable": {
          "projectId": "eam-poc-pixel",
          "datasetId": "eam_processed",
          "tableId": "log_eam"
        },
    "writeDisposition": "WRITE_TRUNCATE",
    "createDisposition": "CREATE_IF_NEEDED",
    "allowLargeResults": true,
    "useLegacySql": false,
      "timePartitioning": {
        "type": "DAY"
      },
   // "query": "CREATE TEMPORARY FUNCTION  URL_DECODE(enc STRING)  RETURNS STRING  LANGUAGE js AS '''  try {    return decodeURIComponent(enc);;  } catch (e) { return null }  return null;''';SELECT FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', receiveTimestamp, 'Europe/Madrid') as date, JSON_EXTRACT_SCALAR(data,'$.hostname') hostname,  JSON_EXTRACT_SCALAR(data,'$.pageURL') page_path,  JSON_EXTRACT_SCALAR(data,'$.Referrer') referrer FROM (  SELECT receiveTimestamp,   URL_DECODE(REGEXP_EXTRACT(httpRequest.requestUrl, r'&bq=([^&]*)')) AS data  FROM    `eam-poc-pixel.gcs_pixel_tracking_analytics_eam.requests_*` )"
    "query": query
    }
  };

  var job = {
    "configuration": configuration
  };

  var jobResult = BigQuery.Jobs.insert(job, "eam-poc-pixel");
  Logger.log(jobResult);
}
