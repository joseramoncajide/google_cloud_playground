#standardSQL
CREATE TEMPORARY FUNCTION
  URL_DECODE(enc STRING)
  RETURNS STRING
  LANGUAGE js AS '''
  try {
    return decodeURIComponent(enc);;
  } catch (e) { return null }
  return null;
''';
SELECT
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', receiveTimestamp, 'Europe/Madrid') as date,
  JSON_EXTRACT_SCALAR(data,'$.hostname') hostname,
  JSON_EXTRACT_SCALAR(data,'$.pageURL') page_path,
  JSON_EXTRACT_SCALAR(data,'$.Referrer') referrer
FROM (
  SELECT
    receiveTimestamp,
    URL_DECODE(REGEXP_EXTRACT(httpRequest.requestUrl, r'&bq=([^&]*)')) AS data
  FROM
    `eam-poc-pixel.gcs_pixel_tracking_analytics_eam.requests_*`
 )


#standardSQL
CREATE TEMPORARY FUNCTION  URL_DECODE(enc STRING)  RETURNS STRING  LANGUAGE js AS """  try {    return decodeURIComponent(enc);;  } catch (e) { return null }  return null;""";SELECT FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', receiveTimestamp, 'Europe/Madrid') as date, JSON_EXTRACT_SCALAR(data,'$.hostname') hostname,  JSON_EXTRACT_SCALAR(data,'$.pageURL') page_path,  JSON_EXTRACT_SCALAR(data,'$.Referrer') referrer FROM (  SELECT receiveTimestamp,   URL_DECODE(REGEXP_EXTRACT(httpRequest.requestUrl, r'&bq=([^&]*)')) AS data  FROM    `eam-poc-pixel.gcs_pixel_tracking_analytics_eam.requests_*` )
