
  gsutil mb gs://eam-gcs-pixel-tracking
  gsutil cp gs://solutions-public-assets/pixel-tracking/pixel.png  gs://eam-gcs-pixel-tracking
  gsutil acl ch -u AllUsers:R gs://eam-gcs-pixel-tracking
  gsutil -m acl set -R -a public-read gs://eam-gcs-pixel-tracking/pixel.png

  resource.type = http_load_balancer AND resource.labels.url_map_name = "eam-pixel-tracking"


https://storage.googleapis.com/eam-gcs-pixel-tracking/pixel.png?uid=19679&pn=checkout&purl=http%3A%2F%2Fexample.com%2Fpage&e=pl&pr=prod1;prod2:
http://35.227.246.2/pixel.png?uid=19679&pn=checkout&purl=http%3A%2F%2Fexample.com%2Fpage&e=pl&pr=prod1;prod2:


SELECT
  count(REGEXP_EXTRACT(httpRequest.requestUrl, r"^.+uid=([0-9]*)")) as c_uid,
  REGEXP_EXTRACT(httpRequest.requestUrl, r"^.+uid=([0-9]*)") as uid
FROM
  `eam-poc-pixel.gcs_pixel_tracking_analytics.requests_*`
GROUP BY uid
ORDER BY c_uid DESC
LIMIT 5


SELECT
  DATE(timestamp) day,
  product,
  count(product) c_prod
FROM
  `eam-poc-pixel.gcs_pixel_tracking_analytics.requests_*`
CROSS JOIN UNNEST(SPLIT(REGEXP_EXTRACT(httpRequest.requestUrl, r"^.+pr=(.*)"), ";")) as product
GROUP By product, day
ORDER by c_prod desc

http://dmitriilin.com/send-google-analytics-hits-to-3rd-party-services-using-google-tag-manager/

https://stackoverflow.com/questions/33872244/sendhittask-not-working-with-hits-sent-via-gtm-universal-analytics-tag-template
To apply your sendHitTask operation on the tags created with GTM's tag templates, you will need to rename the tracker these templates use. You can find this setting under Advanced Configuration > Set Tracker Name. Just leave the field blank (make sure you've checked the checkbox).


#standardSQL
CREATE TEMPORARY FUNCTION
  URL_DECODE(enc STRING)
  RETURNS STRING
  LANGUAGE js AS """
  try {
    return decodeURIComponent(enc);;
  } catch (e) { return null }
  return null;
""";

SELECT
  COUNT(REGEXP_EXTRACT(httpRequest.requestUrl, r"^.+cid=([^&]*)")) AS page_views,
  REGEXP_EXTRACT(httpRequest.requestUrl, r"^.+cid=([^&]*)") AS uid,
  URL_DECODE(REGEXP_EXTRACT(httpRequest.requestUrl, r"^.+dl=([^&]*)")) AS page_path,
  REGEXP_EXTRACT(httpRequest.requestUrl, r"^.+gtm=([^&]*)") AS gtm,
  REGEXP_EXTRACT(httpRequest.requestUrl, r"&t=([^&]*)") AS hit_type,
  httpRequest.referer AS referer
  --CROSS JOIN UNNEST(SPLIT(REGEXP_EXTRACT(httpRequest.requestUrl, r"^.+pr=(.*)"), ";")) as product
FROM
  `eam-poc-pixel.gcs_pixel_tracking_analytics.requests_*`
GROUP BY
  uid,
  page_path,
  hit_type,
  gtm,
  referer
ORDER BY
  page_views DESC
LIMIT
  5
  --select * from `eam-poc-pixel.gcs_pixel_tracking_analytics.requests_*`


# GTM

  <script>
  var bqArray = {};

    bqArray["pageURL"] = document.location.pathname;
    bqArray["Referrer"] = document.referrer;
    bqArray["hostname"] = document.location.hostname;
    //jQuery.post("https://eam-ga-api.appspot.com/bq-streamer", {"bq":JSON.stringify(bqArray)});
    jQuery.get("http://35.227.246.2/pixel.png", {"eam_id":"eam", "bq":JSON.stringify(bqArray)});
  </script>

# STACK

  resource.type="http_load_balancer"
resource.labels.forwarding_rule_name="eam-pixel-tracking-forwarding-rule"
resource.labels.url_map_name="eam-pixel-tracking"
httpRequest.requestUrl:"eam_id=eam"
("resource.type" OR
"=" OR
"http_load_balancer" OR
"AND" OR
"resource.labels.url_map_name" OR
"eam-pixel-tracking")

# APPSCRIPT
https://shinesolutions.com/2017/11/01/scheduling-bigquery-jobs-using-google-apps-script/


bq mk eam_processed

function runQuery() {
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
    "query": "CREATE TEMPORARY FUNCTION  URL_DECODE(enc STRING)  RETURNS STRING  LANGUAGE js AS '''  try {    return decodeURIComponent(enc);;  } catch (e) { return null }  return null;''';SELECT  JSON_EXTRACT_SCALAR(data,'$.hostname') hostname,  JSON_EXTRACT_SCALAR(data,'$.pageURL') page_path,  JSON_EXTRACT_SCALAR(data,'$.Referrer') referrer FROM (  SELECT    URL_DECODE(REGEXP_EXTRACT(httpRequest.requestUrl, r'&bq=([^&]*)')) AS data  FROM    `eam-poc-pixel.gcs_pixel_tracking_analytics_eam.requests_*` )"
    }
  };

  var job = {
    "configuration": configuration
  };

  var jobResult = BigQuery.Jobs.insert(job, "eam-poc-pixel");
  Logger.log(jobResult);
}
