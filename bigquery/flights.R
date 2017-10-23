# https://blog.h2o.ai/2013/04/predicting-airline-data-generalized-linear-modeli/

bdown=function(url, file){
  library('RCurl')
  f = CFILE(file, mode="wb")
  a = curlPerform(url = url, writedata = f@ref, noprogress=FALSE)
  close(f)
  return(a)
}

## ...and now just give remote and local paths     
ret = bdown("https://s3.amazonaws.com/h2o-airlines-unpacked/allyears.csv", "/users/jose/tmp/allyears.csv")

curl https://s3.amazonaws.com/h2o-airlines-unpacked/allyears2k.csv  > /users/jose/tmp/allyears2k.csv 
head -2 /users/jose/tmp/allyears2k.csv > sample.csv
gsutil -m cp sample.csv  gs://kschool
bq load --autodetect --source_format=CSV --skip_leading_rows=1 flights.flights gs://kschool/sample.csv
bq show --format=prettyjson flights.flights | jq '.schema.fields' > schema.json

gsutil -m cp /users/jose/tmp/allyears.csv gs://kschool
bq load --null_marker="NA" --autodetect  --source_format=CSV --skip_leading_rows=1 flights.flights gs://kschool/allyears.csv

select * from [cpb100-162913:flights.flights] limit 100
SELECT * FROM `cpb100-162913.flights.flights` LIMIT 100