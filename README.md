# google_cloud_playground

curl https://sdk.cloud.google.com | bash
exec -l $SHELL
source ~/.bash_profile
gcloud init

gcloud components update &&
gcloud components install beta


gsutil mb -c regional -l europe-west1 gs://project_cloud_functions
gsutil mb -c regional -l europe-west1 gs://project_inbox

mkdir ~/gcf_gcs
cd ~/gcf_gcs

gcloud beta functions deploy helloGCS --stage-bucket project_cloud_functions --trigger-bucket project_inbox
