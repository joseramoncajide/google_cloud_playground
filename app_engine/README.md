# Google App Engine mail handler app

Esta app monitoriza todo los buzones de correo del tipo *@*.appspotmail.com. Cuando encuentra un mail con un CSV lo guarda en Google Cloud Storage.

## Intalación

**Python:**

Descargar en instalar python 2.7.x

    * pip install virtualenv
    * virtualenv env
    * source env/bin/activate

**Google Cloud SDK**

    curl https://sdk.cloud.google.com | bash
    exec -l $SHELL
    source ~/.bash_profile
    gcloud init

    gcloud components update &&
    gcloud components install beta


**App**

    git clone https://github.com/elartedemedir/ventura24_google_cloud
    pip install -t lib -r requirements.txt
    gcloud app deploy app.yaml
