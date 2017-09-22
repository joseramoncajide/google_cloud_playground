# Google App Engine mail handler app

Esta app monitoriza todo los buzones de correo del tipo *@*.appspotmail.com. Cuando encuentra un mail con un CSV lo guarda en Google Cloud Storage.

1. Clonar repo


    git clone https://github.com/elartedemedir/ventura24_google_cloud


2. Instalar librerías en local


    pip install -t lib -r requirements.txt


3. Deploy:


    gcloud app deploy app.yaml
