virtualenv venv
source venv/bin/activate
(venv) $ venv/bin/pip install --upgrade google-cloud
(venv) $ venv/bin/python -c 'import google.cloud.storage'
(venv) $ deactivate
$ rm -fr venv/

venv/bin/pip install --upgrade google-api-python-client
venv/bin/pip install --upgrade Flask


virtualenv venv
source venv/bin/activate

pip install ipykernel
python -m ipykernel install --user --name venv --display-name "Python VENV"

venv/bin/pip install --upgrade google-cloud
pip install --upgrade Flask
pip install --upgrade google-api-python-client


https://stackoverflow.com/questions/43085047/how-to-import-bigquery-in-appengine-for-python


pip install -t lib/ httplib2
pip install --upgrade -t lib httplib2 -r requirements.txt


pip install requests[security]

pip install --upgrade -t lib google-cloud-bigquery==0.25.0

python -c "from google.cloud import logging"


pip install --upgrade -t lib -r requirements.txt
pip install -r requirements.txt -t lib

pip install --upgrade -t lib Flask -r requirements.txt
pip install --upgrade -t lib google-api-python-client -r requirements.txt
pip install --upgrade google-api-python-client
pip install --upgrade -t lib google-api-python-client

pip install --upgrade -t lib bigquery-python
