
# coding: utf-8

# In[1]:


from flask import Flask
from googleapiclient.discovery import build
#from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import httplib2
#from urllib2 import HTTPError
#from slackclient import SlackClient
import json
import time
import datetime
from pprint import pprint
#from google.cloud import bigquery
#from google.cloud import logging
import uuid
import logging

logger = logging.getLogger(__name__)

logger.info('init')

# In[2]:


def _log(msg):
    #logging_client = logging.Client()
    #logger = logging_client.logger('ga-realtime-stream')
    msg = 'ga-realtime-stream: ' + msg
    logger.info(msg)
    #logger.log_text(msg, severity='INFO')


# In[3]:


def get_service(api_name, api_version, scope, key_file_location):
    credentials = ServiceAccountCredentials.from_json_keyfile_name(key_file_location, scope)
    http = credentials.authorize(httplib2.Http())
    service = build(api_name, api_version, http=http)

    return service


def get_first_profile_id(service):
    accounts = service.management().accounts().list().execute()

    if accounts.get('items'):
        account = accounts.get('items')[0].get('id')
        properties = service.management().webproperties().list(accountId=account).execute()

        if properties.get('items'):
            property = properties.get('items')[0].get('id')
            profiles = service.management().profiles().list(accountId=account, webPropertyId=property).execute()

        if profiles.get('items'):
            return profiles.get('items')[0].get('id')

    return None


def get_results(service, profile_id, metrics, dimensions, sort):
    try:
        return service.data().realtime().get(
            ids='ga:' + profile_id,
            metrics=metrics,
            dimensions=dimensions,
            sort=sort
        ).execute()

    except TypeError, error:
        # Handle errors in constructing a query.
        print ('There was an error in constructing your query : %s' % error)

    except HTTPError, error:
        # Handle API errors.
        print ('Arg, there was an API error : %s : %s' %
               (error.resp.status, error._get_reason()))


def get_detailed_totals(results):
    output = []

    totals = results.get('totalsForAllResults')
    for metric_name, metric_total in totals.iteritems():
        output.append(metric_total)

    if results.get('rows', []):
        detailed = ''
        for row in results.get('rows')[0:10]:
            if row[0] == '(not set)':
                detail = ''
            else:
                detail = row[0]
            detailed += '*' + row[2] + '* ' + row[1] + detail + '\n'

        output.append(detailed)

    return output


# In[24]:



def query_rt():
    response = ''
    scope = ['https://www.googleapis.com/auth/analytics.readonly']
    key_file_location = 'bankinterconsumerfinance_ga-api-6c584fc0cf81.json'

    service = get_service('analytics', 'v3', scope, key_file_location)
    profile = get_first_profile_id(service)

    metrics = 'rt:activeUsers'
    dimensions = 'rt:minutesAgo'
    sort = '-rt:activeUsers'

    results = get_results(service, profile, metrics=metrics, dimensions=dimensions, sort=sort)
    #print(results)
    totals = results.get('totalsForAllResults')
    #for metric_name, metric_total in totals.iteritems():
    #    print(metric_total)
    #print(totals.keys()[0])
    #print(totals['rt:activeUsers'])
    ds = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    #print(ds)
    totals['ds'] = ds
    totals['y'] = totals.pop('rt:activeUsers')
    return totals



# In[25]:


rt_res = query_rt()

_log(str(rt_res))
# In[26]:


#[{'y': '10000', 'ds': '2017-11-17 10:17:01'}]
#print rt_res


# In[27]:


#type(rt_res)


# In[28]:


#[rt_res]


# In[29]:


def stream_data():
    from bigquery import get_client
    json_key = 'eam-poc-realtime-5a6f52eaaec6.json'
    client = get_client(json_key_file=json_key, readonly=False)
    rt_res = query_rt()
    _log(str(rt_res))
    inserted = client.push_rows('streaming', 'active_users', [rt_res], insert_id_key=str(uuid.uuid4()))
    return str(inserted)


# # Submit an async query.
# job_id, _results = client.query('SELECT * FROM poc.streaming LIMIT 1000')
#
# # Check if the query has finished running.
# complete, row_count = client.check_job(job_id)
#
# # Retrieve the results.
# results = client.get_query_rows(job_id)

# rows =  [
#     {'ds': '2017-11-17 10:17:01', 'y': '10000'}
# ]
# type(rows); print rows

# In[14]:


#[rt_res]


# In[23]:


#stream_data()


# In[ ]:


app = Flask(__name__)


# In[ ]:


@app.route('/')
def stream():
    return stream_data()
    #return 'streamed!'


# In[ ]:


@app.errorhandler(500)
def server_error(e):
    #logging.exception('An error occurred during a request.')
    msg = 'An error occurred during a request.'
    print(msg)
    _log(msg)
    return """
    An internal error occurred: <pre>{}</pre>
    See logs for full stacktrace.
    """.format(e), 500
