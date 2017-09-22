#!/usr/bin/env python

# Copyright 2017 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START sample]
"""A sample app that uses GCS client to operate on bucket and file."""

# [START imports]
import os
import logging

from google.appengine.api import app_identity
from google.appengine.ext import vendor

vendor.add('lib')

import webapp2
import cloudstorage as gcs


if os.getenv('SERVER_SOFTWARE', '').startswith('Google App Engine/'):
  # Production
  logging.info("Environment: PROD")
else:
  # Local development server
  logging.info("Environment: DEV")


class MainPage(webapp2.RequestHandler):
    """Main page for GCS demo application."""

# [START get_default_bucket]
    def get(self):
        bucket_name = os.environ.get(
            'BUCKET_NAME', app_identity.get_default_gcs_bucket_name())

        self.response.headers['Content-Type'] = 'text/plain'
        self.response.write('\n\nThe demo ran successfully!\n')
        self.response.write('Demo GCS Application running from Version: '
                            + os.environ['CURRENT_VERSION_ID'] + '\n')
        self.response.write('Using bucket name: ' + bucket_name + '\n\n')

        filename = '/ventura24_incoming_files/test2.csv'

        write_retry_params = gcs.RetryParams(backoff_factor=1.1)
        with gcs.open(
            filename, 'w', content_type='text/plain', options={
                'x-goog-meta-foo': 'foo', 'x-goog-meta-bar': 'bar'},
                retry_params=write_retry_params) as cloudstorage_file:
                    cloudstorage_file.write('abcde\n')
                    cloudstorage_file.write('f'*1024*4 + '\n')
        #self.tmp_filenames_to_clean_up.append(filename)




app = webapp2.WSGIApplication(
    [('/', MainPage)], debug=True)
# [END sample]
