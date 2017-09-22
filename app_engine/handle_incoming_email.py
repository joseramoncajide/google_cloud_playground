# Copyright 2016 Google Inc. All rights reserved.
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

# [START log_sender_handler]
import logging
# import os

from google.appengine.ext.webapp.mail_handlers import InboundMailHandler
import webapp2

from google.appengine.ext import vendor

vendor.add('lib')

from google.appengine.api import app_identity
import cloudstorage as gcs



class LogSenderHandler(InboundMailHandler):
    def receive(self, mail_message):
        logging.info("Mensaje de: " + mail_message.sender)

        buf = mail_message.original.get('Subject', 'no subject')
        logging.info("Asunto: %s" % buf)
        #my_file = []
        #my_list = []
        if hasattr(mail_message, 'attachments'):
            file_name = ""
            for filename, filecontents in mail_message.attachments:
                if filename.endswith('.csv'):
                    file_name = filename
                    # file_blob = filecontents.payload
                    # file_blob = file_blob.decode(filecontents.encoding)
                    # my_file.append(file_name)
                    # my_list.append(str(store_file(self, file_name, file_blob)))
                    logging.info("CSV adjunto: " + file_name)
                    # logging.info("Attactment data in blob %s" % filecontents.decode())

                    #bucket_name = os.environ.get(
                    #    'BUCKET_NAME', app_identity.get_default_gcs_bucket_name())

                    # filename = '/' + bucket_name + '/' + file_name

                    filename = "/ventura24_incoming_files/" + file_name

                    write_retry_params = gcs.RetryParams(backoff_factor=1.1)
                    with gcs.open(
                        filename, 'w', content_type='text/plain', options={
                            'x-goog-meta-foo': 'foo', 'x-goog-meta-bar': 'bar'},
                            retry_params=write_retry_params) as cloudstorage_file:
                                cloudstorage_file.write(filecontents.decode())

# [END log_sender_handler]
# [START bodies]
        plaintext_bodies = mail_message.bodies('text/plain')
        html_bodies = mail_message.bodies('text/html')

        for content_type, body in html_bodies:
            decoded_html = body.decode()
            # ...
# [END bodies]
            logging.info("Html body of length %d.", len(decoded_html))
        for content_type, body in plaintext_bodies:
            plaintext = body.decode()
            logging.info("Plain text body of length %d.", len(plaintext))


# [START app]
app = webapp2.WSGIApplication([LogSenderHandler.mapping()], debug=True)
# [END app]
