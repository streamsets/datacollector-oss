# Copyright 2019 StreamSets Inc.
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
# 
# Sample Jython code - HTTP Server
#

sdc.importLock()
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
from SocketServer import ThreadingMixIn
sdc.importUnlock()


class Handler(BaseHTTPRequestHandler):
    
    def do_GET(self):
        print 'serving request'
        record = sdc.createRecord('http get')
        try:
            content_len = int(self.headers.getheader('content-length', 0))
            post_body = self.rfile.read(content_len)
            record.value = post_body
            cur_batch = sdc.createBatch()
            cur_batch.add(record)
            # we use this flavor of processBatch because HTTP Server has no offset
            # it also blocks until all records are written to all destinations (or failure)
            if sdc.processBatch(cur_batch):
                # success
                self.send_response(200, sdc.getSourceResponseRecords())
            else:
                # failure
                self.send_response(500)
            self.end_headers()

        except Exception as e:
            sdc.error.write(record, str(e))

        return


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle requests in a separate thread."""


server = ThreadedHTTPServer(('localhost', 8080), Handler)
log.info('Starting HTTP server')
server.serve_forever()

