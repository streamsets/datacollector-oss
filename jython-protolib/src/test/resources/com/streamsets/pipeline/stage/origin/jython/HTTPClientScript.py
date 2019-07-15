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
# Sample Jython code - HTTP Client Script
#
# New available constants:
#   lastOffsets: Dictionary representing the (string) offset previously reached
#       for each (string) entityName. Offsets are managed by the pipeline.
#   batchSize: Requested record batch size (from UI)
#   nThreads: Number of threads to run (from UI)
#   params: Dictionary of user-specified keys and values (from UI)
#
# New available functions: 
#   sdcFunctions.createBatch(): return a new Batch
#   sdcFunctions.isStopped(): returns whether or not the pipeline has been stopped
#   Batch.add(record): append record to Batch
#   Batch.size(): returns number of records in the batch
#   Batch.process(entityName, entityOffSet): process the batch through the rest of
#       the pipeline and commit the offset in accordance with the pipeline's
#       delivery guarantee
#

sdc.importLock()
from threading import Thread
import sys
sys.path.append('/usr/lib/python3/dist-packages')
import requests
sdc.importUnlock()

if 'baseURL' not in sdc.userParams:
    raise Exception('Missing "baseURL" from required user-defined parameters')
baseURL = sdc.userParams['baseURL']

def fetch(baseURL, entityName, entityOffset):

    cur_batch = sdc.createBatch()
    partition = entityName
    offset = int(entityOffset)

    hasNext = True
    while hasNext:

        try:
            offset = offset + 1
            url = '{}/{}?page={}'.format(baseURL, partition, offset)
            response = requests.get(url)
            if response.status_code != 200:
                raise Exception('Got status code {}'.format(response.status_code))
            record = sdc.createRecord('http response')
            record.value = response
            cur_batch.add(record)

            # if the batch is full, process it and start a new one
            if cur_batch.size() >= batchSize:
                # blocks until all records are written to all destinations (or failure)
                # and updates offset in accordance with delivery guarantee
                sdc.processBatch(batch, entityName, offset)
                cur_batch = sdc.createBatch()

        except Exception as e:
            sdc.error.write(record, str(e))
            hasNext = False
        
    
    # process the final batch if its not empty
    if cur_batch_size > 0:
        sdc.processBatch(batch, entityName, newOffset)

    return


threads = []
for t in range(sdc.numThreads):
    offset = sdc.lastOffsets.get(str(t), 0)
    thread = Thread(
        target=fetch,
        args=(
            str(t),
            str(offset)
        ))
    thread.start()

# wait for every thread to finish
for thread in threads:
    thread.join()

