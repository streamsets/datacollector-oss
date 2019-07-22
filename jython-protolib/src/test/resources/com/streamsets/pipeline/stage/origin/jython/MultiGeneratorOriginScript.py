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
# Sample Jython code - Multithreaded Data Generator


sdc.importLock()
from threading import Thread
sdc.importUnlock()

if sdc.userParams.containsKey('recordPrefix'):
    prefix = sdc.userParams.get('recordPrefix')
else:
    prefix = ''

def produce(entityName, entityOffset):
    offset = int(entityOffset)
            
    cur_batch = sdc.createBatch()
    record = sdc.createRecord('generated data')
    hasNext = True
    while hasNext:
        try:
            offset = offset + 1
            record = sdc.createRecord('generated data')
            value = prefix + entityName + ':' + str(offset)
            record.value = value
            cur_batch.add(record)

            # if the batch is full, process it and start a new one
            if cur_batch.size() >= sdc.batchSize:
                # blocks until all records are written to all destinations
                # (or failure) and updates offset
                # in accordance with delivery guarantee
                cur_batch.process(entityName, str(offset))
                cur_batch = sdc.createBatch()
                if sdc.isStopped():
                    hasNext = False

        except Exception as e:
            sdc.error.write(record, str(e))
            hasNext = False

  
threads = []
for t in range(sdc.numThreads):
    if str(t) in sdc.lastOffsets:
        offset = sdc.lastOffsets[str(t)]
    else:
        offset = '0'
    thread = Thread(
        target=produce,
        args=(
            str(t),
            str(offset)
        ))
    threads.append(thread)
    thread.start()

# wait for every thread to finish
for thread in threads:
    thread.join()

