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

try:
    sdc.importLock()
    import datetime
finally:
    sdc.importUnlock()

# single threaded - no entityName because we need only one offset
entityName = ''

# get previously committed offset or use 0
if sdc.lastOffsets.containsKey(entityName):
    offset = int(sdc.lastOffsets.get(entityName))
else:
    offset = 0

# get record prefix from user parameters or default to empty string
if sdc.userParams.containsKey('recordPrefix'):
    prefix = sdc.userParams.get('recordPrefix')
else:
    prefix = ''

cur_batch = sdc.createBatch()

record = sdc.createRecord('record created ' + str(datetime.datetime.now()))
hasNext = True
while hasNext:

    offset = offset + 1
    record = sdc.createRecord('record created ' + str(datetime.datetime.now()))
    value = prefix + entityName + ':' + str(offset)
    record.value = value
    cur_batch.add(record)
    cur_batch.addError(record, "")

    # if the batch is full, process it and start a new one
    if cur_batch.size() >= sdc.batchSize:

        # blocks until all records are written to all destinations
        # (or failure) and updates offset
        # in accordance with delivery guarantee
        cur_batch.process(entityName, str(offset))
        cur_batch = sdc.createBatch()
        # if the pipeline has been stopped, we should end the script
        if sdc.isStopped():
            hasNext = False

