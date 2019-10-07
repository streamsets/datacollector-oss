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

entityName = ''
offset = 0
cur_batch = sdc.createBatch()

hasNext = True
while hasNext:

    offset = offset + 1
    event = sdc.createEvent("event-type", 1)
    cur_batch.addEvent(event)

    # if the batch is full, process it and start a new one
    if cur_batch.eventCount() >= sdc.batchSize:

        # blocks until all records are written to all destinations
        # (or failure) and updates offset
        # in accordance with delivery guarantee
        cur_batch.process(entityName, str(offset))
        cur_batch = sdc.createBatch()
        # if the pipeline has been stopped, we should end the script
        if sdc.isStopped():
            hasNext = False

