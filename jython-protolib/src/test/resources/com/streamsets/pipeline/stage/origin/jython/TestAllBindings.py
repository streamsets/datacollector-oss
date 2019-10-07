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

# Set up batch and record
batch = sdc.createBatch()
record = sdc.createRecord('bindings test')
record.value = {}

# Test constants
record.value['batchSize'] = sdc.batchSize
record.value['numThreads'] = sdc.numThreads
record.value['lastOffsets'] = sdc.lastOffsets
record.value['params'] = sdc.userParams

# Test sdcFunctions
record.value['isStopped()'] = sdc.isStopped()
record.value['pipelineParameters()'] = sdc.pipelineParameters()
record.value['isPreview()'] = sdc.isPreview()
record.value['createMap()'] = sdc.createMap(False)
record.value['createListMap()'] = sdc.createMap(True)
record.value['getFieldNull()-false'] = sdc.getFieldNull(record, '/isStopped()')
record.value['getFieldNull()-null'] = sdc.getFieldNull(record, '/not-a-real-fieldpath')

records_list = [record]

# Error
batch.addError(record, 'this is only a test')

# Event
eventRecord = sdc.createEvent('new test event', 123)
batch.addEvent(eventRecord)

# Test batch methods
record.value['batch.size()'] = batch.size()
record.value['batch.errorCount()'] = batch.errorCount()
record.value['batch.eventCount()'] = batch.eventCount()

# public void add(ScriptRecord scriptRecord) 
batch.add(record)
# public void add(ScriptRecord[] scriptRecords) 
batch.add(records_list)

# Process the batch and commit new offset
batch.process('newEntityName', 'newEntityOffset')

