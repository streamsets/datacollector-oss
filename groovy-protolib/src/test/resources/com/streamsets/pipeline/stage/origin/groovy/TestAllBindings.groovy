/* Copyright 2019 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Set up batch and record
batch = sdc.createBatch()
record = sdc.createRecord('bindings test')
record.value = sdc.createMap(false)

// Test constants
record.value['batchSize'] = sdc.batchSize
record.value['numThreads'] = sdc.numThreads
record.value['lastOffsets'] = sdc.lastOffsets
record.value['params'] = sdc.userParams

// Test functions
record.value['isStopped()'] = sdc.isStopped()
record.value['pipelineParameters()'] = sdc.pipelineParameters()
record.value['isPreview()'] = sdc.isPreview()
record.value['createMap()'] = sdc.createMap(false)
record.value['createListMap()'] = sdc.createMap(true)
record.value['getFieldNull()-false'] = sdc.getFieldNull(record, '/isStopped()')
record.value['getFieldNull()-null'] = sdc.getFieldNull(record, '/not-a-real-fieldpath')

// Test batch methods
record.value['actualBatchSize'] = batch.size()

records_list = [record]

// public void add(ScriptRecord scriptRecord)
batch.add(record)
// public void add(ScriptRecord[] scriptRecords)
batch.add(records_list)

// Event
eventRecord = sdc.createEvent('new test event', 123)
sdc.toEvent(eventRecord)

// Process the batch and commit new offset
batch.process('newEntityName', 'newEntityOffset')
