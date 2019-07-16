package com.streamsets.pipeline.stage.origin.groovy
// single threaded - no entityName because we need only one offset
entityName = ''
offset = sdc.lastOffsets.get(entityName, '0').toInteger()

prefix = sdc.userParams.get('recordPrefix', '')

cur_batch = sdc.createBatch()
record = sdc.createRecord('generated data')

hasNext = true
while (hasNext) {
    try {
        offset = offset + 1
        record = sdcFunctions.createRecord('generated data')
        value = prefix + entityName + ':' + toString(offset)
        record.value = value
        cur_batch.add(record)

        // if the batch is full, process it and start a new one
        if (cur_batch.size() >= sdc.batchSize) {
            // blocks until all records are written to all destinations
            // (or failure) and updates offset
            // in accordance with delivery guarantee
            cur_batch.process(entityName, toString(offset))
            cur_batch = sdc.createBatch()
            if (sdc.isStopped()) {
                hasNext = false
            }
        }
    } catch (Exception e) {
        sdc.error.write(record, toString(e))
        hasNext = false
    }
}
