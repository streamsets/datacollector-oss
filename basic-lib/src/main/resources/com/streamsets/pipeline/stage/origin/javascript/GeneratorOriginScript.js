// single threaded - no entityName because we need only one offset
var entityName = '';
var offset = '0';
if (sdc.lastOffsets.has('entityName')) {
    offset = parseInt(sdc.lastOffsets.get('entityName'));
}

var prefix = '';
if (sdc.userParams.has('recordPrefix')) {
    prefix = sdc.lastOffsets.get('entityName');
}

var cur_batch = sdc.createBatch();

var record = sdc.createRecord('generated data');
var hasNext = true;
while (hasNext) {
    try {
        offset++;
        record = sdc.createRecord('generated data');
        var value = prefix + entityName + ':' + offset.toString();
        record.value = value;
        cur_batch.add(record);

        // if the batch is full, process it and start a new one
        if (cur_batch.size() >= sdc.batchSize) {
            // blocks until all records are written to all destinations
            // (or failure) and updates offset
            // in accordance with delivery guarantee
            cur_batch.process(entityName, offset.toString())
            cur_batch = sdc.createBatch();
            if (sdc.isStopped) {
                hasNext = false;
            }
        }
    } catch (e) {
        sdc.error.write(record, e.toString());
        hasNext = false;
    }
}