/* Sample JavaScript code - Data Generator
 *
 * Available constants:
 *   sdc.lastOffsets: Dictionary representing the (string) offset previously reached
 *       for each (string) entityName. Offsets are committed by the pipeline.
 *   sdc.batchSize: Requested record batch size (from UI).
 *   sdc.nThreads: Number of threads to run (from UI).
 *   sdc.userParams: Dictionary of user-specified keys and values (from UI).
 *
 *   These can be used to assign a type to a field with a value null:
 *       sdc.NULL_BOOLEAN, sdc.NULL_CHAR, sdc.NULL_BYTE, sdc.NULL_SHORT, sdc.NULL_INTEGER,
 *       sdc.NULL_LONG, sdc.NULL_FLOAT, sdc.NULL_DOUBLE, sdc.NULL_DATE, sdc.NULL_DATETIME,
 *       sdc.NULL_TIME, sdc.NULL_DECIMAL, sdc.NULL_BYTE_ARRAY, sdc.NULL_STRING, sdc.NULL_LIST,
 *       sdc.NULL_MAP
 *
 * Available functions:
 *   sdc.createBatch(): Return a new batch.
 *   sdc.createRecord(String recordId): Return a new record. Pass a recordId to uniquely identify
 *       the record and include enough information to track down the record source.
 *   sdc.createEvent(String type, int version): Return a new empty event with standard headers.
 *   Batch.add(record): Append a record to the batch.
 *   Batch.add(record[]): Append a list of records to the batch.
 *   Batch.addError(record, msg): Add an error record to the batch with the associated error message.
 *   Batch.addEvent(event): Append an event to the batch.
 *       Only events created with sdc.createEvent() are supported.
 *   Batch.size(): Return the number of records in the batch.
 *   Batch.errorCount(): Return the number of error records in the batch.
 *   Batch.process(entityName, entityOffSet): Process the batch through the rest of
 *       the pipeline and commit the offset in accordance with the pipeline's
 *       delivery guarantee.
 *   Batch.getSourceResponseRecords(): After a batch is processed, retrieve any
 *       response records returned by downstream stages.
 *   sdc.isStopped(): Return whether or not the pipeline has been stopped.
 *   sdc.isPreview(): Return whether or not the pipeline is in preview mode.
 *   sdc.importLock() and sdc.importUnlock(): Acquire or release a systemwide lock which can be
 *       used to circumvent a known bug with the thread-safety of Jython imports. (https://bugs.jython.org/issue2642)
 *   sdc.log.<loglevel>(msg, obj...): Use instead of print to send log messages to the log4j log instead of stdout.
 *       loglevel is any log4j level: e.g. info, error, warn, trace.
 *   sdc.getFieldNull(Record, 'field path'): Receive a constant defined above
 *       to check if the field is a typed field with value null.
 *   sdc.createMap(boolean listMap): Create a map for use as a field in a record.
 *       Pass true to this function to create a list map (ordered map).
 *
 * Available Record Header Variables:
 *   record.attributes: A map of record header attributes.
 *   record.<header name>: Get the value of 'header name'.
 */

// single threaded - no entityName because we need only one offset
var entityName = '';
var offset = 0;
if (sdc.lastOffsets.containsKey(entityName)) {
    offset = parseInt(sdc.lastOffsets.get(entityName));
}

var prefix = '';
if ('recordPrefix' in sdc.userParams) {
    prefix = sdc.lastOffsets['entityName'];
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
            if (sdc.isStopped()) {
                hasNext = false;
            }
        }
    } catch (e) {
        cur_batch.addError(record, e.toString());
        cur_batch.process(entityName, offset.toString());
        hasNext = false;
    }
}

if (cur_batch.size() + cur_batch.errorCount() + cur_batch.eventCount() > 0) {
    cur_batch.process(entityName, offset.toString())
}
