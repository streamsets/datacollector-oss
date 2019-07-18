/* Sample Groovy code - Data Generator
 *
 * Available constants:
 *   sdc.lastOffsets: Dictionary representing the (string) offset previously reached
 *       for each (string) entityName. Offsets are committed by the pipeline.
 *   sdc.batchSize: Requested record batch size (from UI)
 *   sdc.nThreads: Number of threads to run (from UI)
 *   sdc.userParams: Dictionary of user-specified keys and values (from UI)
 *
 *   These can be used to assign a type to a field with a value null:
 *       sdc.NULL_BOOLEAN, sdc.NULL_CHAR, sdc.NULL_BYTE, sdc.NULL_SHORT, sdc.NULL_INTEGER,
 *       sdc.NULL_LONG, sdc.NULL_FLOAT, sdc.NULL_DOUBLE, sdc.NULL_DATE, sdc.NULL_DATETIME,
 *       sdc.NULL_TIME, sdc.NULL_DECIMAL, sdc.NULL_BYTE_ARRAY, sdc.NULL_STRING, sdc.NULL_LIST,
 *       sdc.NULL_MAP
 *
 * Available functions:
 *   sdc.createBatch(): return a new Batch
 *   sdc.createRecord(): return a new Record
 *   Batch.add(record): append a record to the Batch
 *   Batch.add(record[]): append a list of records to the Batch
 *   Batch.size(): return the number of records in the batch
 *   Batch.process(entityName, entityOffSet): process the batch through the rest of
 *       the pipeline and commit the offset in accordance with the pipeline's
 *       delivery guarantee
 *   Batch.getSourceResponseRecords(): after a batch is processed, retrieve any
 *       response records returned by downstream stages
 *   sdc.isStopped(): returns whether or not the pipeline has been stopped
 *   sdc.importLock() and sdc.importUnlock(): Acquire or release a systemwide lock which can be
 *       used to circumvent a known bug with the thread-safety of Jython imports (https://bugs.jython.org/issue2642)
 *
 * Available Objects:
 *  sdc.log.<loglevel>(msg, obj...): use instead of print to send log messages to the log4j log instead of stdout.
 *                               loglevel is any log4j level: e.g. info, error, warn, trace.
 *  sdc.error.write(record, message): sends a record to error
 *  sdc.getFieldNull(Record, 'field path'): Receive a constant defined above
 *                                  to check if the field is typed field with value null
 *  sdc.createRecord(String recordId): Creates a new record.
 *                            Pass a recordId to uniquely identify the record and include enough information to track down the record source.
 *  sdc.createMap(boolean listMap): Create a map for use as a field in a record.
 *                            Pass True to this function to create a list map (ordered map)
 *  sdc.createEvent(String type, int version): Creates a new event.
 *                            Create new empty event with standard headers.
 *  sdc.toEvent(Record): Send event to event stream
 *                            Only events created with sdcFunctions.createEvent are supported.
 *  sdc.isPreview(): Determine if pipeline is in preview mode.
 *
 * Available Record Header Variables:
 *  record.attributes: a map of record header attributes.
 *  record.<header name>: get the value of 'header name'.
 */

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
        record = sdc.createRecord('generated data')
        value = prefix + entityName + ':' + offset.toString()
        record.value = value
        cur_batch.add(record)

        // if the batch is full, process it and start a new one
        if (cur_batch.size() >= sdc.batchSize) {
            // blocks until all records are written to all destinations
            // (or failure) and updates offset
            // in accordance with delivery guarantee
            cur_batch.process(entityName, offset.toString())
            cur_batch = sdc.createBatch()
            if (sdc.isStopped()) {
                hasNext = false
            }
        }
    } catch (Exception e) {
        sdc.error.write(record, e.toString())
        hasNext = false
    }
}
