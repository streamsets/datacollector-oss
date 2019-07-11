/*
 * Available constants:
 *   They are to assign a type to a field with a value null.
 *   NULL_BOOLEAN, NULL_CHAR, NULL_BYTE, NULL_SHORT, NULL_INTEGER, NULL_LONG
 *   NULL_FLOATNULL_DOUBLE, NULL_DATE, NULL_DATETIME, NULL_TIME, NULL_DECIMAL
 *   NULL_BYTE_ARRAY, NULL_STRING, NULL_LIST, NULL_MAP
 *
 * Available objects:
 *   records: A collection of Records to process. Depending on the processing mode
 *            it may have 1 record or all the records in the batch (default).
 *
 *   state: A Map<String, Object> that is preserved between invocations of this script.
 *          Useful for caching bits of data, e.g. counters.
 *
 *   log.<level>(msg, obj...): Use instead of println to send log messages to the log4j log
 *                             instead of stdout.
 *                             loglevel is any log4j level: e.g. info, warn, error, trace.
 *   output.write(Record): Writes a record to the processor output.
 *
 *   error.write(Record, message): Writes a record to the error pipeline.
 *
 *   sdcFunctions.getFieldNull(Record, 'field path'): Receive a constant defined above
 *                          to check if the field is typed field with value null
 *
 *   sdcFunctions.createRecord(String recordId): Creates a new record.
 *                          Pass a recordId to uniquely identify the record and include enough information to track down the record source.
 *   sdcFunctions.createMap(boolean listMap): Create a map for use as a field in a record.
 *                          Pass true to this function to create a list map (ordered map)
 *
 *   sdcFunctions.createEvent(String type, int version): Creates a new event.
 *                          Create new empty event with standard headers.
 *   sdcFunctions.toEvent(Record): Send event to event stream
 *                          Only events created with sdcFunctions.createEvent are supported.
 *   sdcFunctions.isPreview(): Determine if pipeline is in preview mode.
 *   sdcFunctions.pipelineParameters(): Map with pipeline runtime parameters.
 *
 * Available Record Header Variables:
 *   record.attributes: a map of record header attributes.
 *
 *   record.<header name>: get the value of 'header name'.
 */

// Sample Groovy code
for (record in records) {
    try {
        // Change record root field value to a String value.
        // record.value = "Hello"

        // Change record root field value to a map value and create an entry
        // record.value = [firstName:'John', lastName:'Doe', age:25]

        // Access a map entry
        // record.value['fullName'] = record.value['firstName'] + ' ' + record.value['lastName']

        // Create a list entry
        // record.value['myList'] = [1, 2, 3, 4]

        // Modify an existing list entry
        // record.value['myList'][0] = 5

        // Assign a integer type to a field and value null
        // record.value['null_int'] = NULL_INTEGER

        // Check if the field is NULL_INTEGER. If so, assign a value
        // if(sdcFunctions.getFieldNull(record, '/null_int') == NULL_INTEGER)
        //    record.value['null_int'] = 123

        // Direct access to the underlying Data Collector Record. Use for read-only operations.
        // fieldAttr = record.sdcRecord.get('/value').getAttribute('attr')

        // Create a new record with map field
        // newRecord = sdcFunctions.createRecord(record.sourceId + ':newRecordId')
        // newRecord.value = ['field1':'val1', 'field2' : 'val2']
        // newMap = sdcFunctions.createMap(true)
        // newMap['field'] = 'val'
        // newRecord.value['field2'] =  newMap
        // output.write(newRecord)

        //Applies if the source uses WHOLE_FILE as data format
        //input_stream = record.value['fileRef'].getInputStream();
        //try {
        //input_stream.read(); //Process the input stream
        //} finally {
        //input_stream.close();
        //}

        // Modify a record header attribute entry
        //record.attributes['name'] = record.attributes['first_name'] + ' ' + record.attributes['last_name']

        // Get a record header with field names ex. get sourceId and errorCode
        //String sourceId = record.sourceId
        //String errorCode = ''
        //if(record.errorCode) {
        //    errorCode = record.errorCode
        //}

        // Write a record to the processor output
        output.write(record)
    } catch (e) {
        // Write a record to the error pipeline
        log.error(e.toString(), e)
        error.write(record, e.toString())
    }
}

