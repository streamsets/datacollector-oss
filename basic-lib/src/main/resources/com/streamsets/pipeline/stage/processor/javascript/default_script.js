/**
 * Available constants:
 *   They are to assign a type to a field with a value null.
 *   NULL_BOOLEAN, NULL_CHAR, NULL_BYTE, NULL_SHORT, NULL_INTEGER, NULL_LONG
 *   NULL_FLOATNULL_DOUBLE, NULL_DATE, NULL_DATETIME, NULL_TIME, NULL_DECIMAL
 *   NULL_BYTE_ARRAY, NULL_STRING, NULL_LIST, NULL_MAP
 *
 * Available Objects:
 *
 *  records: an array of records to process, depending on the JavaScript processor
 *           processing mode it may have 1 record or all the records in the batch.
 *
 *  state: a dict that is preserved between invocations of this script.
 *        Useful for caching bits of data e.g. counters.
 *
 *  log.<loglevel>(msg, obj...): use instead of print to send log messages to the log4j log instead of stdout.
 *                               loglevel is any log4j level: e.g. info, error, warn, trace.
 *
 *  output.write(record): writes a record to processor output
 *
 *  error.write(record, message): sends a record to error
 *
 *  sdcFunctions.getFieldNull(Record, 'field path'): Receive a constant defined above
 *                            to check if the field is typed field with value null
 *  sdcFunctions.createRecord(String recordId): Creates a new record.
 *                            Pass a recordId to uniquely identify the record and include enough information to track down the record source.
 *  sdcFunctions.createMap(boolean listMap): Create a map for use as a field in a record.
 *                            Pass true to this function to create a list map (ordered map)
 *
 *  sdcFunctions.createEvent(String type, int version): Creates a new event.
 *                            Create new empty event with standard headers.
 *  sdcFunctions.toEvent(Record): Send event to event stream
 *                            Only events created with sdcFunctions.createEvent are supported.
 *  sdcFunctions.isPreview(): Determine if pipeline is in preview mode.
 *
 * Available Record Header Variables:n *
 *  record.attributes: a map of record header attributes.
 *
 *  record.<header name>: get the value of 'header name'.
 */

// Sample JavaScript code
for(var i = 0; i < records.length; i++) {
    try {
        // Change record root field value to a STRING value
        //records[i].value = 'Hello ' + i;


        // Change record root field value to a MAP value and create an entry
        //records[i].value = { V : 'Hello' };

        // Access a MAP entry
        //records[i].value.X = records[i].value['V'] + ' World';

        // Modify a MAP entry
        //records[i].value.V = 5;

        // Create an ARRAY entry
        //records[i].value.A = ['Element 1', 'Element 2'];

        // Access a Array entry
        //records[i].value.B = records[i].value['A'][0];

        // Modify an existing ARRAY entry
        //records[i].value.A[0] = 100;

        // Assign a integer type to a field and value null
        // records[i].value.null_int = NULL_INTEGER

        // Check if the field is NULL_INTEGER. If so, assign a value
        // if(sdcFunctions.getFieldNull(records[i], '/null_int') == NULL_INTEGER)
        //    records[i].value.null_int = 123

        // Direct access to the underlying Data Collector Record. Use for read-only operations.
        // fieldAttr = records[i].sdcRecord.get('/value').getAttribute('attr')

        // Create a new record with map field
        // var newRecord = sdcFunctions.createRecord(records[i].sourceId + ':newRecordId');
        // newRecord.value = {'field1' : 'val1', 'field2' : 'val2'};
        // output.write(newRecord);
        // Create a new map and add it to the original record
        // var newMap = sdcFunctions.createMap(true);
        // newMap['key'] = 'value';
        // records[i].value['b'] = newMap;

        //Applies if the source uses WHOLE_FILE as data format
        //var input_stream = record.value['fileRef'].getInputStream();
        //try {
        //input_stream.read(); //Process the input stream
        //} finally{
        //input_stream.close()
        //}

        // Modify a header attribute entry
        // records[i].attributes['name'] = records[i].attributes['first_name'] + ' ' + records[i].attributes['last_name']    //

        // Get a record header with field names ex. get sourceId and errorCode
        // var sourceId = records[i].sourceId
        // var errorCode = ''
        // if(records[i].errorCode) {
        //     errorCode = records[i].errorCode
        // }

        // Write record to processor output
        output.write(records[i]);
    } catch (e) {
        // Send record to error
        error.write(records[i], e);
    }
}
