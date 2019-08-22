# Available constants: 
#   They are to assign a type to a field with a value null.
#   sdc.NULL_BOOLEAN, sdc.NULL_CHAR, sdc.NULL_BYTE, sdc.NULL_SHORT, sdc.NULL_INTEGER, sdc.NULL_LONG,
#   sdc.NULL_FLOAT, sdc.NULL_DOUBLE, sdc.NULL_DATE, sdc.NULL_DATETIME, sdc.NULL_TIME, sdc.NULL_DECIMAL,
#   sdc.NULL_BYTE_ARRAY, sdc.NULL_STRING, sdc.NULL_LIST, sdc.NULL_MAP
# 
# Available Objects:
# 
#  sdc.records: an array of records to process, depending on Jython processor
#           processing mode it may have 1 record or all the records in the batch.
#
#  sdc.state: a dict that is preserved between invocations of this script. 
#         Useful for caching bits of data e.g. counters.
#
#  sdc.log.<loglevel>(msg, obj...): use instead of print to send log messages to the log4j log instead of stdout.
#                               loglevel is any log4j level: e.g. info, error, warn, trace.
#
#  sdc.output.write(record): writes a record to processor output
#
#  sdc.error.write(record, message): sends a record to error
#
#  sdc.getFieldNull(Record, 'field path'): Receive a constant defined above 
#                                  to check if the field is typed field with value null
#  sdc.createRecord(String recordId): Creates a new record.
#                            Pass a recordId to uniquely identify the record and include enough information to track down the record source. 
#  sdc.createMap(boolean listMap): Create a map for use as a field in a record.
#                            Pass True to this function to create a list map (ordered map)
#
#  sdc.createEvent(String type, int version): Creates a new event.
#                            Create new empty event with standard headers.
#  sdc.toEvent(Record): Send event to event stream
#                            Only events created with sdcFunctions.createEvent are supported.
#  sdc.isPreview(): Determine if pipeline is in preview mode.
#
#  sdc.userParams: Dictionary of user-specified keys and values (from UI).
#
#
# Available Record Header Variables:
#
#  record.attributes: a map of record header attributes.
#
#  record.<header name>: get the value of 'header name'.
#
# Add additional module search paths:
#   try:
#       sdc.importLock()
#       import sys
#       sys.path.append('/some/other/dir/to/search')
#       import something
#   finally:
#       sdc.importUnlock()
#

# Sample Jython code
for record in sdc.records:
  try:
    # Change record root field value to a STRING value
    # record.value = 'Hello '


    # Change record root field value to a MAP value and create an entry
    # record.value = { 'V' : 'Hello'}

    # Access a MAP entry
    # record.value['X'] = record.value['V'] + ' World'

    # Modify a MAP entry
    # record.value['V'] = 5

    # Create an ARRAY entry
    # record.value['A'] = [ 'Element 1', 'Element 2' ]

    # Access an ARRAY entry
    # record.value['B'] = record.value['A'][0]

    # Modify an existing ARRAY entry
    # record.value['A'][0] = 100

    # Assign a integer type to a field and value null
    # record.value['null_int'] = sdc.NULL_INTEGER 

    # Check if the field is NULL_INTEGER(Both '==' and 'is' work). If so, assign a value 
    # if sdc.getFieldNull(record, '/null_int') == sdc.NULL_INTEGER:
    #    record.value['null_int'] = 123

    # Direct access to the underlying Data Collector Record. Use for read-only operations.
    # fieldAttr = record.sdcRecord.get('/value').getAttribute('attr')  

    # Create a new record with map field 
    # newRecord = sdc.createRecord(record.sourceId + ':newRecordId')
    # newRecord.value = {'field1' : 'val1', 'field2' : 'val2'}
    # sdc.output.write(newRecord)

    # Applies if the source uses WHOLE_FILE as data format
    # input_stream = record.value['fileRef'].getInputStream()
    # try:
    #   input_stream.read() #Process the input stream
    # finally:  
    #   input_stream.close()

    # Modify a record header attribute entry
    # record.attributes['name'] = record.attributes['first_name'] + ' ' + record.attributes['last_name']

    # Get a record header with field names ex. get sourceId and errorCode
    # sourceId = record.sourceId
    # errorCode = ''
    # if record.errorCode:
    #   errorCode = record.errorCode

    # Write record to processor output
    sdc.output.write(record)

  except Exception as e:
    # Send record to error
    sdc.error.write(record, str(e))

