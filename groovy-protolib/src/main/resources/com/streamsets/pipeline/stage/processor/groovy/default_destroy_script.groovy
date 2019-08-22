/*
 * Available objects:
 *   sdc.state: A Map<String, Object> that is preserved between invocations of this script.
 *          Useful for caching bits of data, e.g. counters.
 *   sdc.log.<level>(msg, obj...): Use instead of println to send log messages to the log4j log
 *                             instead of stdout.
 *                             loglevel is any log4j level: e.g. info, warn, error, trace.
 *   sdc.getFieldNull(Record, 'field path'): Receive a constant defined above
 *                          to check if the field is typed field with value null
 *   sdc.createMap(boolean listMap): Create a map for use as a field in a record.
 *                          Pass true to this function to create a list map (ordered map)
 *   sdc.createEvent(String type, int version): Creates a new event.
 *                          Create new empty event with standard headers.
 *   sdc.toEvent(Record): Send event to event stream
 *                          Only events created with sdc.createEvent are supported.
 *   sdc.pipelineParameters(): Map with pipeline runtime parameters.
 */

// sdc.state.connection.close()
