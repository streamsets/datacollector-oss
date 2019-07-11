/*
 * Available objects:
 *   state: A Map<String, Object> that is preserved between invocations of this script.
 *          Useful for caching bits of data, e.g. counters.
 *
 *   log.<level>(msg, obj...): Use instead of println to send log messages to the log4j log
 *                             instead of stdout.
 *                             loglevel is any log4j level: e.g. info, warn, error, trace.
 *   sdcFunctions.getFieldNull(Record, 'field path'): Receive a constant defined above
 *                          to check if the field is typed field with value null
 *   sdcFunctions.createMap(boolean listMap): Create a map for use as a field in a record.
 *                          Pass true to this function to create a list map (ordered map)
 *   sdcFunctions.createEvent(String type, int version): Creates a new event.
 *                          Create new empty event with standard headers.
 *   sdcFunctions.toEvent(Record): Send event to event stream
 *                          Only events created with sdcFunctions.createEvent are supported.
 *   sdcFunctions.pipelineParameters(): Map with pipeline runtime parameters.
 *
 */

// state?.connection.close()
