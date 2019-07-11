/**
 * Available Objects:
 *
 *  state: a dict that is preserved between invocations of this script.
 *        Useful for caching bits of data e.g. counters and long-lived resources.
 *
 *  log.<loglevel>(msg, obj...): use instead of print to send log messages to the log4j log instead of stdout.
 *                               loglevel is any log4j level: e.g. info, error, warn, trace.
 *   sdcFunctions.getFieldNull(Record, 'field path'): Receive a constant defined above
 *                          to check if the field is typed field with value null
 *   sdcFunctions.createMap(boolean listMap): Create a map for use as a field in a record.
 *                          Pass true to this function to create a list map (ordered map)
 */
