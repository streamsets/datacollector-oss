# Available Objects:
#
#  sdc.state: a dict that is preserved between invocations of this script.
#         Useful for caching bits of data e.g. counters.
#  sdc.log.<loglevel>(msg, obj...): use instead of print to send log messages to the log4j log instead of stdout.
#                               loglevel is any log4j level: e.g. info, error, warn, trace.
#  sdc.getFieldNull(Record, 'field path'): Receive a constant defined above 
#                                  to check if the field is typed field with value null.
#  sdc.createMap(boolean listMap): Create a map for use as a field in a record.
#                            Pass True to this function to create a list map (ordered map).
#  sdc.createEvent(String type, int version): Creates a new event.
#                            Create new empty event with standard headers.
#  sdc.toEvent(Record): Send event to event stream.
#                            Only events created with sdcFunctions.createEvent are supported.
#

# sdc.state['connection'].close()

