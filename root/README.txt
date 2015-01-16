StreamSets Data Collector root module for the implementation.

All Data Collector implementation modules (not libraries) must have
this module as parent.

This module inherits from root-proto POM and in addition it defines
the version of all 3rd party libraries used by the implementation.

Note that modules like 'el' should inherit this POM.
