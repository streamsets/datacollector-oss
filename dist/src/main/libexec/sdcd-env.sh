#!/bin/bash
#
# (c) 2014 StreamSets, Inc. All rights reserved. May not
# be copied, modified, or distributed in whole or part without
# written consent of StreamSets, Inc.
#

#
# This script is sourced when using the data collector initd scripts
# Refer to the initd/README file for details
#

# user that will run the data collector, it must exist in the system
#
export SDC_USER=sdc

# group of the user that will run the data collector, it must exist in the system
#
export SDC_GROUP=sdc

# directory where the data collector will store pipelines and their runtime information
#
export SDC_DATA=/var/run/sdc

# directory where the data collector write its logs
#
export SDC_LOG=/var/log/sdc

# directory where the data collector will read its configuration
#
export SDC_CONF=/etc/sdc

# JVM options for the data collector process
#
export SDC_JAVA_OPTS="-Xmx1024m ${SDC_JAVA_OPTS}"
