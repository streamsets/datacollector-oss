#!/bin/bash
#
# (c) 2014 StreamSets, Inc. All rights reserved. May not
# be copied, modified, or distributed in whole or part without
# written consent of StreamSets, Inc.
#

#
# This script is sourced when using the bin/sdc script to start the data collector
#

# directory where the data collector will store pipelines and their runtime information
#
#export SDC_DATA=/var/lib/sdc

# directory where the data collector write its logs
#
#export SDC_LOG=/var/log/sdc

# directory where the data collector will read its configuration
#
#export SDC_CONF=/etc/sdc

# directory where the data collector will read pipeline resource files from
#
#export SDC_RESOURCES=/var/lib/sdc-resources

# Includes the JARs in extra lib in the root classloader, this is required to support
# Snappy compression in Cassandra
#
export SDC_ROOT_CLASSPATH=${SDC_DIST}/root-lib/'*'

export SDC_JAVA_OPTS="-Xmx1024m -Xms1024m -XX:PermSize=128m -XX:MaxPermSize=256m -server ${SDC_JAVA_OPTS}"

# Enables/disables the JVM security manager
#
export SDC_SECURITY_MANAGER_ENABLED=true
