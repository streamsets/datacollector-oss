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
export SDC_DATA=/var/lib/sdc

 directory where the data collector will read pipeline resource files from
#
export SDC_RESOURCES=/var/lib/sdc-resources

# directory where the data collector write its logs
#
export SDC_LOG=/var/log/sdc

# directory where the data collector will read its configuration
#
export SDC_CONF=/etc/sdc

# Includes the JARs in extra lib in the root classloader, this is required to support
# Snappy compression in Cassandra
#
export SDC_ROOT_CLASSPATH=${SDC_DIST}/root-lib/'*'

# JVM options for the data collector process
#
export SDC_JAVA_OPTS="-Xmx1024m -XX:PermSize=128M -XX:MaxPermSize=256M -server ${SDC_JAVA_OPTS}"

# Enables/disables the JVM security manager
#
export SDC_SECURITY_MANAGER_ENABLED=true
