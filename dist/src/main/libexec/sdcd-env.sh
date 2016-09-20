#!/bin/bash
#
#
# Licensed under the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
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

# directory where the data collector will read pipeline resource files from
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
export SDC_ROOT_CLASSPATH=${SDC_ROOT_CLASSPATH:-${SDC_DIST}/root-lib/'*'}

# JVM options for the data collector process
#
export SDC_JAVA_OPTS="-Dhttps.protocols=TLSv1.2,TLSv1.1 -Xmx1024m -Xms1024m -XX:PermSize=256m -XX:MaxPermSize=512m -server ${SDC_JAVA_OPTS}"

# Enables/disables the JVM security manager
#
export SDC_SECURITY_MANAGER_ENABLED=true
