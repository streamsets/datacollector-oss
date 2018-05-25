#!/bin/bash
#
# Copyright 2017 StreamSets Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
export SDC_ROOT_CLASSPATH=${SDC_ROOT_CLASSPATH:-${SDC_DIST}/root-lib/'*'}

# SDC upon start will verify that `ulimit -n` will return at least the following,
# otherwise SDC start will fail.
export SDC_FILE_LIMIT="${SDC_FILE_LIMIT:-32768}"

# JVM options for the data collector process
#
export SDC_JAVA_OPTS="-Xmx1024m -Xms1024m -server -XX:-OmitStackTraceInFastThrow ${SDC_JAVA_OPTS}"

# Indicate that MapR Username/Password security is enabled
#export SDC_JAVA_OPTS="-Dmaprlogin.password.enabled=true ${SDC_JAVA_OPTS}"

# Java 8 (JDK 1.8) specific options
# by default, use CMS garbage collector
export SDC_JAVA8_OPTS=${SDC_JAVA8_OPTS:-"-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -Djdk.nio.maxCachedBufferSize=262144"}

# Enables/disables the JVM security manager
#
export SDC_SECURITY_MANAGER_ENABLED=${SDC_SECURITY_MANAGER_ENABLED:-true}

# Produce heap dump when SDC will die on OutOfMemoryError
export SDC_HEAPDUMP_ON_OOM=${SDC_HEAPDUMP_ON_OOM:-true}

# Optional path for the heap dump file, default is $SDC_LOG/sdc_heapdump_${timestamp}.hprof
#export SDC_HEAPDUMP_PATH=

# Enable GC logging automatically
export SDC_GC_LOGGING=${SDC_GC_LOGGING:-true}

# SDC will by default only run on Oracle JDK, any other JDK has to be explicitly enabled
export SDC_ALLOW_UNSUPPORTED_JDK=${SDC_ALLOW_UNSUPPORTED_JDK:-false}

# For Cluster yarn streaming mode in CDH.
export SPARK_KAFKA_VERSION=0.10
