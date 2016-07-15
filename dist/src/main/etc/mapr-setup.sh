#
# Copyright 2016 StreamSets Inc.
#
# Licensed under the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/bin/bash
#
# Prerequisites:
# - The MapR client package must be installed and configured
#   on the machine where StreamSets is being installed.
#
# This script does followings:
# - Obtain MapR version and MapR Home directory
# - Remove the MapR version from blacklist in etc/sdc.properties
# - Create soft links to MapR client libraries so that StreamSets data collector can load these at runtime.
# - Add permissions to MapR libraries in sdc-security.policy

BLACKLIST_PROP="system.stagelibs.blacklist"
SDC_PROP_FILE="sdc.properties"
SDC_POLOCY_FILE="sdc-security.policy"
MAPR_VERSION=5.1
MAPR_HOME=""
MAPR_LIB="streamsets-datacollector-mapr"
SDC_HOME=$PWD
if [[ ${SDC_HOME} == *"/etc" ]];then
  SDC_HOME=${SDC_HOME%/etc}
fi

# Confirm sdc.properties exists in the current directory and has a permission to write
if [ ! -w "$SDC_HOME/etc/$SDC_PROP_FILE" ]; then
  echo "Error: Check etc/$SDC_PROP_FILE exists in the current directory and current user has write permission"
  echo "Exit."
  exit 0
fi

# Check if this is running on supported OS
if [[ $OSTYPE == "darwin"* ]]; then #MacOS requires additional '' in sed command
  option="''"
elif [[ $OSTYPE == "cygwin" ]];then
  echo "Error: Cannot used on Windows OS"
  exit 0
fi

# Input from user
read -p "Please enter the MapR version (default 5.1): " MAPR_VERSION
MAPR_VERSION=${MAPR_VERSION:=5.1}

read -p "Please enter the absolute path of MapR Home (default /opt/mapr): " MAPR_HOME
MAPR_HOME=${MAPR_HOME:="/opt/mapr"}

# Check if MAPR_HOME is indeed MapR home directory
if [ ! -e "${MAPR_HOME}/MapRBuildVersion" ]; then
  echo "Error: ${MAPR_HOME} is not the correct path to MapR Home"
  exit 0
fi

# Remove MapR Version from sdc.properies file
printf "Updating etc/sdc.properties file ...."
_MAPR_VERSION=${MAPR_VERSION/"."/"_"}
MAPR_LIB=${MAPR_LIB}_${_MAPR_VERSION}-lib
original_property=$(grep -i "$BLACKLIST_PROP" "${SDC_HOME}/etc/${SDC_PROP_FILE}")
blacklist_property=${original_property/$MAPR_LIB,/}
sed -i ${option} "s/${original_property}/${blacklist_property}/" "${SDC_HOME}/etc/${SDC_PROP_FILE}"
printf "Done.\n"

# Create symbolic links. Ignore the stderr (there will be a lot of symlinks already exist errors)
printf "Creating symbolic links ...."
ln -s ${MAPR_HOME}/lib/*.jar ${SDC_HOME}/streamsets-libs/${MAPR_LIB}/lib/ 2>/dev/null
ln -s ${MAPR_HOME}/hbase/hbase-*/lib/*.jar  ${SDC_HOME}/streamsets-libs/${MAPR_LIB}/lib/ 2>/dev/null
ln -s ${MAPR_HOME}/hive/hive-*/lib/*.jar ${SDC_HOME}/streamsets-libs/${MAPR_LIB}/lib/ 2>/dev/null
ln -s ${MAPR_HOME}/hive/hive-*/hcatalog/share/hcatalog/*.jar ${SDC_HOME}/streamsets-libs/${MAPR_LIB}/lib/ 2>/dev/null
ln -s ${MAPR_HOME}/lib/maprfs-${MAPR_VERSION}*.jar  ${SDC_HOME}/root-lib/ 2>/dev/null
printf "Done.\n"

# Add permission to sdc-security.policy file
printf "Updating etc/sdc-security.policy file ..."
printf "\ngrant c:wodebase \"file://${MAPR_HOME}/-\" {\n  permission java.security.AllPermission;\n};\n" >> ${SDC_HOME}/etc/${SDC_POLOCY_FILE}
printf "Done\n"

echo "Succeed"