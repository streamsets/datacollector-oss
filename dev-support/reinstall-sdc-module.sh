#!/usr/bin/env bash
#
# Copyright 2020 StreamSets Inc.
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

# Rebuilds an entire module (i.e. stagelib) and reinstalls to dist/target
# Assumes a local build has already been done and lives in dist/target/streamsets-datacollector-*
# Deletes the individual modules' target directories, as well as the entire streamsets-libs subdirectory
# within the dist/target installation.  Then, runs a new mvn install invocation (always with all-libs profile)
# that includes all these modules, as well as modules they depend on (ex: container, etc.), as well as dist
# (so they are actually reinstalled properly). Does not touch the UI or any data/config files in the locally
# installed version.
#
# Example invocation (rebuild and reinstall all CDH 6.x stagelibs)
# dev-support/reinstall-sdc-module.sh $(ls -d cdh_6_*-lib)

if [ $# -eq 0 ]
then
    echo >&1 "No module names given as args"
    exit 10
fi
for module in "$@"
do
    echo "Deleting module $module"
    rm -rf $module/target dist/target/streamsets-datacollector-*/streamsets-datacollector-*/streamsets-libs/*$module/lib
done
module_names=$(echo "$@" | tr ' ' ',')
echo "Running new Maven build for modules: $module_names"
mvn install -Dmaven.javadoc.skip=true -DskipTests -DskipRat -Pdist,all-libs -am -pl dist,$module_names