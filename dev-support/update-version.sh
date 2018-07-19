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

set -e
set -x
version=$1
if [[ -z "$version" ]]
then
  echo "Usage: $0 NEW-VERSION"
  exit 1
fi
mvn versions:set -Drelease -Parchetype,stage-lib-parent,hdp-stagelib-base -DnewVersion=$version

ROOTDIR=`echo $PWD | tr -d "\r"`

for d in ${ROOTDIR} dist release root-proto
do
	pushd $d
	mvn versions:set-property -Dproperty=datacollector-edge.version -DnewVersion=$version -DgenerateBackupPoms=false -DallowSnapshots=true
	mvn versions:set-property -Dproperty=datacollector-api.version -DnewVersion=$version -DgenerateBackupPoms=false -DallowSnapshots=true
	mvn versions:set-property -Dproperty=datacollector-spark-api.version -DnewVersion=$version -DgenerateBackupPoms=false -DallowSnapshots=true
	mvn versions:set-property -Dproperty=streamsets-datacollector-rbgen-maven-plugin.version -DnewVersion=$version -DgenerateBackupPoms=false -DallowSnapshots=true
	popd
done

# mvn version:set doesn't work with the following sub-modules. Update them via perl regex.
for d in rbgen-maven-plugin stage-lib-archetype e2e-tests
do
  pushd $d
  perl -i -pe 's@(<version>)(\d+.\d+.\d+(-SNAPSHOT)?)(<\/version>)@${1}'"$version"'${4}@g' pom.xml
  popd
done

for f in BUILD.md dist/src/main/etc/sdc.properties
do
  perl -i -pe 's@(datacollector.(all.)?)\d+.\d+.\d+(-SNAPSHOT)?@${1}'"$version"'@g' $f
done

ui_version=$(echo $version | perl -pe 's@(\d+.\d+.\d+).(\d+)(-SNAPSHOT)?@${1}${2}${3}@g')
for f in datacollector-ui/package.json datacollector-ui/bower.json e2e-tests/package.json
do
  perl -i -pe 's@^(\s+"version"\s*:\s*").*("\s*,\s*)@${1}'"$ui_version"'${2}@g' $f
done

find . -name pom.xml.versionsBackup -delete
