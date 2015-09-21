#!/bin/bash
#
#
# Licensed to the Apache Software Foundation (ASF) under one
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
set -e
set -x
version=$1
if [[ -z "$version" ]]
then
  echo "Usage: $0 NEW-VERSION"
  exit 1
fi
mvn versions:set -Drelease -Parchetype -DnewVersion=$version
pushd rbgen-maven-plugin
mvn versions:set -DnewVersion=$version
popd
pushd stage-lib-archetype
mvn versions:set -DnewVersion=$version
popd
pushd e2e-tests
mvn versions:set -DnewVersion=$version
popd
perl -i -pe 's@^(\s+"version"\s*:\s*").*("\s*,\s*)$@${1}'"$version"'$2@' datacollector-ui/package.json
find . -name pom.xml.versionsBackup -delete
