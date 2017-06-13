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

# this allows people to override the default profile for build scripts
if [[ -n "$CI_PROFILE_APPLIED" ]]
then
  return 0
fi
export CI_PROFILE_APPLIED=1


# define variables here
export JAVA7_HOME=/opt/jdk-7
export JAVA8_HOME=/opt/jdk-8
export M2_HOME=/opt/maven-3.2
export NODE_HOME=/opt/node-0.10

PROFILE=$(basename ${BASH_SOURCE})
PROFILE=${PROFILE%-*}
PROFILE=${PROFILE#*-}

# apply profile here
case "$PROFILE" in
  java8)
    export TEST_JVM=${JAVA8_HOME}
    ;;
  *)
    export TEST_JVM=${JAVA7_HOME}
    ;;
esac
export JAVA_HOME=${JAVA8_HOME}
export PATH=${JAVA_HOME}/bin:${M2_HOME}/bin:${MAVEN_HOME}/bin:${NODE_HOME}/bin:$PATH
echo $PATH
