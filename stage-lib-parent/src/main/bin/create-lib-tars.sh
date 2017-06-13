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

# for debugging
#set -x

VERSION=$1
TARGET=$2
DESTINATION=$3

STAGE_LIBS=${TARGET}/streamsets-libs

if [ ! -d "DESTINATION" ]
then
  `mkdir -p ${DESTINATION}`
fi

cd ${STAGE_LIBS} || exit
for STAGE_LIB in ${STAGE_LIBS}/*
do
  if [ -d "$STAGE_LIB" ]
  then
    STAGE_NAME=`basename ${STAGE_LIB}`
    echo "Processing stage library: ${STAGE_NAME}"
    tar czf ${DESTINATION}/${STAGE_NAME}-${VERSION}.tgz ${STAGE_NAME}/*
  fi
done
