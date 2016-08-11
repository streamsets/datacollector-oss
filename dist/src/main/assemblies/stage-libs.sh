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

VERSION=$1
DIST=$2
TARGET=$3

mkdir -p ${TARGET}
#cd ${DIST}/dist/target/streamsets-datacollector-${VERSION}/streamsets-datacollector-${VERSION} || exit
#mkdir streamsets-libs
cd ${TARGET} || exit
echo "dist : ${DIST}"
echo "target: ${TARGET}"

for STAGE_DIR in ${DIST}/*
do
  echo "stage_dir: ${STAGE_DIR}"
  STAGE_LIB=${STAGE_DIR}/target/streamsets-libs
  echo "stage_lib: ${STAGE_LIB}"
  if [ -d "$STAGE_LIB" ]
  then
    STAGE_NAME=`ls ${STAGE_LIB}`
    echo "Processing stage library: ${STAGE_NAME}"
    echo "tar file: ${STAGE_DIR}/target/${STAGE_NAME}-${VERSION}.tgz"
    #if [ -d "${STAGE_DIR}/target/${STAGE_NAME}-${VERSION}.tgz" ]
    #then
        echo "********** tar -xzf ${STAGE_DIR}/target/${STAGE_NAME}-${VERSION}.tgz"
        tar -xzf ${STAGE_DIR}/target/${STAGE_NAME}-${VERSION}.tgz
    #fi
  fi
done
