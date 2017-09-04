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

VERSION=$1
DIST=$2
TARGET=$3

STAGE_LIB_MANIFEST_FILE="stage-lib-manifest.properties"
STAGE_LIB_MANIFEST_FILE_PATH="${TARGET}/${STAGE_LIB_MANIFEST_FILE}"

STAGE_LIB_MANIFEST_JSON_FILE="stage-lib-manifest.json"
STAGE_LIB_MANIFEST_JSON_FILE_PATH="${TARGET}/${STAGE_LIB_MANIFEST_JSON_FILE}"
JSON_SEPARATOR=""

DOWNLOAD_URL=${4:-"https://archives.streamsets.com/datacollector/${VERSION}/tarball/"}

DIST_NAME=`basename ${DIST}`

STAGE_LIBS="${DIST}/${DIST_NAME}/streamsets-libs"

echo "#" > ${STAGE_LIB_MANIFEST_FILE_PATH}
echo "# Copyright 2015 StreamSets Inc. " >> ${STAGE_LIB_MANIFEST_FILE_PATH}
echo "#" >> ${STAGE_LIB_MANIFEST_FILE_PATH}
echo "" >> ${STAGE_LIB_MANIFEST_FILE_PATH}

echo "download.url=${DOWNLOAD_URL}" >> ${STAGE_LIB_MANIFEST_FILE_PATH}
echo "version=${VERSION}" >> ${STAGE_LIB_MANIFEST_FILE_PATH}

echo "{" >> ${STAGE_LIB_MANIFEST_JSON_FILE_PATH}

cd ${DIST} || exit
for STAGE_LIB in ${STAGE_LIBS}/*
do
  if [ -d "$STAGE_LIB" ]
  then
    LIB_DIR=`basename ${STAGE_LIB}`
    echo "Processing stage library: ${LIB_DIR}"
    tar czf ${TARGET}/${LIB_DIR}-${VERSION}.tgz ${DIST_NAME}/streamsets-libs/${LIB_DIR}/*
    CURRENT_DIR=`pwd`
    cd ${TARGET} || exit
    sha1sum ${LIB_DIR}-${VERSION}.tgz > ${LIB_DIR}-${VERSION}.tgz.sha1
    cd ${CURRENT_DIR}
    LIB_NAME=`unzip -p ${STAGE_LIBS}/${LIB_DIR}/lib/${LIB_DIR}-*.jar data-collector-library-bundle.properties | grep library.name | sed 's/library.name=//'`
    echo "stage-lib.${LIB_DIR}=${LIB_NAME}" >> ${STAGE_LIB_MANIFEST_FILE_PATH}

    STAGE_DEF_LIST_JSON=`unzip -p ${STAGE_LIBS}/${LIB_DIR}/lib/${LIB_DIR}-*.jar StageDefList.json`

    if [ "$STAGE_DEF_LIST_JSON" ]
    then
    echo "${JSON_SEPARATOR}" >> ${STAGE_LIB_MANIFEST_JSON_FILE_PATH}
    echo "\"stage-lib.${LIB_DIR}\": {" >> ${STAGE_LIB_MANIFEST_JSON_FILE_PATH}
    echo "  \"label\": \"${LIB_NAME}\"," >> ${STAGE_LIB_MANIFEST_JSON_FILE_PATH}
    echo "  \"stageDefList\": ${STAGE_DEF_LIST_JSON}" >> ${STAGE_LIB_MANIFEST_JSON_FILE_PATH}
    echo "}" >> ${STAGE_LIB_MANIFEST_JSON_FILE_PATH}

    JSON_SEPARATOR=","
    fi
  fi
done


echo "}" >> ${STAGE_LIB_MANIFEST_JSON_FILE_PATH}

cd ${TARGET} || exit
sha1sum ${STAGE_LIB_MANIFEST_FILE} > ${STAGE_LIB_MANIFEST_FILE}.sha1
