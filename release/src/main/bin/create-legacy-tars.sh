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

mkdir -p $TARGET

# Human readable index file
INDEX_FILE_PATH="${TARGET}/index.html"

echo "<html><head><title>StreamSets Data Collector Legacy Stage Libraries</title></head><body>" > $INDEX_FILE_PATH
echo "<h1>StreamSets Data Collector Legacy Stage Libraries</h1><ul>" > $INDEX_FILE_PATH

# Machine readable manifest file
STAGE_LIB_MANIFEST_FILE="stage-lib-manifest.properties"
STAGE_LIB_MANIFEST_FILE_PATH="${TARGET}/${STAGE_LIB_MANIFEST_FILE}"

DOWNLOAD_URL=${4:-"https://archives.streamsets.com/datacollector/${VERSION}/legacy/"}

STAGE_LIBS="${DIST}/${DIST_NAME}/streamsets-libs"

echo "#" > ${STAGE_LIB_MANIFEST_FILE_PATH}
echo "# Copyright 2015 StreamSets Inc. " >> ${STAGE_LIB_MANIFEST_FILE_PATH}
echo "#" >> ${STAGE_LIB_MANIFEST_FILE_PATH}
echo "" >> ${STAGE_LIB_MANIFEST_FILE_PATH}

echo "download.url=${DOWNLOAD_URL}" >> ${STAGE_LIB_MANIFEST_FILE_PATH}
echo "version=${VERSION}" >> ${STAGE_LIB_MANIFEST_FILE_PATH}


cd ${DIST} || exit
for STAGE_LIB in ${DIST}/*
do
  if [ -d "$STAGE_LIB" ]
  then
    LIB_DIR=`basename ${STAGE_LIB}`
    echo "Processing stage library: ${LIB_DIR}"
    tar -C ${DIST} -czf ${TARGET}/${LIB_DIR}-${VERSION}.tgz ${LIB_DIR}/*
    CURRENT_DIR=`pwd`
    cd ${TARGET} || exit
    sha1sum ${LIB_DIR}-${VERSION}.tgz > ${LIB_DIR}-${VERSION}.tgz.sha1
    cd ${CURRENT_DIR}
    LIB_NAME=`unzip -p ${STAGE_LIBS}/${LIB_DIR}/lib/${LIB_DIR}-*.jar data-collector-library-bundle.properties | grep library.name | sed 's/library.name=//'`
    echo "stage-lib.${LIB_DIR}=${LIB_NAME}" >> ${STAGE_LIB_MANIFEST_FILE_PATH}

    # Human readable file
    echo "<li><a href='${LIB_DIR}-${VERSION}.tgz'>$LIB_DIR</a> (<a href='${LIB_DIR}-${VERSION}.tgz.sha1'>checksum</a>)</li>" >> $INDEX_FILE_PATH
  fi
done

cd ${TARGET} || exit
sha1sum ${STAGE_LIB_MANIFEST_FILE} > ${STAGE_LIB_MANIFEST_FILE}.sha1

# Finish the human readable index file
echo "</ul></body></html>" >> $INDEX_FILE_PATH
