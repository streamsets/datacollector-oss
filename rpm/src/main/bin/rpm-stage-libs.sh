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

# for debugging, it will prints commands and their arguments as they are executed.
set -x

DIST=$1
TARGET=$2
VERSION=$3
DISTS=(el6 el7)

cd "${TARGET}" || exit

for dist in "${DISTS[@]}"; do
  RPM_ALL_DIST=streamsets-datacollector-${VERSION}-${dist}-all-rpms
  mkdir -p "${RPM_ALL_DIST}"

  # copy all stage-lib rpms to ${RPM_ALL_DIST}
  for STAGE_DIR in ${DIST}/*
  do
    STAGE_LIB=${STAGE_DIR}/target/rpm
    STAGE_NAME=$(basename "${STAGE_DIR}")
    RPM="rpm"
    if [ -d "${STAGE_LIB}" ] && [ "${STAGE_NAME}" != "${RPM}" ];
    then
      echo "Processing stage library: ${STAGE_NAME}"
      ln -sf "${STAGE_LIB}"/*/RPMS/noarch/streamsets-datacollector-*.noarch.rpm "${RPM_ALL_DIST}"
    fi
  done

  # copy core rpm to ${RPM_ALL_DIST}
  ln -sf "${TARGET}"/streamsets-datacollector-"${dist}"/RPMS/noarch/streamsets-datacollector-*.noarch.rpm "${RPM_ALL_DIST}"

  # additional step to archive all stage-libs into tar file -- no additional compression since rpm already uses cpio
  tar cfh "${RPM_ALL_DIST}/streamsets-datacollector-${VERSION}-${dist}-all-rpms.tar" "${RPM_ALL_DIST}"/streamsets-datacollector-*.noarch.rpm
done