#!/bin/bash
#
# Copyright 2018 StreamSets Inc.
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

# This script generates the following manifest
#
MANIFEST_TYPE="repo-library"
MANIFEST_VERSION="1"

# Script starts running after "Script STARTS RUNNING HERE" comment
#
# The only global variables (they are set by this script) used by the functions in this script are:
#  SCRIPT_NAME: the name of this script at invocation time
#  WORK_DIR: temporary working directory created by the script
#  KEEP_WORK_DIR: if the temporary directory is to be kept after execution
#

help() {
      echo ""
      echo "${SCRIPT_NAME} <OPTIONS>"
      echo ""
      echo "  Creates a repository manifest from stage library manifests."
      echo ""
      echo "  Options:"
      echo ""
      echo "    --repoLabel <REPOSITORY LABEL>"
      echo "        Label for the repository"
      echo ""
      echo "    --stagelibsDir <STAGE LIBS DIRECTORY>"
      echo "       A directory with multiple stage library tarballs to create "
      echo "       repository manifest for."
      echo "       It can be an absolute or relative path."
      echo "       This option and the --lib option are exclusive of each other."
      echo ""
      echo "    [--stagelibsVersion <REPOSITORY VERSION>]"
      echo "        If all stage libraries in the repository are of the same version"
      echo "        that version is indicated at repo level"
      echo ""
      echo "    [--stagelibsLicense <REPOSITORY LICENSE>]"
      echo "        If all stage libraries in the repository have the same license"
      echo "        that license is indicated at repo level"
      echo ""
      echo "    [--stagelibsMinSdcVersion <MINIMUM SDC VERSION>]"
      echo "        If all stage libraries in the repository have the same minimum SDC version"
      echo "        that minimum SDC version is indicated at repo level"
      echo ""
      echo "    [--help]"
      echo "       Prints this help message."
      echo ""
      echo "  Output (on success):"
      echo ""
      echo "      <STAGE LIBS DIRECTORY>/repository.manifest.json"
      echo ""
}

assertOption() {
  msg=${1}
  value=${2}
  if [ "${2}" = "" ]; then
    echo ""
    echo "ERROR: Option ${option} not set"
    help
    exit 1
  fi
}

assertOptionValue() {
  option=${1}
  value=${2}
  if [ "${2}" = "" ]; then
    echo ""
    echo "ERROR: Option ${option} expects a value"
    help
    exit 1
  fi
}

# deletes ${WORK_DIR} directory if set and if ${KEEP_WORK_DIR} is not TRUE
cleanUp() {
  if [ "${WORK_DIR}" != "" ]; then
    if [ "${KEEP_WORK_DIR}" = "false" ]; then
      rm -rf ${WORK_DIR}
    else
      echo "  Cleanup disabled, working dir at ${WORK_DIR}"
      echo ""
    fi
  fi
}

fail() {
  code=${1}
  shift
  msg="${@}"
  echo ""
  echo "  ERROR: ${@}"
  echo ""
  cleanUp
  exit ${code}
}

run() {
  msg=${1}
  shift
  "${@}"
  ret=$?
  if [ ${ret} != 0 ]; then
    fail ${ret} ${msg}
  fi
}

runEcho() {
  defaultValue=${1}
  shift
  output="`${@}; if [ ${?} != 0 ]; then echo ERROR; fi`"
  if [ "${output}" = "ERROR" ]; then
    echo ${defaultValue}
  else
    echo ${output}
  fi
}

failIf() {
  value1=${1}
  value2=${2}
  msg=${3}
  if [ "${value1}" == "${value2}" ]; then
    fail 1 ${msg}
  fi
}

#
# Script STARTS RUNNING HERE
#

SCRIPT_NAME=$(runEcho "Could not get the name of the script" basename $0)
STAGE_LIBS_DIR=""
REPO_LABEL=""
ALL_STAGE_LIBS_VERSION="undefined"
ALL_STAGE_LIBS_LICENSE="undefined"
ALL_STAGE_LIBS_MIN_SDC_VERSION="undefined"

while test ${#} -gt 0; do
  case "${1}" in
    --repoLabel)
      shift
      assertOptionValue "--repoLabel" ${1}
      REPO_LABEL=${1}
      shift
      ;;
    --stagelibsDir)
      shift
      assertOptionValue "--stagelibsDir" ${1}
      STAGE_LIBS_DIR=${1}
      shift
      ;;
    --stagelibsVersion)
      shift
      assertOptionValue "--stagelibsVersion" ${1}
      ALL_STAGE_LIBS_VERSION=${1}
      shift
      ;;
    --stagelibsLicense)
      shift
      assertOptionValue "--stagelibsLicense" ${1}
      ALL_STAGE_LIBS_LICENSE=${1}
      shift
      ;;
    --stagelibsMinSdcVersion)
      shift
      assertOptionValue "--stagelibsMinSdcVersion" ${1}
      ALL_STAGE_LIBS_MIN_SDC_VERSION=${1}
      shift
      ;;
    --help)
      help
      exit 0
      ;;
    *)
      echo ""
      echo "ERROR: Invalid option ${1}"
      help
      exit 1
      ;;
  esac
done

failIf "${REPO_LABEL}" "" "ERROR: A repository label must be specified"

failIf "${STAGE_LIBS_DIR}" "" "ERROR: A stagelibs directory must be specified"

INITIAL_DIR=`pwd`
run "(1) Directory ${STAGE_LIBS_DIR} does not exist" cd ${STAGE_LIBS_DIR}
STAGE_LIBS_DIR=`pwd`
run "(2) Could not go back to ${INITIAL_DIR} directory" cd ${INITIAL_DIR}

MANIFEST_NAME="repository.manifest.json"

MANIFEST_FILE=${STAGE_LIBS_DIR}/${MANIFEST_NAME}

if [ "${STAGE_LIBS_DIR}" != "" ]; then
  STAGE_LIB_MANIFESTS=$(runEcho "()" find ${STAGE_LIBS_DIR} -name *.stagelib-manifest.json -depth 1)
  if [ "${STAGE_LIB_MANIFESTS}" != "" ]; then
    run "(3) Could not write stagelib manifest" echo "{" > ${MANIFEST_FILE}
    run "(3) Could not write stagelib manifest" echo "  \"manifest.type\" : \"${MANIFEST_TYPE}\"," >> ${MANIFEST_FILE}
    run "(3) Could not write stagelib manifest" echo "  \"manifest.version\" : \"${MANIFEST_VERSION}\"," >> ${MANIFEST_FILE}
    run "(3) Could not write stagelib manifest" echo "  \"repo.label\" : \"${REPO_LABEL}\"," >> ${MANIFEST_FILE}
    run "(3) Could not write stagelib manifest" echo "  \"stage-libraries.version\" : \"${ALL_STAGE_LIBS_VERSION}\"," >> ${MANIFEST_FILE}
    run "(3) Could not write stagelib manifest" echo "  \"stage-libraries.license\" : \"${ALL_STAGE_LIBS_LICENSE}\"," >> ${MANIFEST_FILE}
    run "(3) Could not write stagelib manifest" echo "  \"stage-libraries.min.sdc.version\" : \"${ALL_STAGE_LIBS_MIN_SDC_VERSION}\"," >> ${MANIFEST_FILE}
    run "(3) Could not write stagelib manifest" echo -n "  \"stage-libraries\" : [" >> ${MANIFEST_FILE}
    SEPARATOR=""
    for STAGE_LIB_MANIFEST in ${STAGE_LIB_MANIFESTS}; do
      STAGE_LIB_MANIFEST=$(runEcho "(3) Could not get the file name of ${STAGE_LIB_MANIFEST}" basename ${STAGE_LIB_MANIFEST})
      run "(3) Could not write stagelib manifest" echo "${SEPARATOR}" >> ${MANIFEST_FILE}
      run "(3) Could not write stagelib manifest" echo -n "    \"${STAGE_LIB_MANIFEST}\"" >> ${MANIFEST_FILE}
      SEPARATOR=","
    done
    run "(3) Could not write stagelib manifest" echo "" >> ${MANIFEST_FILE}
    run "(3) Could not write stagelib manifest" echo "  ]" >> ${MANIFEST_FILE}
    run "(3) Could not write stagelib manifest" echo "}" >> ${MANIFEST_FILE}
    run "(3) Could not write stagelib manifest" echo "" >> ${MANIFEST_FILE}

    run "(4) Could not create SHA1 file for ${MANIFEST_NAME}" sha1sum ${MANIFEST_FILE} > ${MANIFEST_FILE}.sha1

    echo ""
    echo "  Repository manifest and SHA1 files created at ${STAGE_LIBS_DIR}"
    echo ""
  else
    echo ""
    echo "  WARN: There are no stage library tarballs at ${STAGE_LIBS_DIR}"
    echo ""
  fi


fi

echo ""
exit 0
