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
MANIFEST_TYPE="stage-library"
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
      echo "  Creates a stage library manifest from a stage library tarball."
      echo "  It also creates the SHA1 checksums for the stage library tarball and"
      echo "  the stage library manifest file."
      echo ""
      echo "  Options:"
      echo ""
      echo "    --stagelib <STAGE LIB TARBALL>"
      echo "       The stage library tarball to create stage library manifest for."
      echo "       It can be an absolute or relative path."
      echo "       This option and the --stagelibsDir option are exclusive of each other."
      echo ""
      echo "    --stagelibsDir <STAGE LIBS DIRECTORY>"
      echo "       A directory with multiple stage library tarballs to create "
      echo "       stage library manifest for."
      echo "       It can be an absolute or relative path."
      echo "       This option and the --stagelib option are exclusive of each other."
      echo ""
      echo "    [--output <OUTPUT DIR>]"
      echo "       The directory where to write the manifest and SHA1 files."
      echo "       It can be an absolute or relative path."
      echo "       The directory must exist. Any existing files are overwritten."
      echo "       If not specified the directory where the stage library tarball"
      echo "       will be used as output."
      echo ""
      echo "    [--keepWorkdir]"
      echo "       Keeps the temporary working directory and files used to generate"
      echo "       the stage library manifest."
      echo ""
      echo "    [--help]"
      echo "       Prints this help message."
      echo ""
      echo "  Output (on success):"
      echo ""
      echo "      <OUTPUT DIR>/<STAGE LIB>.sha1"
      echo "      <OUTPUT DIR>/<STAGE LIB>.stagelib-manifest.json"
      echo "      <OUTPUT DIR>/<STAGE LIB>.stagelib-manifest.json.sha1"
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

extractFileFromJarsOnce() {
  file=${1}
  if [ "${2}" = "required" ]; then
    required="true"
  else
    required="false"
  fi
  jars=${3}
  foundIn=""
  for jar in ${jars}
  do
    unzip -n ${jar} ${file} &> /dev/null
    ret=$?
    if [ "${ret}" = "0" ]; then
      if [ "${foundIn}" == "" ]; then
        foundIn=${jar}
      else
        jar1=$(runEcho "error" basename ${foundIn})
        jar2=$(runEcho "error" basename ${jar})
        fail 1 "(3) ${file} found in ${jar1} and ${jar2}"
      fi
    elif [ "${ret}" != "11" ]; then
      fail ${ret} "(4) Could not look for ${file} in ${jar}"
    fi
  done
  if [ "${required}" = "true" ]; then
    if [ "${foundIn}" = "" ]; then
      fail ${ret} "(5) None of the JARs contains ${file}"
    fi
  fi
}

extractFileFromJarsAll() {
  file=${1}
  postfix=${2}
  jars=${3}
  filePath=$(runEcho "error" dirname ${file})
  fileName=$(runEcho "error" basename ${file})
  if [ "${filePath}" = "." ];  then
    filePath=""
  else
    filePath="${filePath}/"
  fi

  for jar in ${jars}
  do
    unzip -n ${jar} ${file} &> /dev/null
    ret=${?}
    if [ "${ret}" = "0" ]; then
      jarName=$(runEcho "error" basename ${jar})
      run "(9) Could not rename file ${file} to ${filePath}${jarName}-${postfix}" mv ${file} ${filePath}${jarName}-${postfix}
    elif [ "${ret}" != "11" ]; then
      fail ${ret} "(10) Could not look for ${file} in ${jar}"
    fi
  done
}

extractProperty() {
  file=${1}
  propertyKey=${2}
  property=$(runEcho "not-found" grep -e ^${propertyKey}= ${file} | sed "s/^${propertyKey}=//")
  echo ${property}
}

failIf() {
  value1=${1}
  value2=${2}
  msg=${3}
  if [ "${value1}" == "${value2}" ]; then
    fail 1 ${msg}
  fi
}

processStageLib() {
  stagelib=$1
  target=$2
  enforcement=$3

  INITIAL_DIR=`pwd`

  # Verify the stage library file exists and get absolute DIR and NAME for it
  STAGE_LIB_DIR=`dirname ${stagelib}`
  STAGE_LIB_NAME=`basename ${stagelib}`
  run "(12) Directory ${STAGE_LIB_DIR} does not exist" cd ${STAGE_LIB_DIR}
  stagelib=`pwd`/${STAGE_LIB_NAME}
  if [ ! -f ${stagelib} ]; then
    fail 1 "Stage library file ${stagelib} not found"
  fi
  run "(13) Could not go back to ${INITIAL_DIR} directory" cd ${INITIAL_DIR}

  # Verify the target output directory exist, if not set use the stage library directory
  if [ "${target}" = "" ]; then
    target=${STAGE_LIB_DIR}
  fi
  run "(14) Directory ${target} does not exist" cd ${target}
  target=`pwd`
  run "(15) Could not go back to ${INITIAL_DIR} directory" cd ${INITIAL_DIR}

  # Create working directory
  WORK_DIR=${target}/${$}-${STAGE_LIB_NAME}.manifestWork
  run "(16) Could not create ${WORK_DIR} as a work directory" mkdir ${WORK_DIR}

  echo ""
  echo "  Processing ${stagelib} stage library"
  echo "  Output will be at ${target} directory"

  run "(17) Could not go into ${WORK_DIR} directory" cd ${WORK_DIR}

  run "(18) Could not create ${WORK_DIR}/expanded-stagelib as a work directory" mkdir expanded-stagelib

  run "(19) Could not go into ${WORK_DIR}/expanded-stagelib directory" cd expanded-stagelib

  run "(20) Could not expand ${stagelib} tarball" tar xzf ${stagelib}

  JARS=$(runEcho "error" find ${WORK_DIR} -type f -name \*.jar)
  failIf "error" ${JARS} "(21) Could not find the stagelib JARs"

  run "(22) Could not go into ${WORK_DIR} directory" cd ${WORK_DIR}

  extractFileFromJarsOnce data-collector-library-bundle.properties "optional" "${JARS}"

  if [ ! -f data-collector-library-bundle.properties ]; then
    # it is not a stagelib tarball

    if [ "${enforcement}" = "must-do" ]; then
      fail 1 "The ${stagelib} file is not a valid stage library"
    fi

    echo "  Skipped, ${stagelib} is not a stage library"

  else
    # it is a stagelib tarball

    extractFileFromJarsOnce data-collector-library.properties "optional" "${JARS}"

    extractFileFromJarsAll StageDefList.json "stagedef.json" "${JARS}"

    # Create SHA1 for stagelib tarball
    STAGE_LIB_FILE_SHA1="${target}/${STAGE_LIB_NAME}.sha1"

    run "(23) Could not create SHA1 file for ${stagelib}" sha1sum ${stagelib} > ${STAGE_LIB_FILE_SHA1}
    STAGE_LIB_SHA1=$(runEcho "(24) Could not load stagelib SHA1" cat ${STAGE_LIB_FILE_SHA1} | sed 's/ .*$//')
    failIf "error" ${STAGE_LIB_SHA1} "(24) Could not load stagelib SHA1"

    # Write Stage library Manifest
    MANIFEST_FILE="${target}/${STAGE_LIB_NAME}.stagelib-manifest.json"

    STAGE_LIB_FILE=${STAGE_LIB_NAME}
    STAGE_LIB_LABEL=$(extractProperty data-collector-library-bundle.properties library.name)
    STAGE_LIB_ID=$(extractProperty data-collector-library.properties id)
    STAGE_LIB_VERSION=$(extractProperty data-collector-library.properties version)
    STAGE_LIB_LICENSE=$(extractProperty data-collector-library.properties license)
    MIN_SDC_VERSION=$(extractProperty data-collector-library.properties min.sdc.version)

    failIf "not-found" ${STAGE_LIB_ID} "(30) ${STAGE_LIB_FILE} does not define its id"
    failIf "not-found" ${STAGE_LIB_VERSION} "(31) ${STAGE_LIB_VERSION} does not define its version"
    failIf "not-found" ${STAGE_LIB_LICENSE} "(32) ${STAGE_LIB_LICENSE} does not define its license"
    failIf "not-found" ${MIN_SDC_VERSION} "(33) ${MIN_SDC_VERSION} does not define its min.sdc.version"

    run "(25) Could not write stagelib manifest" echo "{" > ${MANIFEST_FILE}
    run "(25) Could not write stagelib manifest" echo "  \"manifest.type\" : \"${MANIFEST_TYPE}\"," >> ${MANIFEST_FILE}
    run "(25) Could not write stagelib manifest" echo "  \"manifest.version\" : \"${MANIFEST_VERSION}\"," >> ${MANIFEST_FILE}
    run "(25) Could not write stagelib manifest" echo "  \"stagelib.id\" : \"${STAGE_LIB_ID}\"," >> ${MANIFEST_FILE}
    run "(25) Could not write stagelib manifest" echo "  \"stagelib.file\" : \"${STAGE_LIB_NAME}\"," >> ${MANIFEST_FILE}
    run "(25) Could not write stagelib manifest" echo "  \"stagelib.file.sha1\" : \"${STAGE_LIB_SHA1}\"," >> ${MANIFEST_FILE}
    run "(25) Could not write stagelib manifest" echo "  \"stagelib.label\" : \"${STAGE_LIB_LABEL}\"," >> ${MANIFEST_FILE}
    run "(25) Could not write stagelib manifest" echo "  \"stagelib.version\" : \"${STAGE_LIB_VERSION}\"," >> ${MANIFEST_FILE}
    run "(25) Could not write stagelib manifest" echo "  \"stagelib.license\" : \"${STAGE_LIB_LICENSE}\"," >> ${MANIFEST_FILE}
    run "(25) Could not write stagelib manifest" echo "  \"stagelib.min.sdc.version\" : \"${MIN_SDC_VERSION}\"," >> ${MANIFEST_FILE}
    run "(25) Could not write stagelib manifest" echo "  \"stages\" : [" >> ${MANIFEST_FILE}
    run "(25) Could not write stagelib manifest" echo "" >> ${MANIFEST_FILE}

    STAGEDEF_FILES=$(runEcho "error" ls *-stagedef.json)
    failIf "error" ${STAGEDEF_FILES} "(26) Could not list stage definition files"

    SEPARATOR=""
    for stageDef in ${STAGEDEF_FILES}; do
      data=$(runEcho "error" cat ${stageDef} | sed '1s/^.//' | sed '$s/.$//' | sed '1s/^ *$//')
      failIf "error" ${data} "(27) Could not trim [] from stage definition"
      if [ "${data}" != "" ]; then
        run "(25) Could not write stagelib manifest" echo "  ${SEPARATOR}" >> ${MANIFEST_FILE}
        run "(25) Could not write stagelib manifest" echo "${data}" >> ${MANIFEST_FILE}
        SEPARATOR=","
      fi
    done

    run "(25) Could not write stagelib manifest" echo "" >> ${MANIFEST_FILE}
    run "(25) Could not write stagelib manifest" echo "  ]" >> ${MANIFEST_FILE}
    run "(25) Could not write stagelib manifest" echo "}" >> ${MANIFEST_FILE}

    # Create SHA1 files for manifest and stagelib

    MANIFEST_FILE_SHA1="${MANIFEST_FILE}.sha1"

    run "(28) Could not create SHA1 file for ${MANIFEST_FILE}" sha1sum ${MANIFEST_FILE} > ${MANIFEST_FILE_SHA1}

    echo "  Manifest and SHA1s files for ${STAGE_LIB_NAME} available at ${target}"

  fi

  run "(29) Could not go back to ${INITIAL_DIR} directory" cd ${INITIAL_DIR}
  cleanUp

}

#
# Script STARTS RUNNING HERE
#

SCRIPT_NAME=$(runEcho "Could not get the name of the script" basename $0)
STAGE_LIB=""
STAGE_LIBS_DIR=""
TARGET=""
KEEP_WORK_DIR="false"

while test ${#} -gt 0; do
  case "${1}" in
    --stagelib)
      shift
      assertOptionValue "--stagelib" ${1}
      STAGE_LIB=${1}
      shift
      ;;
    --stagelibsDir)
      shift
      assertOptionValue "--stagelibsDir" ${1}
      STAGE_LIBS_DIR=${1}
      shift
      ;;
    --output)
      shift
      assertOptionValue "--stagelib" ${1}
      TARGET=${1}
      shift
      ;;
    --keepWorkdir)
      KEEP_WORK_DIR="true"
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

failIf "${STAGE_LIB}${STAGE_LIBS_DIR}" "" "ERROR: One of the following options must be specified: --stagelib, --stagelibsDir"

if [ "${STAGE_LIB}" != "" ] && [ "${STAGE_LIBS_DIR}" != "" ]; then
  echo "ERROR: --stagelib and --stagelibsDir option are exclusive of each other"
  exit 1
fi

if [ "${STAGE_LIB}" != "" ]; then
  processStageLib ${STAGE_LIB} "${TARGET}" "must-do"
fi

if [ "${STAGE_LIBS_DIR}" != "" ]; then
  if [ -d ${STAGE_LIBS_DIR} ]; then
    TARBALLS=$(runEcho "()" find ${STAGE_LIBS_DIR} -name *.tar.gz -depth 1)
    if [ "${TARBALLS}" != "" ]; then
      for TARBALL in ${TARBALLS}; do
        processStageLib ${TARBALL} "${TARGET}" "may-skip"
      done
    fi
  else
    echo "WARN: The directory ${STAGE_LIBS_DIR} does not exist"
  fi
fi

echo ""
exit 0
