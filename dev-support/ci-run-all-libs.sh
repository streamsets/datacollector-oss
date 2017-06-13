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
DEV_SUPPORT=$(cd $(dirname ${BASH_SOURCE}) && /bin/pwd)
. ${DEV_SUPPORT}/ci-default-profile.sh
PROFILE=$(basename ${BASH_SOURCE})
PROFILE=${PROFILE#*-}
PROFILE=${PROFILE%-*}
RUN_TESTS=""
# apply profile here
case "$PROFILE" in
  run-all)
    TEST_OPTS=(-Pall-libs)
    RUN_TESTS="true"
    ;;
  run-min)
    TEST_OPTS=()
    RUN_TESTS="true"
    ;;
  build)
    TEST_OPTS=(-DskipTests)
    ;;
  *)
    echo "Unknown profile $PROFILE" 1>&2
    exit 1
    ;;
esac
env
git log --oneline | head
git show
/opt/scripts/docker-stop-all-running-containers.sh || true
/opt/scripts/docker-delete-stopped-containers.sh || true
/opt/scripts/docker-delete-local-images.sh || true
rm -rf cloudera-integration/parcel/target/cm_ext
# compile and install
mvn clean install -U -Pdist,all-libs,ui,rpm,miniIT,cloudera -Drelease -Dtest=DoesNotExist -DfailIfNoTests=false
# package and run tests (if appropiate)
pushd cloudera-integration/parcel/target
git clone https://github.com/cloudera/cm_ext.git
parcel=$(ls STREAMSETS_DATACOLLECTOR-*.parcel)
# the parcel maven generates cannot be read by python tarfile
# the workaround is to re-generate with guntar
rm -r ${parcel%-el6*}
tar -zxf $parcel
rm $parcel
tar -zcf $parcel ${parcel%-el6*}
# we can use the same parcel for multiple os
ln -s $parcel ${parcel%-el6*}-el7.parcel
ln -s $parcel ${parcel%-el6*}-trusty.parcel
python cm_ext/make_manifest/make_manifest.py .
popd
set +e
export JAVA_HOME=${TEST_JVM}
mvn package -U -fae -Pdist,ui,rpm,miniIT -Dmaven.main.skip=true -DlastModGranularityMs=604800000 ${TEST_OPTS[@]}
exitCode=$?
if [[ -n $PUBLISH_SDC_TAR ]] && [[ $exitCode -eq 0 ]]
then
  /opt/scripts/build-upload-sdc-tar.sh dist/target/streamsets-datacollector-*/streamsets-datacollector-*
fi
if [[ -n $RUN_TESTS ]] && git log --format=%B -n 1 HEAD | grep -q "RUN_IT_TESTS"
then
  set -e
  bash ../infra/dev-support/ci-build-sdc-docker-image.sh
  python -u /opt/scripts/systest-create-docker-environment.py
  bash $DEV_SUPPORT/ci-run-it.sh
fi
exit $exitCode
