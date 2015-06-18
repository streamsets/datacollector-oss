#!/bin/bash
set -e
set -x
DEV_SUPPORT=$(cd $(dirname ${BASH_SOURCE}) && /bin/pwd)
. ${DEV_SUPPORT}/ci-default-profile.sh
PROFILE=$(basename ${BASH_SOURCE})
PROFILE=${PROFILE#*-}
PROFILE=${PROFILE%%-*}
RUN_TESTS=""
# apply profile here
case "$PROFILE" in
  run)
    PACKAGE_OPTS=()
    RUN_TESTS="true"
    ;;
  build)
    PACKAGE_OPTS=(-DskipTests)
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
# compile and install
mvn clean install -Pdist,all-libs,ui,rpm -Drelease -Dtest=DoesNotExist -DfailIfNoTests=false
# package and run tests (if appropiate)
set +e
export JAVA_HOME=${TEST_JVM}
mvn package -fae -Pdist,all-libs,ui,rpm,miniIT -Drelease -Dmaven.main.skip=true -DlastModGranularityMs=604800000 ${PACKAGE_OPTS[@]}
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
