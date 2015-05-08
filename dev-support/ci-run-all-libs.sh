#!/bin/bash
set -e
set -x
DEV_SUPPORT=$(cd $(dirname ${BASH_SOURCE}) && /bin/pwd)
. ${DEV_SUPPORT}/ci-default-profile.sh
PROFILE=$(basename ${BASH_SOURCE})
PROFILE=${PROFILE#*-}
PROFILE=${PROFILE%%-*}
# apply profile here
case "$PROFILE" in
  run)
    PACKAGE_OPTS=()
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
export JAVA_HOME=${TEST_JVM}
mvn test -fae -Pdist,all-libs,ui,rpm -Drelease -Dmaven.main.skip=true -DlastModGranularityMs=604800000 ${PACKAGE_OPTS[@]}
