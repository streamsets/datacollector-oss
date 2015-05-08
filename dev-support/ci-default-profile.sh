#!/bin/bash

# this allows people to override the default profile for build scripts
if [[ -n "$CI_PROFILE_APPLIED" ]]
then
  return 0
fi
export CI_PROFILE_APPLIED=1


# define variables here
export JAVA7_HOME=/opt/jdk-7
export JAVA8_HOME=/opt/jdk-8
export M2_HOME=/opt/maven-3.2
export NODE_HOME=/opt/node-0.10

PROFILE=$(basename ${BASH_SOURCE})
PROFILE=${PROFILE%-*}
PROFILE=${PROFILE#*-}

# apply profile here
case "$PROFILE" in
  java8)
    export TEST_JVM=${JAVA8_HOME}
    ;;
  *)
    export TEST_JVM=${JAVA7_HOME}
    ;;
esac
export JAVA_HOME=${JAVA8_HOME}
export PATH=${JAVA_HOME}/bin:${M2_HOME}/bin:${MAVEN_HOME}/bin:${NODE_HOME}/bin:$PATH
echo $PATH
