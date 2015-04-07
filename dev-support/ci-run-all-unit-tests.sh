#!/bin/bash
set -e
export JAVA_HOME=/opt/jdk-7
export M2_HOME=/opt/maven-3.2
export NODE_HOME=/opt/node-0.10
export PATH=$JAVA_HOME/bin:$M2_HOME/bin:$MAVEN_HOME/bin:$NODE_HOME/bin:$PATH
set -x
env
git log --oneline | head
git show
mvn clean install -Drelease -DskipTests
mvn package -Drelease -fae -Pall-libs
