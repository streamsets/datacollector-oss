#!/bin/bash
#
# (c) 2014 StreamSets, Inc. All rights reserved. May not
# be copied, modified, or distributed in whole or part without
# written consent of StreamSets, Inc.
#

#export PIPELINE_HOME=/home/pipeline
#export PIPELINE_CONF=/etc/pipeline
#export PIPELINE_DATA=/var/run/pipeline
#export PIPELINE_LOG=/var/log/pipeline

#export PIPELINE_MAIN_CLASS="com.streamsets.pipeline.agent.Main"

export PIPELINE_PRE_CLASSPATH=${PIPELINE_PRE_CLASSPATH}

export PIPELINE_POST_CLASSPATH=${PIPELINE_POST_CLASSPATH}

export PIPELINE_JAVA_OPTS="-Xmx1024m ${PIPELINE_JAVA_OPTS}"
