#!/bin/bash
#
# (c) 2014 StreamSets, Inc. All rights reserved. May not
# be copied, modified, or distributed in whole or part without
# written consent of StreamSets, Inc.
#

#export SDC_HOME=/home/sdc
#export SDC_CONF=/etc/sdc
#export SDC_DATA=/var/run/sdc
#export SDC_LOG=/var/log/sdc

#export SDC_MAIN_CLASS="com.streamsets.sdc.agent.Main"

export SDC_PRE_CLASSPATH=${SDC_PRE_CLASSPATH}

export SDC_POST_CLASSPATH=${SDC_POST_CLASSPATH}

export SDC_JAVA_OPTS="-Xmx1024m ${SDC_JAVA_OPTS}"
