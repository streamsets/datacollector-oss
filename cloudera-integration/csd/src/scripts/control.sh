#!/bin/bash

# For better debugging
echo ""
echo "Date: `date`"
echo "Host: `hostname -f`"
echo "Pwd: `pwd`"
echo "CONF_DIR: $CONF_DIR"
echo ""

printenv

export SDC_CONF=$CONF_DIR
chmod 600 $CONF_DIR/local-realm.properties
exec $SDC_HOME/bin/sdc -verbose
