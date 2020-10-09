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

# For better debugging
date 1>&2

CMD=$1

function log {
  timestamp=$(date)
  echo "$timestamp: $1"       #stdout
  echo "$timestamp: $1" 1>&2; #stderr
}

# Generated variables
export DPM_TOKEN_FILE="${DPM_TOKEN_PATH}/applicationToken.txt"

# Dump environmental variables
log "CONF_DIR: $CONF_DIR"
log "SDC_LOG: $SDC_LOG"
log "SDC_DATA: $SDC_DATA"
log "SDC_RESOURCES: $SDC_RESOURCES"
log "CONFIGURED_USERS: $CONFIGURED_USERS"
log "AUTH_TYPE: $AUTH_TYPE"
log "FILE_AUTH_TYPE: $FILE_AUTH_TYPE"
log "LOGIN_MODULE: $LOGIN_MODULE"
log "DPM_TOKEN_PATH: $DPM_TOKEN_PATH"
log "DPM_TOKEN_FILE: $DPM_TOKEN_FILE"
log "DPM_BASE_URL: $DPM_BASE_URL"
log "DPM_USER: $DPM_USER"
log "DPM_PASSWORD: (omitted)"
log "SDC_JAVA_OPTS: $SDC_JAVA_OPTS"
log "SDC_CURL_OPTS: $SDC_CURL_OPTS"
log "CUSTOMER_ID: $CUSTOMER_ID"
log "DEBUG: $DEBUG"
log "Running from: $0"

# Source versioning strings
source $(dirname $0)/buildinfo.sh
log "CSD_VERSION=$CSD_VERSION"
log "CSD_BUILT_BY=$CSD_BUILT_BY"
log "CSD_BUILT_DATE=$CSD_BUILT_DATE"

# If we're in debug mode, enable printing each executed command
if [[ $DEBUG = "true" ]]; then
  set -x
fi

function update_users {
   IFS=';' read -r -a array <<< "$CONFIGURED_USERS"
  # If the auth type is aster, we don't do anything
  if [[ "$FILE_AUTH_TYPE" == "aster" ]]; then
    log "Skip update_users for aster"
  else
    for element in "${array[@]}"; do
      echo "$element" >> "$CONF_DIR"/"$FILE_AUTH_TYPE"-realm.properties
    done
    chmod 600 "$CONF_DIR"/"$FILE_AUTH_TYPE"-realm.properties
  fi
}

function generate_ldap_configs {
  if [ ! -z "$USE_LDAP_FILE_CONFIG" ] && [ "$USE_LDAP_FILE_CONFIG" = true ];then
    log "use.ldap.login.file set to true. Using the already generated content from $CONF_DIR/ldap-login.conf"
  else
    log "use.ldap.login.file set to false. Applying the configurations from ldap.* entries"
    ldap_configs=`cat "$CONF_DIR"/ldap.properties | grep "ldap" | grep -v "ldap.bindPassword" | sed -e 's/ldap\.\([^=]*\)=\(.*\)/  \1=\"\2\"/g'`
    echo "ldap {
    com.streamsets.datacollector.http.LdapLoginModule required
    bindPassword=\"@ldap-bind-password.txt@\"
    contextFactory=\"com.sun.jndi.ldap.LdapCtxFactory\"
    $ldap_configs;
    };
    " > "$CONF_DIR"/ldap-login.conf

    # Append any required additions (such as Kafka JAAS config)
    cat "$CONF_DIR"/generated-ldap-login-append.conf >> "$CONF_DIR"/ldap-login.conf

    # And finally create password file
    ldap_bind_password=`cat "$CONF_DIR"/ldap.properties | grep "ldap.bindPassword"`
    echo "$ldap_bind_password" | awk -F'=' '{ print $2 }' | tr -d '\n' > "$CONF_DIR"/ldap-bind-password.txt
  fi

  if [ ! -z "$LDAP_FILE_SUBSTITUTIONS" ] && [ "$LDAP_FILE_SUBSTITUTIONS" = true ];then
    log "Performing LDAP config file substitutions"
    hostname=`hostname -f`
    sed -i -e "s|_HOST|$hostname|g" -e "s|_KEYTAB_PATH|$CONF_DIR/streamsets.keytab|g" $CONF_DIR/ldap-login.conf
  fi
}

# Prepend content of file specified in $2 to the file specified in $1
function prepend_file_content {
  work_file=$1
  prepend_file=$2

  log "Prepending content from $prepend_file to $work_file"
  cat $prepend_file $work_file > work.tmp
  mv work.tmp $work_file
}

# Create symlinks for standard hadoop services to SDC_RESOURCES directory
function create_config_symlinks {
  # Hadoop
  if [ ! -d $SDC_RESOURCES/hadoop-conf ]; then
    mkdir -p $SDC_RESOURCES/hadoop-conf
    ln -s /etc/hadoop/conf/*.xml $SDC_RESOURCES/hadoop-conf
  fi
  # Hbase
  if [ ! -d $SDC_RESOURCES/hbase-conf ]; then
    mkdir -p $SDC_RESOURCES/hbase-conf
    ln -s /etc/hbase/conf/*.xml $SDC_RESOURCES/hbase-conf
  fi
  # Hive
  if [ ! -d $SDC_RESOURCES/hive-conf ]; then
    mkdir -p $SDC_RESOURCES/hive-conf
    ln -s /etc/hive/conf/*.xml $SDC_RESOURCES/hive-conf
  fi
}

# Make sure that proper redaction configuration is available
# Logic is as such - if user configured it in CM, use it as it is, otherwise load
# default file from the parcel itself.
function support_bundle_redaction_configuration {
  REDACT_CONF="$CONF_DIR/support-bundle-redactor.json"
  if [ -s $REDACT_CONF ] ; then
    log "Found non-empty support-bundle-redactor.json configuration, using it."
  else
    log "Using default parcel file for support-bundle-redactor.json"
    cp $SDC_DIST/etc/support-bundle-redactor.json $REDACT_CONF
  fi
}

# Start SDC (exec into it)
function start {
  log "Starting StreamSets Data Collector"

  # If we have DPM enabled, make sure that this SDC is registered
  if [[ $DPM_ENABLED = "true" ]]; then
    dpm
  fi

  if [[ "$LOGIN_MODULE" = "file" ]]; then
    update_users
  else
    generate_ldap_configs
  fi

  create_config_symlinks

  # Load default configuration from the parcel
  support_bundle_redaction_configuration
  prepend_file_content $CONF_DIR/sdc-security.policy $SDC_DIST/etc/sdc-security.policy
  prepend_file_content $CONF_DIR/sdc-env.sh $SDC_DIST/libexec/sdc-env.sh

  # (Re)Generate customer.id file
  if [[ ! -z $CUSTOMER_ID  ]]; then
    echo $CUSTOMER_ID > $SDC_DATA/customer.id
  fi

  # Source environment (at this point merged the CM config and parcel default)
  source "$CONF_DIR"/sdc-env.sh

  # Finally start SDC
  exec $SDC_DIST/bin/streamsets dc -verbose -skipenvsourcing -exec
}

# Validate that we have all variables that are required for Control Hub (to register and such)
function sch_verify_config {
  die="false"
  log "Validating Control Hub configuration"

  if [ -z "$DPM_BASE_URL" ]; then
    log "Configuration 'dpm.base.url' is not properly set."
    die="true"
  fi
  if [ -z "$DPM_TOKEN_PATH" ]; then
    log "Configuration 'dpm.token.path' is not properly set."
    die="true"
  fi
  if [ -z "$DPM_USER" ]; then
    log "Configuration 'dpm.user' is not properly set."
    die="true"
  fi
}

# Register auth token for this SDC instance in Control Hub
function dpm {
  if [[ -f "$DPM_TOKEN_FILE" || -s "$DPM_TOKEN_FILE" ]]; then
    log "Control Hub token already exists, skipping for now."
    return
  fi

  # Since we know that the token doesn't exists yet, this will always generate new token
  sch_enable
}

# Enable SCH
function sch_enable {
  sch_verify_config

  # Finally enable SCH
  touch $DPM_TOKEN_FILE
  exec $SDC_DIST/bin/streamsets sch register -l $DPM_BASE_URL -u $DPM_USER -p $DPM_PASSWORD --token-file-path $DPM_TOKEN_FILE --skip-config-update
}

# Disable SCH
function sch_disable {
  sch_verify_config

  # Finally disable SCH
  exec $SDC_DIST/bin/streamsets sch unregister -u $DPM_USER -p $DPM_PASSWORD --token-file-path $DPM_TOKEN_FILE --skip-config-update
  rm -rf $DPM_TOKEN_FILE
}

export SDC_CONF=$CONF_DIR

SDC_PROPERTIES=$SDC_CONF/sdc.properties
if [ -f $SDC_PROPERTIES ]; then
  # Default sdc.properties file (from the parcel)
  SDC_PROP_FILE=$SDC_DIST/etc/sdc.properties

  # Propagate system white and black lists
  if ! grep -q "system.stagelibs.*list" $SDC_PROPERTIES; then
    log "System white nor black list found in configuration"
    if [ -f ${SDC_PROP_FILE} ]; then
      log "Propagating default white and black list from parcel"
      line_nums=$(grep -n "system.stagelibs.*list" ${SDC_PROP_FILE} | cut -f1 -d:)
      list_start=$(echo ${line_nums} | cut -f1 -d' ')  # line number of where whitelist starts
      list_end=$(echo ${line_nums} | cut -f2 -d' ')    # line number of where blacklist starts
      blacklist=$(sed "${list_end}q;d" ${SDC_PROP_FILE})
      # Increment list_end while the line ends with '\'
      while [[ $blacklist == *"\\" ]]
      do
          list_end=$((list_end+1))
          blacklist=$(sed "${list_end}q;d" ${SDC_PROP_FILE})
      done
      log "Copying lines from ${list_start} to ${list_end} in $SDC_PROP_FILE to $SDC_PROPERTIES"
      sed -n "${list_start},${list_end}p" ${SDC_PROP_FILE} >> $SDC_PROPERTIES
    else
      log "Parcel doesn't contain default configuration file, skipping white/black list propagation"
    fi
  fi

  # Propagate alias configuration from the parcel. We don't offer safety valve for this one
  # as it's extremely unlikely that someone would need to change those.
  grep "^stage.alias.streamsets" $SDC_PROP_FILE >> $SDC_PROPERTIES
  grep "^library.alias.streamsets" $SDC_PROP_FILE >> $SDC_PROPERTIES

  # Detect if this is a Control Hub enabled deployment
  if grep -q "dpm.enabled=true" $SDC_PROPERTIES; then
    log "Detected Control Hub environment"
    DPM_ENABLED="true"
  else
    log "Running in non-Control Hub environment"
  fi

  # CM exposes Control Hub token config as path to file, so we need to convert it to
  # the actual value that is expected by SDC. We will append it to the config
  # only if we're actually running with Control Hub enabled, otherwise SDC can fail
  # to start if given file doesn't exists.
  if [[ $DPM_ENABLED = "true" ]]; then
    echo "dpm.appAuthToken=@$DPM_TOKEN_FILE@" >> $SDC_PROPERTIES
  fi

fi

log "Executing command '$CMD'"
case $CMD in
  start)
    start
    ;;

  update_users)
    update_users
    exit 0
    ;;

  sch_enable)
    sch_enable
    exit 0
    ;;

  sch_disable)
    sch_disable
    exit 0
    ;;
esac
