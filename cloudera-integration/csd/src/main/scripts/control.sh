#!/bin/bash
#
#
# Licensed under the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
#

# For better debugging
date 1>&2

CMD=$1

function log {
  timestamp=$(date)
  echo "$timestamp: $1"       #stdout
  echo "$timestamp: $1" 1>&2; #stderr
}

# Dump environmental variables
log "CONF_DIR: $CONF_DIR"
log "SDC_LOG: $SDC_LOG"
log "SDC_DATA: $SDC_DATA"
log "SDC_RESOURCES: $SDC_RESOURCES"
log "CONFIGURED_USERS: $CONFIGURED_USERS"
log "AUTH_TYPE: $AUTH_TYPE"
log "FILE_AUTH_TYPE: $FILE_AUTH_TYPE"
log "LOGIN_MODULE: $LOGIN_MODULE"
log "DPM_TOKEN_FILE: $DPM_TOKEN_FILE"
log "DPM_BASE_URL: $DPM_BASE_URL"
log "DPM_USER: $DPM_USER"
log "DPM_PASSWORD: (omitted)"
log "SDC_JAVA_OPTS: $SDC_JAVA_OPTS"
log "SDC_CURL_OPTS: $SDC_CURL_OPTS"
log "DPM_TOKEN_REGENERATE: $DPM_TOKEN_REGENERATE"
log "DEBUG: $DEBUG"

# If we're in debug mode, enable printing each executed command
if [[ $DEBUG = "true" ]]; then
  set -x
  CURL_DEBUG="-v"
fi

function update_users {
  IFS=';' read -r -a array <<< "$CONFIGURED_USERS"
  for element in "${array[@]}"; do
    echo "$element" >> "$CONF_DIR"/"$FILE_AUTH_TYPE"-realm.properties
  done
  chmod 600 "$CONF_DIR"/"$FILE_AUTH_TYPE"-realm.properties
}

function generate_ldap_configs {
  ldap_configs=`cat "$CONF_DIR"/ldap.properties | grep "ldap" | grep -v "ldap.bindPassword" | sed -e 's/ldap\.\([^=]*\)=\(.*\)/  \1=\"\2\"/g'`
  echo "ldap {
  com.streamsets.datacollector.http.LdapLoginModule required
  bindPassword=\"@ldap-bind-password.txt@\"
  contextFactory=\"com.sun.jndi.ldap.LdapCtxFactory\"
$ldap_configs;
};" > "$CONF_DIR"/ldap-login.conf
  ldap_bind_password=`cat "$CONF_DIR"/ldap.properties | grep "ldap.bindPassword"`
  echo "$ldap_bind_password" | awk -F'=' '{ print $2 }' | tr -d '\n' > "$CONF_DIR"/ldap-bind-password.txt
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

  source "$CONF_DIR"/sdc-env.sh
  exec $SDC_DIST/bin/streamsets dc -verbose -skipenvsourcing -exec
}

# Execute CURL and die the script if the CURL execution failed
function run_curl {
  method=$1
  url=$2
  payload=$3
  extraArgs=$4

  log "Executing curl $method to $url"
  output=`curl $CURL_DEBUG $SDC_CURL_OPTS -S -X "$method" -d "$payload" "$url" -H 'Content-Type:application/json' -H 'X-Requested-By:SDC' $extraArgs`
  if [ $? -ne 0 ]; then
    log "Failed $method to $url"
    exit 1
  fi
}

# Print out DPM's login URL
function dpm_url_login {
  echo ${DPM_BASE_URL}/security/public-rest/v1/authentication/login
}

# Print out DPM's token generation URL
function dpm_url_token_gen {
  echo ${DPM_BASE_URL}/security/rest/v1/organization/$dpmOrg/components
}

# Print out DPM's token regeneration URL
function dpm_url_token_regen {
  echo ${DPM_BASE_URL}/security/rest/v1/organization/$dpmOrg/components/regenerateAuthToken
}

# Print out DPM's logout URL
function dpm_url_logout {
  echo ${DPM_BASE_URL}/security/_logout
}

# Create DPM session and sets in in shared variable dpmSession
function dpm_login {
  log "Creating DPM session"
  run_curl "POST" "$(dpm_url_login)" "{\"userName\":\"$DPM_USER\", \"password\": \"$DPM_PASSWORD\"}" "-D -"
  # Session is stored in SS_SSO_LOGIN header and needs to be extracted
  dpmSession=$(echo $output | grep SS-SSO-LOGIN | sed -e 's/[^=]*=//' -e 's/;.*//')

  if [ -z "$dpmSession" ]; then
    log "Can't open DPM session: $output"
    exit 1
  fi

  log "Opened DPM session: $dpmSession"
}

# Log out DPM session
function dpm_logout {
  log "Logging out session: $dpmSession"
  run_curl "GET" "$(dpm_url_logout)" "" "--header X-SS-User-Auth-Token:$dpmSession"
}

# Generate new DPM application token and fill it into token variable
function dpm_generate_token {
  log "Generating new token in session $dpmSession and in org $dpmOrg"
  run_curl "PUT" "$(dpm_url_token_gen)" "{\"organization\": \"$dpmOrg\", \"componentType\" : \"dc\", \"numberOfComponents\" : 1, \"active\" : true}" "-H X-SS-REST-CALL:true -H X-SS-User-Auth-Token:$dpmSession"
  token=$(echo $output | sed -e 's/.*"fullAuthToken":"//' -e 's/".*//')

  if [ -z "$token" ]; then
    log "Can't generate new token"
    exit 1
  fi
}

# Regenerate existing DPM application token and fill it into token variable
function dpm_regenerate_token {
  log "Regenerating token in session $dpmSession and in org $dpmOrg"

  sdcId=`cat $SDC_DATA/sdc.id`
  if [ -z "$sdcId" ]; then
    log "Can't load SDC id"
    exit 1
  fi

  log "SDC id is $sdcId"

  run_curl "POST" "$(dpm_url_token_regen)" "[ \"$sdcId\" ]" "-H X-SS-REST-CALL:true -H X-SS-User-Auth-Token:$dpmSession"
  token=$(echo $output | sed -e 's/.*"fullAuthToken":"//' -e 's/".*//')

  if [ -z "$token" ]; then
    log "Can't generate new token"
    exit 1
  fi
}


# Validate that we have all variables that are required for DPM (to register and such)
function dpm_verify_config {
  die="false"
  log "Validating DPM configuration"

  if [ -z "$DPM_BASE_URL" ]; then
    log "Configuration 'dpm.base.url' is not properly set."
    die="true"
  fi
  if [ -z "$DPM_TOKEN_FILE" ]; then
    log "Configuration 'dpm.token.path' is not properly set."
    die="true"
  fi
  if [ -z "$DPM_USER" ]; then
    log "Configuration 'dpm.user' is not properly set."
    die="true"
  fi

  # Calculate DPM organization
  dpmOrg=`echo $DPM_USER | cut -f2 -d@`
  if [ -z "$dpmOrg" ]; then
    log "Configuration 'dpm.user' doesn't properly contain organization."
    die="true"
  fi
  log "DPM organization is $dpmOrg"

  if [ $die = "true" ]; then
    log "Invalid configuration for DPM"
    exit 1
  fi
}

# Register auth token for this SDC instance in DPM
function dpm {
  if [[ -f $DPM_TOKEN_FILE ]]; then
    echo "DPM token already exists, skipping for now."
    return
  fi

  # Since we know that the token doesn't exists yet, this will always generate new token
  dpm_generate_or_regenerate
}

# Either generate new or re-generate existing DPM token
function dpm_generate_or_regenerate {
  dpm_verify_config
  dpm_login

  # Based on whether the token file already exists
  if [ -f $DPM_TOKEN_FILE ]; then
    dpm_regenerate_token
    echo $token > $DPM_TOKEN_FILE
  else
    # Application token doesn't exists yet, we need to generate it
    dpm_generate_token
    mkdir -p "$(dirname $DPM_TOKEN_FILE)"
    echo $token > $DPM_TOKEN_FILE
  fi

  dpm_logout
}

export SDC_CONF=$CONF_DIR

SDC_PROPERTIES=$SDC_CONF/sdc.properties
if [ -f $SDC_PROPERTIES ]; then
  # Propagate system white and black lists
  if ! grep -q "system.stagelibs.*list" $SDC_PROPERTIES; then
    echo "System white nor black list found in configuration"
    SDC_PROP_FILE=$SDC_DIST/etc/sdc.properties
    if [ -f ${SDC_PROP_FILE} ]; then
      echo "Propagating default white and black list from parcel"
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
      echo "Copying lines from ${list_start} to ${list_end} in $SDC_PROP_FILE to $SDC_PROPERTIES"
      sed -n "${list_start},${list_end}p" ${SDC_PROP_FILE} >> $SDC_PROPERTIES
    else
      echo "Parcel doesn't contain default configuration file, skipping white/black list propagation"
    fi
  fi

  # Detect if this is a DPM enabled deployment
  if grep -q "dpm.enabled=true" $SDC_PROPERTIES; then
    log "Detected DPM environment"
    DPM_ENABLED="true"
  else
    log "Running in non-DPM environment"
  fi

  # CM exposes DPM token config as path to file, so we need to convert it to
  # the actual value that is expected by SDC. We will append it to the config
  # only if we're actually running with DPM enabled, otherwise SDC can fail
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

  dpm)
    dpm_generate_or_regenerate
    exit 0
    ;;
esac
