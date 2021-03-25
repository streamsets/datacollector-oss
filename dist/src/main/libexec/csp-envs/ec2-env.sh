#!/bin/bash
#
# Copyright 2021 StreamSets Inc.
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

if [[ ! -x "$(which aws)" ]]; then
  echo "AWS CLI is needed to run EC2 Deployment Bootstrap"
  exit -1
fi

http_count=0
for http_count in {0..2}; do
  http_code=$(curl --silent --output /dev/null --write-out "%{http_code}" http://169.254.169.254/latest/user-data --connect-timeout 10)
  if [ "200" = "${http_code}" ]; then
    break
  else
    sleep $(expr 2 + $http_count \* 2)
  fi
done

if [ "200" != "${http_code}" ]; then
  echo "Could not reach AWS Metadata Service"
  exit -1
fi

do_source() {
  data=$(curl --silent http://169.254.169.254/latest/user-data --connect-timeout 10)

  local IFS=$'\n'
  data=($data)

  for value in ${data[@]}; do
    local IFS='='
    pair=($value)
    if [ "${pair[0]}" = "STREAMSETS_DEPLOYMENT_SCH_URL" ]; then
      export STREAMSETS_DEPLOYMENT_SCH_URL="${pair[1]}"
    elif [ "${pair[0]}" = "STREAMSETS_DEPLOYMENT_ID" ]; then
      export STREAMSETS_DEPLOYMENT_ID="${pair[1]}"
    elif [ "${pair[0]}" = "STREAMSETS_DEPLOYMENT_TOKEN_PARAM_NAME" ]; then
      region=$(curl --silent http://169.254.169.254/latest/meta-data/placement/region --connect-timeout 10)
      export STREAMSETS_DEPLOYMENT_TOKEN=$(aws ssm get-parameter --region "$region" --name "${pair[1]}" --with-decryption --query Parameter.Value | tr -d '"')
    fi
  done
}
do_source
