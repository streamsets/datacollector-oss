#!/bin/bash

#
# Copyright 2020 StreamSets Inc.
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

# set -x

PRG="${0}"

while [ -h "${PRG}" ]; do
  ls=$(ls -ld "${PRG}")
  link=$(expr "$ls" : '.*-> \(.*\)$')
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=$(dirname "${PRG}")/"$link"
  fi
done

prg_dir="$(dirname "${PRG}")"

source "$prg_dir/_test_util"

cache_file="$target_dir/.cloud_provider_cache"

assert_cache_contains() {
  assert_file_contains "$cache_file" "$@"
}

assert_cache_not_contains() {
  assert_file_not_contains "$cache_file" "$@"
}

invoke_sys_info_util() {
  # log to stderr so caller can parse stdout without issue
  >&2 echo Invoking _sys_info_util with "$@"
  assert "invocation failed" /bin/bash "$libexec_main_dir/_sys_info_util" "$@" 2>/dev/null
}

reset_test() {
  export SDC_DATA="$target_dir"
  export SDC_SYS_INFO_UTIL_METADATA_WAIT=3
  unset SDC_SYS_INFO_CURL_TEST_OVERRIDE
  unset SKIP_EC2_HYPERVISOR_CHECK
  if [[ -e "$cache_file" ]] ; then
    rm "$cache_file"
  fi
}

valid_ip_pattern='[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}'

reset_test
cloud_provider="$(invoke_sys_info_util get_cloud_provider)"
assert_matches "invalid cloud provider" '^[a-zA-Z\\-]{3,255}$' "$cloud_provider"
assert_file_contains "$cache_file" "$cloud_provider"
assert_file_contains "$cache_file" "$valid_ip_pattern"

private_ip="$(invoke_sys_info_util get_private_ip)"
assert_matches "invalid private ip" "^$valid_ip_pattern$" "$private_ip"

echo "Test changing the cache file's cloud provider"
echo "test-cloud_provider" > "$cache_file"
echo "$private_ip" >> "$cache_file"

result="$(invoke_sys_info_util get_cloud_provider)"
assert_matches "invalid cloud provider" "$result" 'test-cloud_provider'

echo "Test illegal ip to cause cache to reset"
echo "test-cloud_provider" > "$cache_file"
echo "333.333.333.333" >> "$cache_file"
result="$(invoke_sys_info_util get_cloud_provider)"
assert_matches "cloud provider doesn't match original" "$cloud_provider" "$result"
assert_file_contains "$cache_file" "$cloud_provider"
assert_file_contains "$cache_file" "$private_ip"

reset_test
export SKIP_EC2_HYPERVISOR_CHECK=true
echo "Test aws detection when azure throws 404"
export MOCK_CURL_RESPONSES="$(cat <<END
[
  [".*/api/token ",            "MY_TOKEN"],
  [".*/meta-data/local-ipv4$", "1.2.3.4"],
  [".*/privateIpAddress\\\\?api-version=.*&format=text$", "<?xml blah>\n<html>\n <head>\n  <title>404 - Not Found</title>\n </head>\n</html>\n"]
]
END
)"
export SDC_SYS_INFO_CURL_TEST_OVERRIDE="python $prg_dir/mock_cmd.py MOCK_CURL_RESPONSES"
result="$(CLD_PROVIDER="azure" invoke_sys_info_util get_private_ip)"
assert_equals "Expected empty ip address due to 404" "" "$result"
result="$(CLD_PROVIDER="aws" invoke_sys_info_util get_private_ip)"
assert_equals "Wrong IP for mocked aws" "1.2.3.4" "$result"
assert "Should be no cache file" [ ! -e "$cache_file" ]
result="$(invoke_sys_info_util get_cloud_provider)"
assert_equals "Wrong cloud provider" "aws" "$result"

reset_test
export SKIP_EC2_HYPERVISOR_CHECK=true
echo "Test azure detection when aws throws 404"
export MOCK_CURL_RESPONSES="$(cat <<END
[
  [".*/api/token ",            "<?xml blah>\n<html>\n <head>\n  <title>404 - Not Found</title>\n </head>\n</html>\n"],
  [".*/meta-data/local-ipv4$", "<?xml blah>\n<html>\n <head>\n  <title>404 - Not Found</title>\n </head>\n</html>\n"],
  [".*/privateIpAddress\\\\?api-version=.*&format=text$", "2.3.4.5"]
]
END
)"
export SDC_SYS_INFO_CURL_TEST_OVERRIDE="python $prg_dir/mock_cmd.py MOCK_CURL_RESPONSES"
result="$(CLD_PROVIDER="aws" invoke_sys_info_util get_private_ip)"
assert_equals "Expected empty ip address due to 404" "" "$result"
result="$(CLD_PROVIDER="azure" invoke_sys_info_util get_private_ip)"
assert_equals "Wrong IP for mocked azure" "2.3.4.5" "$result"
assert "Should be no cache file" [ ! -e "$cache_file" ]
result="$(invoke_sys_info_util get_cloud_provider)"
assert_equals "Wrong cloud provider" "azure" "$result"

reset_test
echo "Test simple aws metadata"
export MOCK_CURL_RESPONSES="$(cat <<END
[
  [".*/token$", "MOCK_TOKEN"],
  [".*/meta-data$", "first"],
  [".*/meta-data/first$", "firstVal"]
]
END
)"
export SDC_SYS_INFO_CURL_TEST_OVERRIDE="python $prg_dir/mock_cmd.py MOCK_CURL_RESPONSES"
export CLD_PROVIDER="aws"
result="$(invoke_sys_info_util get_cloud_metadata)"
assert_matches "mock metadata incorrect" "firstVal" "$result"

reset_test
echo "Test simple aws metadata"
export MOCK_CURL_RESPONSES="$(cat <<END
[
  [".*/token$",           "MOCK_TOKEN"],
  [".*/meta-data$",       "first"],
  [".*/meta-data/first$", "firstVal"]
]
END
)"
export SDC_SYS_INFO_CURL_TEST_OVERRIDE="python $prg_dir/mock_cmd.py MOCK_CURL_RESPONSES"
export CLD_PROVIDER="aws"
result="$(invoke_sys_info_util get_cloud_metadata)"
expected=$(cat <<END
{
  "first":"firstVal"
}
END
)
assert_equals "mock metadata incorrect" "$expected" "$result"

reset_test
echo "Test nested aws metadata"
export MOCK_CURL_RESPONSES="$(cat <<END
[
  [".*/token$",                                "MOCK_TOKEN"],
  [".*/meta-data$",                            "first\nsubtree/"],
  [".*/meta-data/first$",                      "firstVal"],
  [".*/meta-data/subtree/$",                   "innerProp\ninnerTree/"],
  [".*/meta-data/subtree/innerProp$",          "innerVal"],
  [".*/meta-data/subtree/innerTree/$",         "deepProp"],
  [".*/meta-data/subtree/innerTree/deepProp$", "deepVal"]
]
END
)"
export SDC_SYS_INFO_CURL_TEST_OVERRIDE="python $prg_dir/mock_cmd.py MOCK_CURL_RESPONSES"
export CLD_PROVIDER="aws"
result="$(invoke_sys_info_util get_cloud_metadata)"
expected=$(cat <<END
{
  "first":"firstVal",
  "subtree":{
    "innerProp":"innerVal",
    "innerTree":{
      "deepProp":"deepVal"
    }
  }
}
END
)
assert_equals "mock metadata incorrect" "$expected" "$result"

reset_test
echo "Test aws metadata redaction"
export MOCK_CURL_RESPONSES="$(cat <<END
[
  [".*/token$",                        "MOCK_TOKEN"],
  [".*/meta-data$",                    "first\nredactThisKey\nnoSecretGoesThrough\npasswordToRedact\nsubtree/"],
  [".*/meta-data/first$",              "firstVal"],
  [".*/meta-data/redactThisKey$",       "SHOULD NOT BE CALLED"],
  [".*/meta-data/noSecretGoesThrough$", "SHOULD NOT BE CALLED"],
  [".*/meta-data/passwordToRedact$",    "SHOULD NOT BE CALLED"],
  [".*/meta-data/subtree/$",           "innerProp\ninnerTreeCredentials/"],
  [".*/meta-data/subtree/innerProp$",  "innerVal"],
  [".*/meta-data/subtree/innerTreeCredentials/$", "SHOULD NOT BE CALLED"]
]
END
)"
export SDC_SYS_INFO_CURL_TEST_OVERRIDE="python $prg_dir/mock_cmd.py MOCK_CURL_RESPONSES"
export CLD_PROVIDER="aws"
result="$(invoke_sys_info_util get_cloud_metadata)"
expected=$(cat <<END
{
  "first":"firstVal",
  "redactThisKey":"**REDACTED**",
  "noSecretGoesThrough":"**REDACTED**",
  "passwordToRedact":"**REDACTED**",
  "subtree":{
    "innerProp":"innerVal",
    "innerTreeCredentials/":"**REDACTED**"
  }
}
END
)
assert_equals "mock metadata incorrect" "$expected" "$result"

echo "Test aws endpoints with JSON values"
export MOCK_CURL_RESPONSES=$(cat <<END
[
  [".*/token$",           "MOCK_TOKEN"],
  [".*/meta-data$",       "list\nmap"],
  [".*/meta-data/list$",  "[\n  \"listElem\",\n  {\n    \"listMapKey\":\"notRedacted\"\n  }\n]"],
  [".*/meta-data/map$",   "{\n  \"mapKey\":\"notRedacted\",\n  \"mapKey2\":\"notRedacted\"\n}"]
]
END
)
export SDC_SYS_INFO_CURL_TEST_OVERRIDE="python $prg_dir/mock_cmd.py MOCK_CURL_RESPONSES"
export CLD_PROVIDER="aws"
result="$(invoke_sys_info_util get_cloud_metadata)"
# note that the keys are not redacted, even though they have "key", because they are values returned by the metadata
# endpoint. The script only filters which endpoints it fetches from, and it is the responsibility of the caller to do
# more thorough redaction on the full metadata.
expected=$(cat <<END
{
  "list":[
    "listElem",
    {
      "listMapKey":"notRedacted"
    }
  ],
  "map":{
    "mapKey":"notRedacted",
    "mapKey2":"notRedacted"
  }
}
END
)
assert_equals "mock metadata incorrect" "$expected" "$result"

end_test
