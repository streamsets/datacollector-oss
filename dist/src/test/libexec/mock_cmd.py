#!/bin/python

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

# This file should be used when testing to replace some other command like curl with canned output, allowing you to
# mock system interactions

# The first argument is the name of the environment variable with the list of argument regexes to responses. The
# remaining arguments are what would be passed to the real program. Example:
# MOCK_CURL_RESPONSES='[[".*/token$", "MOCK_TOKEN"], [".*/meta-data$", "first"], [".*/meta-data/first$", "firstVal"]]'

# regexes match from the start of the args list (re.match behavior)

import os
import sys
import json
import re

response_pairs = json.loads(os.environ[sys.argv[1]])
to_check = ' '.join(sys.argv[2:])

for pair in response_pairs:
    if re.match(pair[0], to_check):
        print(pair[1])
        sys.stderr.write("DEBUG: returning {ret} for args {args}\n".format(ret=pair[1], args=to_check))
        sys.exit(0)

sys.stderr.write("ERROR: Could not find matching response among {patterns} for args: {args}\n".format(patterns=len(response_pairs), args=to_check))

sys.exit(1)
