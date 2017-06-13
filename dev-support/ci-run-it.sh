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

set -x
cd $(dirname ${BASH_SOURCE})
sdcSource=$PWD/../python/sdc-cli/
if [[ ! -d $sdcSource ]]
then
  echo "ERROR: Cannot find sdc source" 1>&2
  exit 1
fi
# install sdc
. /usr/local/bin/virtualenvwrapper.sh
venv=sdc-$(date +%Y%m%d-%H%M)
mkvirtualenv --system-site-packages $venv
workon $venv
set -e
pushd ../integration-testing/
pip install $sdcSource
python -c 'from sdc import api'
python $(which nosetests) -x --with-xunit --verbosity=3
