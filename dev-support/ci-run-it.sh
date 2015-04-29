#!/bin/bash
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
