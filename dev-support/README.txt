====
    Copyright 2017 StreamSets Inc.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
====


Profile scripts

These scripts setup the environment for the other build scripts.
Note that the java* versions are just symlinks to the default profile.

* ci-default-profile.sh
* ci-java7-profile.sh
* ci-java8-profile.sh

Scripts used to execute the build and/or test.

Note that the build script is just a symlink to the run script.

* ci-build-all-libs.sh
* ci-run-all-libs.sh
* ci-run-all-unit-tests.sh (depcrecated)
