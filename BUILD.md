<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
--->

[StreamSets](http://streamsets.com)

# Building StreamSets Data Collector

To build the Data Collector you will need the following software :

- Git 1.9+
- JDK 1.8.x       (JDK 1.7.x if not running integration tests)
- Docker 1.10+    (required only if running integration tests, older verisons may work but are not tested.)
- Maven 3.2.3+
- Node 0.10.32+1  (OSX, brew install nodejs       : Linux, curl -sL https://deb.nodesource.com/setup_4.x | sudo -E bash OR sudo apt-get install -y nodejs)
 - npm            (OSX, brew install npm          : Linux, sudo install npm)
 - bower          (OSX, npm -g install bower      : Linux, sudo npm -g install bower)
 - grunt-cli      (OSX, npm -g install grunt-cli  : Linux, sudo npm -g install grunt-cli)
- md5sum          (OSX, brew install md5sha1sum)

Prerequisites for Data Collector :

If you're building master branch, then you need to install API module to your maven cache first before compiling Data Collector. Released versions
are published to public maven repositories and for them this step can be skipped. While Data Collector runs on Java 7, Java 8 is required for
running integration tests.

- You can do that by getting the latest code from github

`git clone http://github.com/streamsets/datacollector-api`

- And install it to maven cache

`mvn clean install -DskipTests`

Follow these instructions to build the Data Collector :

- Get the latest code from github

`git clone http://github.com/streamsets/datacollector`

## Development build

From within the Data Collector directory, execute:

`mvn package -Pdist,ui -DskipTests`

To start the Data Collector, execute:

`dist/target/streamsets-datacollector-2.1.0.0-SNAPSHOT/streamsets-datacollector-2.1.0.0-SNAPSHOT/bin/streamsets dc`

For Data Collector CLI, execute:

`dist/target/streamsets-datacollector-2.1.0.0-SNAPSHOT/streamsets-datacollector-2.1.0.0-SNAPSHOT/bin/streamsets cli`

To skip the RAT report during the build use the `-DskipRat` option.

## Release build

From within the Data Collector directory, execute:

`mvn package -Drelease -DskipTests`

The release tarball will be created at:

`release/target/streamsets-datacollector-all-2.1.0.0-SNAPSHOT.tgz`

Untar the tarball in your prefered location :

`tar xvzf streamsets-datacollector-all-2.1.0.0-SNAPSHOT.tgz`

To start the DataCollector, execute:

`streamsets-datacollector-all-2.1.0.0-SNAPSHOT/bin/streamsets dc`
