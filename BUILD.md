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

# Building StreamSets Data Collector

To build the Data Collector you will need the following software :

- Git 1.9+
- Oracle JDK 8
- Docker 1.10+    (required only if running integration tests, older versions may work but are not tested.)
- Maven 3.3.9+
- Node 0.10.32+1  (macOS, `brew install nodejs`       : Linux, [nodejs.org](https://nodejs.org) or [Packages from NodeSource](https://github.com/nodesource/distributions))
 - npm            (macOS, `brew install nodejs`       : Linux, [nodejs.org](https://nodejs.org) or [Packages from NodeSource](https://github.com/nodesource/distributions))
 - bower          (macOS, `npm -g install bower`      : Linux, `sudo npm -g install bower`)
 - grunt-cli      (macOS, `npm -g install grunt-cli`  : Linux, `sudo npm -g install grunt-cli`)
- md5sum          (macOS, `brew install md5sha1sum`)

Prerequisites for Data Collector :

If you're building master branch, then you need to install API and Plugin API modules to your maven cache first before compiling Data Collector. Released versions
are published to public maven repositories and for them this step can be skipped. While Data Collector runs on Java 7, Java 8 is required for
running integration tests.

- You can do that by getting the latest code from Github

`git clone http://github.com/streamsets/datacollector-api`

and

`git clone http://github.com/streamsets/datacollector-plugin-api`

- And install each of these to your local maven repository

`mvn clean install -DskipTests`

Follow these instructions to build the Data Collector :

- Get the latest code from Github

`git clone http://github.com/streamsets/datacollector`

## Development build

From within the Data Collector directory, execute:

`mvn package -Pdist,ui -DskipTests`

To start the Data Collector, execute:

`dist/target/streamsets-datacollector-3.0.0.0-SNAPSHOT/streamsets-datacollector-3.0.0.0-SNAPSHOT/bin/streamsets dc`

For Data Collector CLI, execute:

`dist/target/streamsets-datacollector-3.0.0.0-SNAPSHOT/streamsets-datacollector-3.0.0.0-SNAPSHOT/bin/streamsets cli`

To skip the RAT report during the build use the `-DskipRat` option.

## Release build

From within the Data Collector directory, execute:

`mvn package -Drelease -DskipTests`

The release tarball will be created at:

`release/target/streamsets-datacollector-all-3.0.0.0-SNAPSHOT.tgz`

Extract the tarball to your preferred location :

`tar xf streamsets-datacollector-all-3.0.0.0-SNAPSHOT.tgz`

To start the DataCollector, execute:

`streamsets-datacollector-all-3.0.0.0-SNAPSHOT/bin/streamsets dc`
