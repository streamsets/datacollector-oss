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
 - bower 1.8.2    (macOS, `npm -g install bower`      : Linux, `sudo npm -g install bower`)
 - grunt-cli      (macOS, `npm -g install grunt-cli`  : Linux, `sudo npm -g install grunt-cli`)
- md5sum          (macOS, `brew install md5sha1sum`)

## Prerequisite Tasks for Building Data Collector

If you're building master branch, then you need to install API and Plugin API modules to your maven cache first before compiling Data Collector. Released versions
are published to public maven repositories and for them this step can be skipped.

- You can do that by getting the latest code from Github

  `git clone http://github.com/streamsets/datacollector-api`

  and

  `git clone http://github.com/streamsets/datacollector-plugin-api`

- And install each of these to your local maven repository

  `mvn clean install -DskipTests`

You also need the artifacts for datacollector-edge installed into your local maven repository.

- Ensure you have the [prerequisites listed for building datacollector-edge](https://github.com/streamsets/datacollector-edge/blob/master/BUILD.md#minimum-requirements)

- Get the latest version of datacollector-edge from Github

  `git clone https://github.com/streamsets/datacollector-edge.git`

- Gradle is used in this project as a build tool, so in order to install it to your local maven repository execute:

  `./gradlew clean dist publishToMavenLocal`

Finally, get the latest Data Collector code from Github

`git clone http://github.com/streamsets/datacollector`

## Development build

From within the Data Collector directory, execute:

`mvn package -Pdist,ui -DskipTests`

To start the Data Collector, execute:

`dist/target/streamsets-datacollector-3.7.0-SNAPSHOT/streamsets-datacollector-3.7.0-SNAPSHOT/bin/streamsets dc`

For Data Collector CLI, execute:

`dist/target/streamsets-datacollector-3.7.0-SNAPSHOT/streamsets-datacollector-3.7.0-SNAPSHOT/bin/streamsets cli`

To skip the RAT report during the build use the `-DskipRat` option.

## Running integration tests

From within the Data Collector directory, execute:

`mvn install -Pdist -DskipTests`

Once the dependencies are installed, run the integration tests:

`mvn failsafe:integration-test -DfailIfNoTests=false`

In case you want to run a specific integration class (here the module basic-lib is executed with integration tests in 'HttpProcessorIT'):

`mvn -pl basic-lib failsafe:integration-test -Dit.test="HttpProcessorIT" -DfailIfNoTests=false`

## Release build

From within the Data Collector directory, execute:

`mvn package -Drelease -DskipTests`

The release tarball will be created at:

`release/target/streamsets-datacollector-all-3.7.0-SNAPSHOT.tgz`

Extract the tarball to your preferred location :

`tar xf streamsets-datacollector-all-3.7.0-SNAPSHOT.tgz`

To start the DataCollector, execute:

`streamsets-datacollector-all-3.7.0-SNAPSHOT/bin/streamsets dc`

## Troubleshooting

Recent changes in Node might cause the build to fail with an error similar to:

    [INFO] --------------------------------------
    [INFO]          BOWER INSTALL
    [INFO] --------------------------------------
    [INFO] grunt version :
    grunt-cli v1.2.0
    grunt v0.4.5
    [INFO] --------------------------------------
    [INFO]          GRUNT TEST --NO-COLOR
    [INFO] --------------------------------------
    grunt[12558]: ../src/node_contextify.cc:629:static void node::contextify::ContextifyScript::New(const FunctionCallbackInfo<v8::Value> &): Assertion `args[1]->IsString()' failed.
     1: node::Abort() [/usr/local/bin/node]
     2: node::MakeCallback(v8::Isolate*, v8::Local<v8::Object>, char const*, int, v8::Local<v8::Value>*, node::async_context) [/usr/local/bin/node]
     3: node::contextify::ContextifyScript::New(v8::FunctionCallbackInfo<v8::Value> const&) [/usr/local/bin/node]
     4: v8::internal::FunctionCallbackArguments::Call(v8::internal::CallHandlerInfo*) [/usr/local/bin/node]
     5: v8::internal::MaybeHandle<v8::internal::Object> v8::internal::(anonymous namespace)::HandleApiCallHelper<true>(v8::internal::Isolate*, v8::internal::Handle<v8::internal::HeapObject>, v8::internal::Handle<v8::internal::HeapObject>, v8::internal::Handle<v8::internal::FunctionTemplateInfo>, v8::internal::Handle<v8::internal::Object>, v8::internal::BuiltinArguments) [/usr/local/bin/node]
     6: v8::internal::Builtin_Impl_HandleApiCall(v8::internal::BuiltinArguments, v8::internal::Isolate*) [/usr/local/bin/node]

Upgrade Node to the current version (at the time of writing, this was 10.7.0) and execute the following commands from the datacollector directory:

    cd datacollector-ui
    rm -rf node_modules
    rm -rf package-lock.json
    npm --force cache clean
    npm install