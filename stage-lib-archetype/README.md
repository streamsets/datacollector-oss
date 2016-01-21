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

StreamSets Stage Library Archetype
===================================

This is a Maven archetype for creating StreamSets stages.

Creating a skeleton project
--------------------------

Creating a project using the archetype is accomplished like so:

```
% mvn archetype:generate \
    -DarchetypeGroupId=com.streamsets \
    -DarchetypeArtifactId=streamsets-datacollector-stage-lib-tutorial \
    -DarchetypeVersion=<version>
```

This will then ask you a few questions:

  * Define value for property 'groupId': : **com.mytest**
  * Define value for property 'artifactId': : **mygame**
  * Define value for property 'version':  <version>: : **<default>**
  * Define value for property 'package':  com.mytest: : **<default>**
