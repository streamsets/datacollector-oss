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

StreamSets Data Collector root module for the implementation.

All Data Collector implementation modules (not libraries) must have
this module as parent.

This module inherits from root-proto POM and in addition it defines
the version of all 3rd party libraries used by the implementation.

Note that modules like 'el' should inherit this POM.
