<!--
  Copyright 2016 StreamSets Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# MySql BinLog origin
Origin acting as MySql replication slave. It consumes events directly from MySql binary log using library [https://github.com/shyiko/mysql-binlog-connector-java](mysql-binlog-connector).
Events consumed from log are further enriched with columns names and converted to `Record` with appropriate data types.

Origin works well with MySql servers with and without GTID enabled.

## Implementation details and caveats
- GTID support
  Origin works both with GTID enabled and disabled. But if you change GTID mode on/off on the server - please reset origin, as it will not be able to parse offset correctly.
- Origin connects to mysql with `ssl=off`, if you need to change this - please change the source.
- Origin may produce more records than `maxBatchSize`, because individual `update/delete` events may affect many rows and they all are processed as one event. But mysql internally breaks statements affecting many rows to multiple events with moderate rows count in each.
- Origin can work with `binlog_row_image=MINIMAL`, but in this case it does not include default columns values for inserts.
- Origin does not check server's `serverId`, so its up to you to setup offset correctly when connecting to different server if GTID is off. When GTID is on - it does not matter.
- In case of server errors origin stops.
- If mysql connector cannot parse some event (it is known that this happens with `POINT` columns) - error is logged, but origin continues working.
- Known issue - if you start from beginning on first start and there very schema changes between first binlog event and current time - result may be unpredictable. The same issue happens if you stop origin, make changes to schema (drop a column for example) and then start origin. If it meets some event for table with changed schema - it may produce complete nonsense.
