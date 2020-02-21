--
-- Copyright 2017 StreamSets Inc.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

/*
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
*/
CREATE TABLE ALL_TYPES (
  _decimal decimal,
  _tinyint tinyint,
  _smallint smallint,
  _mediumint mediumint,
  _float float,
  _double double,
  _timestamp timestamp,
  _bigint bigint,
  _int int,
  _date date,
  _time time,
  _datetime datetime,
  _year year,
  _varchar varchar(10),
  _enum enum('a', 'b', 'c'),
  _set set('1', '2', '3'),
  _tinyblob tinyblob,
  _mediumblob mediumblob,
  _longblob longblob,
  _blob blob,
  _text text,
  _tinytext tinytext,
  _mediumtext mediumtext,
  _longtext longtext
);

CREATE TABLE foo (
  bar int
);

CREATE TABLE foo2 (
  a int,
  b int,
  c int DEFAULT 3
);
