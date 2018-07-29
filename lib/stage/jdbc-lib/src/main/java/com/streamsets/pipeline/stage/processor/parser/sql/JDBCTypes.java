/*
 * Copyright 2018 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.processor.parser.sql;

import com.streamsets.pipeline.api.Label;

public enum JDBCTypes implements Label {
  BIGINT,
  BINARY,
  LONGVARBINARY,
  VARBINARY,
  BIT,
  BOOLEAN,
  CHAR,
  LONGNVARCHAR,
  LONGVARCHAR,
  NCHAR,
  NVARCHAR,
  VARCHAR,
  DECIMAL,
  NUMERIC,
  DOUBLE,
  FLOAT,
  REAL,
  INTEGER,
  SMALLINT,
  TINYINT,
  DATE,
  TIME,
  TIMESTAMP,
  TIMESTAMP_WITH_TIMEZONE,
  TIMESTAMP_TZ_TYPE,
  TIMESTAMP_LTZ_TYPE,
  OTHER,
  ;


  @Override
  public String getLabel() {
    return this.name();
  }
}
