/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.jdbc;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;

public class JdbcFieldColumnMapping {

  /**
   * Constructor used for unit testing purposes
   * @param columnName
   * @param field
   */
  public JdbcFieldColumnMapping(final String columnName, final String field) {
    this.columnName = columnName;
    this.field = field;
  }

  /**
   * Parameter-less constructor required.
   */
  public JdbcFieldColumnMapping() {}

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.STRING,
          defaultValue="",
          label = "Column Name",
          description = "The database column name.",
          displayPosition = 10
  )
  public String columnName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "SDC Field",
      description = "The field in the record to receive the value.",
      displayPosition = 20
  )
  @FieldSelectorModel(singleValued = true)
  public String field;
}
