/**
 * Copyright 2016 StreamSets Inc.
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

import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.api.Record;

import java.sql.SQLException;
import java.util.Map;

public class MSOperationCode {

  protected static final int DELETE = 1;
  protected static final int INSERT = 2;
  protected static final int BEFORE_UPDATE = 3;
  protected static final int AFTER_UPDATE = 4;

  public static final String OP_FIELD = "__$operation";

  // Mapping of MS operation code and SDC Operation Type
  protected static final ImmutableMap<Integer, OperationType> CRUD_MAP = ImmutableMap.<Integer, OperationType> builder()
      .put(DELETE, OperationType.DELETE)
      .put(INSERT, OperationType.INSERT)
      .put(BEFORE_UPDATE, OperationType.BEFORE_UPDATE)
      .put(AFTER_UPDATE, OperationType.AFTER_UPDATE)
      .build();

  public static String getOpField(){
    return String.format("/%s", OP_FIELD);
  }

  public static void addOperationCodeToRecordHeader(Record record) {
    Map<String, Field> map = record.get().getValueAsListMap();
    if (map.containsKey(OP_FIELD)) {
      record.getHeader().setAttribute(
          OperationType.SDC_OPERATION_TYPE,
          CRUD_MAP.get(map.get(OP_FIELD).getValueAsInteger()).getLabel()
      );
    }
  }
}
