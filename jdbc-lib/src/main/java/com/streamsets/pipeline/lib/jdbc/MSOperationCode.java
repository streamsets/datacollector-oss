/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.pipeline.lib.jdbc;

import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.Utils;


import java.util.Map;

public class MSOperationCode {

  static final int DELETE = 1;
  static final int INSERT = 2;
  static final int BEFORE_UPDATE = 3;
  static final int AFTER_UPDATE = 4;

  static final String CT_DELETE = "D";
  static final String CT_INSERT = "I";
  static final String CT_UPDATE = "U";

  static final String OP_FIELD = "__$operation";

  // Mapping of MS operation code and SDC Operation Type
  static final ImmutableMap<Integer, Integer> CRUD_MAP = ImmutableMap.<Integer, Integer> builder()
      .put(DELETE, OperationType.DELETE_CODE)
      .put(INSERT, OperationType.INSERT_CODE)
      .put(BEFORE_UPDATE, OperationType.UNSUPPORTED_CODE)
      .put(AFTER_UPDATE, OperationType.UPDATE_CODE)
      .build();

  static final ImmutableMap<String, Integer> CT_CRUD_MAP = ImmutableMap.<String, Integer> builder()
      .put(CT_DELETE, OperationType.DELETE_CODE)
      .put(CT_INSERT, OperationType.INSERT_CODE)
      .put(CT_UPDATE, OperationType.UPDATE_CODE)
      .build();

  static String getOpField(){
    return "/" + OP_FIELD;
  }

  public static void addOperationCodeToRecordHeader(Record record) {
    Map<String, Field> map = record.get().getValueAsListMap();
    if (map.containsKey(OP_FIELD)) {
      int op = CRUD_MAP.get(map.get(OP_FIELD).getValueAsInteger());
      record.getHeader().setAttribute(
          OperationType.SDC_OPERATION_TYPE,
          String.valueOf(op)
      );
    }
  }

  public static int convertToJDBCCode(String op) {
    if (CT_CRUD_MAP.containsKey(op)) {
      return CT_CRUD_MAP.get(op);
    }
    throw new UnsupportedOperationException(Utils.format("Operation code {} is not supported", op));
  }

  /**
   * Take an numeric operation code and check if the number is
   * valid operation code.
   * The operation code must be numeric: 1(insert), 2(update), 3(delete), etc,
   * @param op Numeric operation code in String
   * @return Operation code in int, -1 if invalid number
   */
  public static int convertToJDBCCode(int op)  {
    if (CRUD_MAP.containsKey(op)){
      return CRUD_MAP.get(op);
    }
    throw new UnsupportedOperationException(Utils.format("Operation code {} is not supported", op));
  }
}
