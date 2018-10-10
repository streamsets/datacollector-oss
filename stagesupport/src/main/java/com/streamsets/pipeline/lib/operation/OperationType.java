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
package com.streamsets.pipeline.lib.operation;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class OperationType {

  public static final String SDC_OPERATION_TYPE = "sdc.operation.type";

  public static final int INSERT_CODE = 1;
  public static final int DELETE_CODE = 2;
  public static final int UPDATE_CODE = 3;
  public static final int UPSERT_CODE = 4;
  public static final int UNSUPPORTED_CODE = 5;
  public static final int UNDELETE_CODE = 6;
  public static final int REPLACE_CODE = 7;
  public static final int MERGE_CODE = 8;
  public static final int LOAD_CODE = 9;

  private static final Map<Integer, String> CODE_LABEL;
  static {
    Map<Integer, String> map = new HashMap<>();
    map.put(INSERT_CODE, "INSERT");
    map.put(DELETE_CODE, "DELETE");
    map.put(UPDATE_CODE, "UPDATE");
    map.put(UPSERT_CODE, "UPSERT");
    map.put(UNSUPPORTED_CODE, "UNSUPPORTED");
    map.put(UNDELETE_CODE, "UNDELETE");
    map.put(REPLACE_CODE, "REPLACE");
    map.put(MERGE_CODE, "MERGE");
    map.put(LOAD_CODE, "LOAD");

    CODE_LABEL = Collections.unmodifiableMap(map);
  }

  private static final Map<String, Integer> LABEL_CODE;
  static {
    Map<String,  Integer> map = new HashMap<>();
    map.put("INSERT", INSERT_CODE);
    map.put("DELETE", DELETE_CODE);
    map.put("UPDATE", UPDATE_CODE);
    map.put("UPSERT", UPSERT_CODE);
    map.put("UNSUPPORTED", UNSUPPORTED_CODE);
    map.put("UNDELETE", UNDELETE_CODE);
    map.put("REPLACE", REPLACE_CODE);
    map.put("MERGE", MERGE_CODE);
    map.put("LOAD", LOAD_CODE);

    LABEL_CODE = Collections.unmodifiableMap(map);
  }

  /**
   * Convert from code in int type to String
   * @param code
   * @return
   */
  public static String getLabelFromIntCode(int code)  {
    return CODE_LABEL.getOrDefault(code, "UNSUPPORTED");
  }

  /**
   * Convert from code in String type to label
   * @param code
   * @return
   */
  public static String getLabelFromStringCode(String code) throws NumberFormatException {
    try {
      int intCode = Integer.parseInt(code);
      return getLabelFromIntCode(intCode);
    } catch (NumberFormatException ex) {
      throw new NumberFormatException(
          String.format("%s but received '%s'","operation code must be numeric", code)
      );
    }
  }

  /**
   * Convert from label in String to Code.
   * @param op
   * @return int value of the code. -1 if not defined.
   */
  public static int getCodeFromLabel(String op) {
    return LABEL_CODE.getOrDefault(op, -1);
  }
}
