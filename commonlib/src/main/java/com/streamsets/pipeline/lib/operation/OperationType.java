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

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;

public class OperationType {

  public static final String SDC_OPERATION_TYPE = "sdc.operation.type";

  public static final int INSERT_CODE = 1;
  public static final int DELETE_CODE = 2;
  public static final int UPDATE_CODE = 3;
  public static final int UPSERT_CODE = 4;
  public static final int UNSUPPORTED_CODE = 5;
  public static final int UNDELETE_CODE = 6;
  public static final int REPLACE_CODE = 7;

  private static final BiMap<Integer, String> CODE_LABEL = new ImmutableBiMap.Builder<Integer, String>()
      .put(INSERT_CODE, "INSERT")
      .put(DELETE_CODE, "DELETE")
      .put(UPDATE_CODE, "UPDATE")
      .put(UPSERT_CODE, "UPSERT")
      .put(UNSUPPORTED_CODE, "UNSUPPORTED")
      .put(UNDELETE_CODE, "UNDELETE")
      .put(REPLACE_CODE, "REPLACE")
      .build();

  private static final ImmutableMap<String, Integer> LABEL_CODE = new ImmutableMap.Builder<String, Integer>()
      .put("INSERT", INSERT_CODE)
      .put("DELETE", DELETE_CODE)
      .put("UPDATE", UPDATE_CODE)
      .put("UPSERT", UPSERT_CODE)
      .put("UNSUPPORTED", UNSUPPORTED_CODE)
      .put("UNDELETE", UNDELETE_CODE)
      .put("REPLACE", REPLACE_CODE)
      .build();


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
    return CODE_LABEL.inverse().getOrDefault(op, -1);
  }
}
