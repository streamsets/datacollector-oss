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

import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.operation.OperationType;

public class OracleCDCOperationCode {

  public static final String OPERATION = "oracle.cdc.operation"; //this will be deprecated

  //These codes are defined by Oracle Database
  public static final int INSERT_CODE = 1;
  public static final int DELETE_CODE = 2;
  public static final int UPDATE_CODE = 3;
  public static final int DDL_CODE = 5;
  public static final int SELECT_FOR_UPDATE_CODE = 25;
  public static final int COMMIT_CODE = 7;
  public static final int ROLLBACK_CODE = 36;

  /**
   * This is called when JDBC target didn't find sdc.operation.code in record header
   * but found oracle.cdc.operation. Since oracle.cdc.operation contains Oracle specific
   * operation code, we need to convert to SDC operation code.
   * @param code Operation code defined by Oracle.
   * @return Operation code defined by SDC.
   */
  public static int convertFromOracleToSDCCode(String code){
    try {
      int intCode = Integer.parseInt(code);
      switch (intCode) {
        case INSERT_CODE:
          return OperationType.INSERT_CODE;
        case DELETE_CODE:
          return OperationType.DELETE_CODE;
        case UPDATE_CODE:
        case SELECT_FOR_UPDATE_CODE:
          return OperationType.UPDATE_CODE;
        default:  //DDL_CODE
          throw new UnsupportedOperationException(Utils.format("Operation code {} is not supported", code));
      }
    } catch (NumberFormatException ex) {
      throw new NumberFormatException("Operation code must be a numeric value. " + ex.getMessage());
    }
  }
}
