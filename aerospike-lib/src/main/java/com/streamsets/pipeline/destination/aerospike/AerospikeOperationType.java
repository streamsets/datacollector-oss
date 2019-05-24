/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.pipeline.destination.aerospike;

import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.operation.OperationType;

public enum AerospikeOperationType implements Label {
  DELETE(OperationType.DELETE_CODE),
  UPSERT(OperationType.UPSERT_CODE),
  ;

  final int code;

  AerospikeOperationType(int code) {
    this.code = code;
  }

  @Override
  public String getLabel() {
    return OperationType.getLabelFromIntCode(this.code);
  }

  public int getCode() {
    return code;
  }

  /**
   * Take a numeric operation code in String and check if the number is
   * valid operation code.
   * The operation code must be numeric: 1(insert), 2(update), 3(delete), etc,
   *
   * @param op Numeric operation code in String type
   * @return Operation code in int. Throws UnsupportedOperationException or
   * NumberFormatException if invalid.
   */
  static int convertToIntCode(String op) {
    try {
      int intOp = Integer.parseInt(op);
      switch (intOp) {
        case OperationType.DELETE_CODE:
        case OperationType.UPSERT_CODE:
          return intOp;
        default:
          throw new UnsupportedOperationException(Utils.format("Operation code {} is not supported", op));
      }
    } catch (NumberFormatException ex) {
      throw new NumberFormatException("Operation code must be a numeric value. " + ex.getMessage());
    }
  }
}
