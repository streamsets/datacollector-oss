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
package com.streamsets.pipeline.stage.destination.kudu;

import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.lib.operation.OperationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum KuduOperationType implements Label {
  INSERT(OperationType.INSERT_CODE),
  UPSERT(OperationType.UPSERT_CODE),
  UPDATE(OperationType.UPDATE_CODE),
  DELETE(OperationType.DELETE_CODE),
  ;

  final int code;
  private static final Logger LOG = LoggerFactory.getLogger(KuduTarget.class);

  KuduOperationType(int code) {
    this.code = code;
  }

  @Override
  public String getLabel() {
    return OperationType.getLabelFromIntCode(this.code);
  }

  /**
   * Take an numeric operation code in String and check if the number is
   * valid operation code.
   * The operation code must be numeric: 1(insert), 2(update), 3(delete), etc,
   * @param op Numeric operation code in String
   * @return Operation code in int, -1 if invalid number
   */
  static int convertToIntCode(String op)  {
    try {
      int intOp = Integer.parseInt(op);
      for (KuduOperationType type : KuduOperationType.values()) {
        if (type.code == intOp) {
          return type.code;
        }
      }
      LOG.error("Unsupported operation by Kudu Destination: Operation Code {}", op);
      return -1;
    } catch (NumberFormatException ex) {
      throw new NumberFormatException("Operation code must be numeric value. " + ex.getMessage());
    }
  }
}
