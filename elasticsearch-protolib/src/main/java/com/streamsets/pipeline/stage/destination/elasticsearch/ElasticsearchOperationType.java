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
package com.streamsets.pipeline.stage.destination.elasticsearch;

import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.lib.operation.OperationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum ElasticsearchOperationType implements Label {
  INDEX(OperationType.UPSERT_CODE),
  CREATE(OperationType.INSERT_CODE),
  UPDATE(OperationType.UPDATE_CODE),
  DELETE(OperationType.DELETE_CODE),
  MERGE(OperationType.MERGE_CODE)
  ;

  final int code;
  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchTarget.class);

  ElasticsearchOperationType(int code) {
    this.code = code;
  }

  @Override
  public String getLabel() {
    switch (this.code) {
      case OperationType.UPSERT_CODE:
        return "INDEX";
      case OperationType.INSERT_CODE:
        return "CREATE";
      case OperationType.UPDATE_CODE:
        return "UPDATE";
      case OperationType.DELETE_CODE:
        return "DELETE";
      case OperationType.MERGE_CODE:
        return "UPDATE with doc_as_upsert";
      default:
        return null;
    }
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
      for (ElasticsearchOperationType type : ElasticsearchOperationType.values()) {
        if (type.code == intOp) {
          return type.code;
        }
      }
      LOG.error("Unsupported operation by Elasticsearch Destination: Operation Code {}", op);
      return -1;
    } catch (NumberFormatException ex) {
      throw new NumberFormatException("Operation code must be numeric value. " + ex.getMessage());
    }
  }
}
