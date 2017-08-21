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
package com.streamsets.pipeline.stage.origin.mongodb.oplog;

import com.streamsets.pipeline.api.Label;

public enum OplogOpType implements Label{
  INSERT("INSERT(i)", "i"),
  DELETE("DELETE(d)", "d"),
  UPDATE("UPDATE(u)", "u"),
  NOOP("NOOP(n)", "n"),
  CMD("CMD(c)", "c"),
  DB("DB(db)", "db"),
  ;

  private final String label;
  private final String op;

  OplogOpType(String label, String op) {
    this.label = label;
    this.op = op;
  }

  @Override
  public String getLabel() {
    return this.label;
  }

  public String getOp() {
    return this.op;
  }

  public static OplogOpType getOplogTypeFromOpString(String op) {
    for (OplogOpType oplogOpType : OplogOpType.values()) {
      if (oplogOpType.getOp().equals(op)) {
        return oplogOpType;
      }
    }
    return null;
  }

}
