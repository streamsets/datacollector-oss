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

public class MySQLBinLogFieldConverter implements FieldPathConverter {

  private static final String DATA_FIELD = "Data";
  private static final String OLDDATA_FIELD = "OldData";

  /**
   * This is used by destination which performs CRUD operations.
   * Records sent from MySQLBinLog origin have /Data field that contains a map of
   * key-value for INSERT and UPDATE operation, but
   * /OldData field for DELETE operation.
   * This method looks into /OldData for DELETE operation so that users don't
   * need a separate field-column mapping for DELETE.
   * @param fieldPath fieldPath specified in field-column mapping
   * @param operation Crud operation code
   * @return Actual field path in record
   */
  @Override
  public String getFieldPath(String fieldPath, int operation) {
    if (operation == OperationType.DELETE_CODE) {
      return fieldPath.replace(DATA_FIELD, OLDDATA_FIELD);
    }
    return fieldPath;
  }
}
