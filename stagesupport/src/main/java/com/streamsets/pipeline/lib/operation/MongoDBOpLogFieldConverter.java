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

public class MongoDBOpLogFieldConverter implements FieldPathConverter {

  private static final String ID_FIELD = "_id";
  private static final String OP_FIELD = "o";
  private static final String OP2_FIELD = "o2";
  private static final String SET_FIELD = "\\$set";

  /**
   * This is used by destination which performs CRUD operations.
   * Records sent from MongoOpLog origin have /o field that contains a map of
   * key-value for INSERT and DELETE operation, but
   * UPDATE record has /o2 field which contains _id and /o/$set field
   * which contains a map of key-value to update.
   * This method looks into the right field path for UPDATE so that
   * users don't need a separate field-column mapping for UPDATE.
   * @param fieldPath fieldPath in incoming record
   * @param operation Crud operation code
   * @return Actual field path in record
   */
  @Override
  public String getFieldPath(String fieldPath, int operation) {
    if (operation == OperationType.UPDATE_CODE) {
    if (fieldPath.contains(ID_FIELD)){
      // _id is stored in "/o2/column_name for update records. Need to change the fieldpath"
      return fieldPath.replace(OP_FIELD, OP2_FIELD);
    } else {
      // column and values are stored in "/o/$set/column_name". Need to change the fieldpath
      return fieldPath.replaceFirst(OP_FIELD, String.format("%s/%s", OP_FIELD, SET_FIELD));
    }
  }
    return fieldPath;
  }

}
