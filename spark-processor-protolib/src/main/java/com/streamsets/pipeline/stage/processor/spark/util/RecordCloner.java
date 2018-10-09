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
package com.streamsets.pipeline.stage.processor.spark.util;

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class RecordCloner {

  private static final String LIST = "LIST";
  private static final String LIST_MAP = "LIST_MAP";
  private static final String MAP = "MAP";

  private static final String DATE = "DATE";
  private static final String DATETIME = "DATETIME";
  private static final String TIME = "TIME";

  private static final String FILE_REF = "FILE_REF";

  private RecordCloner() {
  }

  /**
   * Kryo loads the RecordImpl in Spark's classloader. So this one clones it to this stage's classloader.
   *
   * @param record Record to be cloned
   * @param context The context of the {@linkplain Processor} to use to clone the record
   * @return Cloned record
   */
  @SuppressWarnings("unchecked")
  public static Record clone(Object record, Processor.Context context) {
    Record newRecord = context.createRecord("dummyId");
    try {
      Object origHeaders = record.getClass().getMethod("getHeader").invoke(record);
      Map<String, Object> headers =
          (Map<String, Object>) origHeaders.getClass().getMethod("getAllAttributes").invoke(origHeaders);
      Record.Header newHeaders = newRecord.getHeader();
      newHeaders.getClass().getMethod("overrideUserAndSystemAttributes", Map.class).invoke(newHeaders, headers);
      newRecord.set(RecordCloner.cloneField(record.getClass().getMethod("get").invoke(record)));
      return newRecord;
    } catch(Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @SuppressWarnings("unchecked")
  public static Field cloneField(Object field) throws Exception {
    Object fieldType = field.getClass().getMethod("getType").invoke(field);
    String fieldTypeName = (String) fieldType.getClass().getMethod("name").invoke(fieldType);

    Preconditions.checkArgument(!FILE_REF.equals(fieldTypeName), "FILE_REF is not supported in Cluster Mode");

    if (MAP.equals(fieldTypeName) || LIST_MAP.equals(fieldTypeName)) {
      Map<String, Object> fields = (Map<String, Object>) field.getClass().getMethod("getValueAsMap").invoke(field);
      LinkedHashMap<String, Field> mapData = fields == null ? null : new LinkedHashMap<>();
      if (fields != null) {
        for (Map.Entry<String, Object> fieldEntry : fields.entrySet()) {
          mapData.put(fieldEntry.getKey(), cloneField(fieldEntry.getValue()));
        }
      }
      if (LIST_MAP.equals(fieldTypeName)) {
        return Field.createListMap(mapData);
      } else {
        return Field.create(mapData);
      }
    }

    if (LIST.equals(fieldTypeName)) {
      List<Object> fields = (List<Object>) field.getClass().getMethod("getValueAsList").invoke(field);
      List<Field> listData = fields == null ? null : new ArrayList();
      if (fields != null) {
        for (Object fieldEntry : fields) {
          listData.add(cloneField(fieldEntry));
        }
      }
      return Field.create(listData);
    }

    if (DATE.equals(fieldTypeName) || DATETIME.equals(fieldTypeName) || TIME.equals(fieldTypeName)) {
      Date val = (Date) field.getClass().getMethod("getValueAsDatetime").invoke(field);
      if (DATETIME.equals(fieldTypeName)) {
        return Field.createDatetime(val);
      } else if(DATE.equals(fieldTypeName)) {
        return Field.createDate(val);
      } else {
        return Field.createTime(val);
      }
    }

    Object val = field.getClass().getMethod("getValue").invoke(field);
    return Field.create(Field.Type.valueOf(fieldTypeName), val);
  }

}
