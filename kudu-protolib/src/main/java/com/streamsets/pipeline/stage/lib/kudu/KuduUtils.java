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
package com.streamsets.pipeline.stage.lib.kudu;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Stage.Context;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.stage.processor.kudulookup.Groups;
import com.streamsets.pipeline.stage.processor.kudulookup.KuduLookupConfig;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.Type;
import org.apache.kudu.client.RowResult;

import java.util.List;
import java.util.Date;

public class KuduUtils {

  /**
   * Check network connection to the kudu master.
   * @param kuduClient AsyncKuduClient
   * @param context
   * @param KUDU_MASTER
   * @param issues
   */
  public static void checkConnection(AsyncKuduClient kuduClient,
                                     Context context,
                                     String KUDU_MASTER,
                                     final List<Stage.ConfigIssue> issues
  ){
    try {
      kuduClient.getTablesList().join();
    } catch (Exception ex) {
      issues.add(
          context.createConfigIssue(
              Groups.KUDU.name(),
              KuduLookupConfig.CONF_PREFIX + KUDU_MASTER ,
              Errors.KUDU_00,
              ex.toString(),
              ex
          )
      );
    }
  }

  /**
   * Convert from Kudu type to SDC Field type
   * @param kuduType
   * @return Field.Type
   */
  public static Field.Type convertFromKuduType(Type kuduType){
    switch(kuduType) {
      case BINARY: return Field.Type.BYTE_ARRAY;
      case BOOL: return Field.Type.BOOLEAN;
      case DOUBLE: return Field.Type.DOUBLE;
      case FLOAT: return Field.Type.FLOAT;
      case INT8: return Field.Type.BYTE;
      case INT16: return Field.Type.SHORT;
      case INT32: return Field.Type.INTEGER;
      case INT64: return Field.Type.LONG;
      case STRING: return  Field.Type.STRING;
      case UNIXTIME_MICROS: return Field.Type.DATETIME;
      default:
        throw new UnsupportedOperationException("Unknown data type: " + kuduType.getName());
    }
  }

  /**
   * Create a field and assign a value off of RowResult.
   * @param result Result obtained from scan
   * @param fieldName Field name to create
   * @param type Kudu Type for the field
   * @return Generated field
   * @throws StageException
   */
  public static Field createField(RowResult result, String fieldName, Type type) throws StageException {
    switch (type) {
      case INT8:
        return Field.create(Field.Type.BYTE, result.getByte(fieldName));
      case INT16:
        return Field.create(Field.Type.SHORT, result.getShort(fieldName));
      case INT32:
        return Field.create(Field.Type.INTEGER, result.getInt(fieldName));
      case INT64:
        return Field.create(Field.Type.LONG, result.getLong(fieldName));
      case BINARY:
        try {
          return Field.create(Field.Type.BYTE_ARRAY, result.getBinary(fieldName));
        } catch (IllegalArgumentException ex) {
          throw new OnRecordErrorException(Errors.KUDU_35, fieldName);
        }
      case STRING:
        return Field.create(Field.Type.STRING, result.getString(fieldName));
      case BOOL:
        return Field.create(Field.Type.BOOLEAN, result.getBoolean(fieldName));
      case FLOAT:
        return Field.create(Field.Type.FLOAT, result.getFloat(fieldName));
      case DOUBLE:
        return Field.create(Field.Type.DOUBLE, result.getDouble(fieldName));
      case UNIXTIME_MICROS:
        //UNIXTIME_MICROS is in microsecond
        return Field.create(Field.Type.DATETIME, new Date(result.getLong(fieldName)/1000L));
      default:
        throw new StageException(Errors.KUDU_10, fieldName, type.getName());
    }
  }
}
