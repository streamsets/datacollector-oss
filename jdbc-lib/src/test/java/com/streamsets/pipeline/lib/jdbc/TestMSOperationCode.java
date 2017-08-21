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

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.junit.Test;
import org.junit.Assert;

import java.util.HashMap;
import java.util.Map;

public class TestMSOperationCode {

  @Test
  public void testGetOpField(){
    Assert.assertEquals("/__$operation", MSOperationCode.getOpField());
  }

  @Test
  public void testAddOperationCodeToRecordHeader(){
    Record record = RecordCreator.create();
    Map<String, Field> fields = new HashMap<>();
    fields.put(MSOperationCode.OP_FIELD, Field.create(String.valueOf(MSOperationCode.INSERT)));
    record.set(Field.create(fields));

    MSOperationCode.addOperationCodeToRecordHeader(record);
    Assert.assertEquals(
        String.valueOf(OperationType.INSERT_CODE),
        record.getHeader().getAttribute(OperationType.SDC_OPERATION_TYPE)
    );

    fields.put(MSOperationCode.OP_FIELD, Field.create(String.valueOf(MSOperationCode.DELETE)));
    record.set(Field.create(fields));

    MSOperationCode.addOperationCodeToRecordHeader(record);
    Assert.assertEquals(
        String.valueOf(OperationType.DELETE_CODE),
        record.getHeader().getAttribute(OperationType.SDC_OPERATION_TYPE)
    );

    fields.put(MSOperationCode.OP_FIELD, Field.create(String.valueOf(MSOperationCode.AFTER_UPDATE)));
    record.set(Field.create(fields));

    MSOperationCode.addOperationCodeToRecordHeader(record);
    Assert.assertEquals(
        String.valueOf(OperationType.UPDATE_CODE),
        record.getHeader().getAttribute(OperationType.SDC_OPERATION_TYPE)
    );
  }

  @Test
  public void testConvertToJDBCCode(){
    Assert.assertEquals(
        OperationType.INSERT_CODE,
        MSOperationCode.convertToJDBCCode(MSOperationCode.INSERT)
    );

    Assert.assertEquals(
        OperationType.UPDATE_CODE,
        MSOperationCode.convertToJDBCCode(MSOperationCode.AFTER_UPDATE)
    );

    Assert.assertEquals(
        OperationType.DELETE_CODE,
        MSOperationCode.convertToJDBCCode(MSOperationCode.DELETE)
    );

    try {
      MSOperationCode.convertToJDBCCode(10);
    } catch (UnsupportedOperationException ex){
      //pass
    } catch (Exception ex){
      Assert.fail("Wrong exception is thrown");
    }
  }
}
