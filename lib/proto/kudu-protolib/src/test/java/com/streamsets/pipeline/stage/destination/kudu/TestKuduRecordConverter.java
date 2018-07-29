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

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.stage.lib.kudu.Errors;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.PartialRowHelper;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TestKuduRecordConverter {

  private Stage.Context context;
  private Record record;
  private KuduRecordConverter kuduRecordConverter;
  private PartialRow partialRow;

  @Before
  public void setup() throws Exception {
    context = ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR, Collections.EMPTY_LIST);
    record = context.createRecord("123");
    record.set("/", Field.create(new LinkedHashMap<String, Field>()));
    record.set("/byte", Field.create((byte)1));
    record.set("/short", Field.create((short)123));
    record.set("/int", Field.create(123));
    record.set("/long", Field.create(123L));
    record.set("/float", Field.create(123.0f));
    record.set("/double", Field.create(123.0d));
    record.set("/bytes", Field.create("ABC".getBytes(StandardCharsets.UTF_8)));
    record.set("/str", Field.create("ABC"));
    record.set("/bool", Field.create(true));
    DateTime dt = new DateTime(2017, 8, 24, 9, 15, 30, DateTimeZone.UTC); // 2017/8/24 9:15:30
    record.set("/unixtime", Field.create(dt.getMillis() * 1000L));
    Map<String, Field.Type> columnsToFieldTypes = ImmutableMap.<String, Field.Type>builder()
      .put("byte1", Field.Type.BYTE)
      .put("short1", Field.Type.SHORT)
      .put("int1", Field.Type.INTEGER)
      .put("long1", Field.Type.LONG)
      .put("float1", Field.Type.FLOAT)
      .put("double1", Field.Type.DOUBLE)
      .put("bytes", Field.Type.BYTE_ARRAY)
      .put("str", Field.Type.STRING)
      .put("bool1", Field.Type.BOOLEAN)
      .put("unixtime_micro", Field.Type.LONG)
      .build();
    Map<String, String> fieldsToColumns = ImmutableMap.<String, String>builder()
      .put("/byte", "byte1")
      .put("/short", "short1")
      .put("/int", "int1")
      .put("/long", "long1")
      .put("/float", "float1")
      .put("/double", "double1")
      .put("/bytes", "bytes")
      .put("/str", "str")
      .put("/bool", "bool1")
      .put("/unixtime", "unixtime_micro")
      .build();
    Schema schema = new Schema(Arrays.asList(
      new ColumnSchema.ColumnSchemaBuilder("str", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("byte1", Type.INT8).build(),
      new ColumnSchema.ColumnSchemaBuilder("short1", Type.INT16).nullable(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("int1", Type.INT32).build(),
      new ColumnSchema.ColumnSchemaBuilder("long1", Type.INT64).build(),
      new ColumnSchema.ColumnSchemaBuilder("float1", Type.FLOAT).build(),
      new ColumnSchema.ColumnSchemaBuilder("double1", Type.DOUBLE).build(),
      new ColumnSchema.ColumnSchemaBuilder("bytes", Type.BINARY).build(),
      new ColumnSchema.ColumnSchemaBuilder("bool1", Type.BOOL).build(),
      new ColumnSchema.ColumnSchemaBuilder("unixtime_micro", Type.UNIXTIME_MICROS).build()
      ));
    partialRow = new PartialRow(schema);
    kuduRecordConverter = new KuduRecordConverter(columnsToFieldTypes, fieldsToColumns, schema, null);
  }

  private String toString(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return new String(bytes, Charsets.UTF_8);
  }

  @Test
  public void testBasic() throws Exception {
    kuduRecordConverter.convert(record, partialRow, KuduOperationType.INSERT.code);
    Assert.assertEquals(
      "[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 123, 0, 123, 0, 0, 0, 123, " +
          "0, 0, 0, 0, 0, 0, 0, 0, 0, -10, 66, 0, 0, 0, 0, 0, -64, 94, 64, 0, 0, 0, 0, 0, 0, 0, 0, " +
          "0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 116, -11, -46, 109, -67, -35, 20, 0, 0]",
      PartialRowHelper.toString(partialRow));
    List<ByteBuffer> varLengthData = PartialRowHelper.getVarLengthData(partialRow);
    Assert.assertEquals(10, varLengthData.size());
    Assert.assertEquals("ABC", toString(varLengthData.get(7)));
    Assert.assertNull(varLengthData.get(1));
    Assert.assertNull(varLengthData.get(2));
  }

  @Test
  public void testNotNullable() throws Exception {
    record.delete("/byte");
    try {
      kuduRecordConverter.convert(record, partialRow, KuduOperationType.INSERT.code);
      Assert.fail();
    } catch (OnRecordErrorException ex) {
      Assert.assertEquals(Errors.KUDU_06, ex.getErrorCode());
    }
  }

  @Test
  public void testNumberFormatException() throws Exception {
    record.set("/long", Field.create("ABC"));
    try {
      kuduRecordConverter.convert(record, partialRow, KuduOperationType.INSERT.code);
      Assert.fail();
    } catch (OnRecordErrorException ex) {
      Assert.assertEquals(Errors.KUDU_09, ex.getErrorCode());
    }
  }

  @Test
  public void testNullButExists() throws Exception {
    record.set("/short1", Field.create((String)null));
    kuduRecordConverter.convert(record, partialRow, KuduOperationType.INSERT.code); // must not throw NPE
  }

  @Test
  public void testUpdate() throws Exception {
    record.set("/str", Field.create("val1"));
    record.set("/long", Field.create((long)10));
    record.set("/short1", Field.create(Field.Type.SHORT, null));
    kuduRecordConverter.convert(record, partialRow, KuduOperationType.UPDATE.code); // must not throw NPE
  }

  @Test
  public void testDelete() throws Exception {
    record.set("/str", Field.create("primary key"));
    record.set("/long", Field.create((long)10));
    record.set("/short1", Field.create(Field.Type.SHORT, null));
    kuduRecordConverter.convert(record, partialRow, KuduOperationType.DELETE.code); // must not throw NPE
    Assert.assertTrue(Utils.format("Message: {}", partialRow.stringifyRowKey()), partialRow.stringifyRowKey().contains("primary key"));
  }
}
