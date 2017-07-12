/**
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
package com.streamsets.datacollector.json;

import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.ext.json.Mode;
import org.junit.Assert;
import org.junit.Test;

import java.io.StringWriter;

public class TestJsonRecordWriterImpl {

  @Test
  public void testMultipleObjects() throws Exception {
    StringWriter writer = new StringWriter();

    JsonRecordWriterImpl jsonRecordWriter = new JsonRecordWriterImpl(writer, Mode.MULTIPLE_OBJECTS);

    Record record = new RecordImpl("stage", "id", null, null);
    record.set(Field.create(10));

    jsonRecordWriter.write(record);
    jsonRecordWriter.write(record);
    jsonRecordWriter.write(record);
    jsonRecordWriter.close();

    Assert.assertEquals("10\n10\n10", writer.toString());
  }

  @Test
  public void testArray() throws Exception {
    StringWriter writer = new StringWriter();

    JsonRecordWriterImpl jsonRecordWriter = new JsonRecordWriterImpl(writer, Mode.ARRAY_OBJECTS);

    Record record = new RecordImpl("stage", "id", null, null);
    record.set(Field.create(666));

    jsonRecordWriter.write(record);
    jsonRecordWriter.write(record);
    jsonRecordWriter.write(record);
    jsonRecordWriter.close();

    Assert.assertEquals("[666,666,666]", writer.toString());
  }
}
