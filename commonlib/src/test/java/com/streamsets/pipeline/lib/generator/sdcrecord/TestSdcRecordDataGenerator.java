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
package com.streamsets.pipeline.lib.generator.sdcrecord;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.RecordReader;
import com.streamsets.pipeline.lib.data.DataFactory;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactoryBuilder;
import com.streamsets.pipeline.lib.generator.DataGeneratorFormat;
import com.streamsets.pipeline.lib.util.SdcRecordConstants;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;

public class TestSdcRecordDataGenerator {

  private ContextExtensions getContextExtensions() {
    return(ContextExtensions) ContextInfoCreator.createTargetContext("i", false, OnRecordError.TO_ERROR);
  }

  @Test
  public void testFactory() throws Exception {
    Stage.Context context = ContextInfoCreator.createTargetContext("i", false, OnRecordError.TO_ERROR);

    DataFactory dataFactory = new DataGeneratorFactoryBuilder(context, DataGeneratorFormat.SDC_RECORD)
      .setCharset(Charset.forName("UTF-16")).build();
    Assert.assertTrue(dataFactory instanceof SdcRecordDataGeneratorFactory);
    SdcRecordDataGeneratorFactory factory = (SdcRecordDataGeneratorFactory) dataFactory;
    SdcRecordDataGenerator generator = (SdcRecordDataGenerator) factory.getGenerator(new ByteArrayOutputStream());
    Assert.assertNotNull(generator);

    Writer writer = factory.createWriter(new ByteArrayOutputStream());
    Assert.assertTrue(writer instanceof OutputStreamWriter);
    OutputStreamWriter outputStreamWriter = (OutputStreamWriter) writer;
    Assert.assertEquals("UTF-16", outputStreamWriter.getEncoding());
  }

  @Test
  public void testGenerate() throws Exception {
    ByteArrayOutputStream writer = new ByteArrayOutputStream();
    DataGenerator gen = new SdcRecordDataGenerator(
        getContextExtensions().createRecordWriter(writer),
        getContextExtensions()
    );
    Record record = RecordCreator.create();
    record.set(Field.create("Hello"));
    gen.write(record);
    gen.close();
    RecordReader reader = getContextExtensions().createRecordReader(new ByteArrayInputStream(writer.toByteArray()), 0,
                                                                    -1);
    Record readRecord = reader.readRecord();
    Assert.assertNotNull(readRecord);
    Assert.assertNull(reader.readRecord());
    Assert.assertEquals(record, readRecord);
  }

  @Test
  public void testClose() throws Exception {
    ByteArrayOutputStream writer = new ByteArrayOutputStream();
    DataGenerator gen = new SdcRecordDataGenerator(
        getContextExtensions().createRecordWriter(writer),
        getContextExtensions()
    );
    Record record = RecordCreator.create();
    record.set(Field.create("Hello"));
    gen.write(record);
    gen.close();
    gen.close();
  }

  @Test(expected = IOException.class)
  public void testWriteAfterClose() throws Exception {
    ByteArrayOutputStream writer = new ByteArrayOutputStream();
    DataGenerator gen = new SdcRecordDataGenerator(
        getContextExtensions().createRecordWriter(writer),
        getContextExtensions()
    );
    Record record = RecordCreator.create();
    record.set(Field.create("Hello"));
    gen.close();
    gen.write(record);
  }

  @Test(expected = IOException.class)
  public void testFlushAfterClose() throws Exception {
    ByteArrayOutputStream writer = new ByteArrayOutputStream();
    DataGenerator gen = new SdcRecordDataGenerator(
        getContextExtensions().createRecordWriter(writer),
        getContextExtensions()
    );
    Record record = RecordCreator.create();
    record.set(Field.create("Hello"));
    gen.close();
    gen.flush();
  }

  // https://issues.streamsets.com/browse/SDC-4126
  @Test
  @Ignore
  public void testGenerateWithSampler() throws Exception {
    ByteArrayOutputStream writer = new ByteArrayOutputStream();
    DataGenerator gen = new SdcRecordDataGenerator(
        getContextExtensions().createRecordWriter(writer),
        getContextExtensions()
    );

    Record record = RecordCreator.create();
    record.set(Field.create("Hello"));

    long timeBeforeWrite = System.currentTimeMillis();
    gen.write(record);
    long timeAfterWrite = System.currentTimeMillis();
    gen.close();

    long sampledTime = Long.parseLong(record.getHeader().getAttribute(SdcRecordConstants.SDC_SAMPLED_TIME));
    Assert.assertTrue(sampledTime >= timeBeforeWrite);
    Assert.assertTrue(sampledTime <= timeAfterWrite);
  }

}
