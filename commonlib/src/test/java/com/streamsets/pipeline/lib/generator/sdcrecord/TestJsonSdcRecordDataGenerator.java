/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.generator.sdcrecord;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.JsonRecordReader;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

public class TestJsonSdcRecordDataGenerator {

  private ContextExtensions getContextExtensions() {
    return(ContextExtensions) ContextInfoCreator.createTargetContext("i", false, OnRecordError.TO_ERROR);
  }

  @Test
  public void testFactory() throws Exception {
    Stage.Context context = ContextInfoCreator.createTargetContext("i", false, OnRecordError.TO_ERROR);
    Map<String, Object> configs = new HashMap<>();
    JsonSdcRecordCharDataGeneratorFactory.registerConfigs(configs);
    JsonSdcRecordCharDataGeneratorFactory factory = new JsonSdcRecordCharDataGeneratorFactory(context, configs);
    JsonSdcRecordDataGenerator generator = (JsonSdcRecordDataGenerator) factory.getGenerator(new StringWriter());
    Assert.assertNotNull(generator);
  }

  @Test
  public void testGenerate() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new JsonSdcRecordDataGenerator(getContextExtensions().createJsonRecordWriter(writer));
    Record record = RecordCreator.create();
    record.set(Field.create("Hello"));
    gen.write(record);
    gen.close();
    JsonRecordReader reader = getContextExtensions().createJsonRecordReader(new StringReader(writer.toString()), 0, -1);
    Record readRecord = reader.readRecord();
    Assert.assertNotNull(readRecord);
    Assert.assertNull(reader.readRecord());
    Assert.assertEquals(record, readRecord);
  }

  @Test
  public void testClose() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new JsonSdcRecordDataGenerator(getContextExtensions().createJsonRecordWriter(writer));
    Record record = RecordCreator.create();
    record.set(Field.create("Hello"));
    gen.write(record);
    gen.close();
    gen.close();
  }

  @Test(expected = IOException.class)
  public void testWriteAfterClose() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new JsonSdcRecordDataGenerator(getContextExtensions().createJsonRecordWriter(writer));
    Record record = RecordCreator.create();
    record.set(Field.create("Hello"));
    gen.close();
    gen.write(record);
  }

  @Test(expected = IOException.class)
  public void testFlushAfterClose() throws Exception {
    StringWriter writer = new StringWriter();
    DataGenerator gen = new JsonSdcRecordDataGenerator(getContextExtensions().createJsonRecordWriter(writer));
    Record record = RecordCreator.create();
    record.set(Field.create("Hello"));
    gen.close();
    gen.flush();
  }

}
