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
package com.streamsets.pipeline.stage.origin.spooldir;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.Compression;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.OnParseError;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.lib.dirspooler.SpoolDirBaseSource;
import com.streamsets.pipeline.lib.dirspooler.SpoolDirConfigBean;
import com.streamsets.pipeline.lib.dirspooler.SpoolDirRunnable;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.lib.dirspooler.WrappedFile;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.junit.Assert;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class AvroSpoolDirSourceTestUtil {
  private static final int threadNumber = 0;
  private static final int batchSize = 10;

  public static String createTestDir() {
    File f = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(f.mkdirs());
    return f.getAbsolutePath();
  }

  public static SpoolDirConfigBean getConf() {
    DataParserFormatConfig dataFormatConfig = new DataParserFormatConfig();
    dataFormatConfig.compression = Compression.NONE;
    dataFormatConfig.filePatternInArchive = "*";
    dataFormatConfig.avroSchema = AVRO_SCHEMA;
    dataFormatConfig.onParseError = OnParseError.ERROR;
    dataFormatConfig.maxStackTraceLines = 0;

    return new SpoolBaseSourceBuilder()
        .dataFormat(DataFormat.AVRO)
        .spoolDir(createTestDir())
        .filePattern("file-[0-9].avro")
        .dataFormatConfig(dataFormatConfig)
        .postProcessing(PostProcessingOptions.ARCHIVE)
        .archiveDir(createTestDir())
        .getConf();
  }

  public static final String AVRO_SCHEMA = "{\n"
      +"\"type\": \"record\",\n"
      +"\"name\": \"Employee\",\n"
      +"\"fields\": [\n"
      +" {\"name\": \"name\", \"type\": \"string\"},\n"
      +" {\"name\": \"age\", \"type\": \"int\"},\n"
      +" {\"name\": \"emails\", \"type\": {\"type\": \"array\", \"items\": \"string\"}},\n"
      +" {\"name\": \"boss\", \"type\": [\"Employee\",\"null\"]}\n"
      +"]}";

  public static File createAvroDataFile() throws Exception {
    File f = new File(createTestDir(), "file-0.avro");
    Schema schema = new Schema.Parser().parse(AVRO_SCHEMA);
    GenericRecord boss = new GenericData.Record(schema);
    boss.put("name", "boss");
    boss.put("age", 60);
    boss.put("emails", ImmutableList.of("boss@company.com", "boss2@company.com"));
    boss.put("boss", null);

    GenericRecord e3 = new GenericData.Record(schema);
    e3.put("name", "c");
    e3.put("age", 50);
    e3.put("emails", ImmutableList.of("c@company.com", "c2@company.com"));
    e3.put("boss", boss);

    GenericRecord e2 = new GenericData.Record(schema);
    e2.put("name", "b");
    e2.put("age", 40);
    e2.put("emails", ImmutableList.of("b@company.com", "b2@company.com"));
    e2.put("boss", boss);

    GenericRecord e1 = new GenericData.Record(schema);
    e1.put("name", "a");
    e1.put("age", 30);
    e1.put("emails", ImmutableList.of("a@company.com", "a2@company.com"));
    e1.put("boss", boss);

    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(schema, f);
    dataFileWriter.append(e1);
    dataFileWriter.append(e2);
    dataFileWriter.append(e3);

    dataFileWriter.flush();
    dataFileWriter.close();

    return f;
  }

  public static <C extends PushSource> void testProduceFullFile(Class<C> clazz, SpoolDirBaseSource source, WrappedFile file) throws Exception {
    PushSourceRunner runner = new PushSourceRunner.Builder(clazz, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
      SpoolDirRunnable runnable = source.getSpoolDirRunnable(threadNumber, batchSize, null);
      Assert.assertEquals("-1", runnable.generateBatch(file, null, 10, batchMaker));
      StageRunner.Output output = SourceRunner.getOutput(batchMaker);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(3, records.size());

      Record e3Record = records.get(2);
      Assert.assertTrue(e3Record.has("/name"));
      Assert.assertEquals("c", e3Record.get("/name").getValueAsString());
      Assert.assertTrue(e3Record.has("/age"));
      Assert.assertEquals(50, e3Record.get("/age").getValueAsInteger());
      Assert.assertTrue(e3Record.has("/emails"));
      Assert.assertTrue(e3Record.get("/emails").getValueAsList() instanceof List);
      List<Field> emails = e3Record.get("/emails").getValueAsList();
      Assert.assertEquals(2, emails.size());
      Assert.assertEquals("c@company.com", emails.get(0).getValueAsString());
      Assert.assertEquals("c2@company.com", emails.get(1).getValueAsString());
      Assert.assertTrue(e3Record.has("/boss"));
      Assert.assertTrue(e3Record.get("/boss").getValueAsMap() instanceof Map);
      Assert.assertTrue(e3Record.has("/boss/name"));
      Assert.assertEquals("boss", e3Record.get("/boss/name").getValueAsString());
      Assert.assertTrue(e3Record.has("/boss/age"));
      Assert.assertEquals(60, e3Record.get("/boss/age").getValueAsInteger());
      Assert.assertTrue(e3Record.has("/boss/emails"));
      Assert.assertTrue(e3Record.get("/boss/emails").getValueAsList() instanceof List);
      emails = e3Record.get("/boss/emails").getValueAsList();
      Assert.assertEquals(2, emails.size());
      Assert.assertEquals("boss@company.com", emails.get(0).getValueAsString());
      Assert.assertEquals("boss2@company.com", emails.get(1).getValueAsString());

      Record e2Record = records.get(1);
      Assert.assertTrue(e2Record.has("/name"));
      Assert.assertEquals("b", e2Record.get("/name").getValueAsString());
      Assert.assertTrue(e2Record.has("/age"));
      Assert.assertEquals(40, e2Record.get("/age").getValueAsInteger());
      Assert.assertTrue(e2Record.has("/emails"));
      Assert.assertTrue(e2Record.get("/emails").getValueAsList() instanceof List);
      emails = e2Record.get("/emails").getValueAsList();
      Assert.assertEquals(2, emails.size());
      Assert.assertEquals("b@company.com", emails.get(0).getValueAsString());
      Assert.assertEquals("b2@company.com", emails.get(1).getValueAsString());
      Assert.assertTrue(e2Record.has("/boss"));
      Assert.assertTrue(e2Record.get("/boss").getValueAsMap() instanceof Map);
      Assert.assertTrue(e2Record.has("/boss/name"));
      Assert.assertEquals("boss", e2Record.get("/boss/name").getValueAsString());
      Assert.assertTrue(e2Record.has("/boss/age"));
      Assert.assertEquals(60, e2Record.get("/boss/age").getValueAsInteger());
      Assert.assertTrue(e2Record.has("/boss/emails"));
      Assert.assertTrue(e2Record.get("/boss/emails").getValueAsList() instanceof List);
      emails = e2Record.get("/boss/emails").getValueAsList();
      Assert.assertEquals(2, emails.size());
      Assert.assertEquals("boss@company.com", emails.get(0).getValueAsString());
      Assert.assertEquals("boss2@company.com", emails.get(1).getValueAsString());

      Record e1Record = records.get(0);
      Assert.assertTrue(e1Record.has("/name"));
      Assert.assertEquals("a", e1Record.get("/name").getValueAsString());
      Assert.assertTrue(e1Record.has("/age"));
      Assert.assertEquals(30, e1Record.get("/age").getValueAsInteger());
      Assert.assertTrue(e1Record.has("/emails"));
      Assert.assertTrue(e1Record.get("/emails").getValueAsList() instanceof List);
      emails = e1Record.get("/emails").getValueAsList();
      Assert.assertEquals(2, emails.size());
      Assert.assertEquals("a@company.com", emails.get(0).getValueAsString());
      Assert.assertEquals("a2@company.com", emails.get(1).getValueAsString());
      Assert.assertTrue(e1Record.has("/boss"));
      Assert.assertTrue(e1Record.get("/boss").getValueAsMap() instanceof Map);
      Assert.assertTrue(e1Record.has("/boss/name"));
      Assert.assertEquals("boss", e1Record.get("/boss/name").getValueAsString());
      Assert.assertTrue(e1Record.has("/boss/age"));
      Assert.assertEquals(60, e1Record.get("/boss/age").getValueAsInteger());
      Assert.assertTrue(e1Record.has("/boss/emails"));
      Assert.assertTrue(e1Record.get("/boss/emails").getValueAsList() instanceof List);
      emails = e1Record.get("/boss/emails").getValueAsList();
      Assert.assertEquals(2, emails.size());
      Assert.assertEquals("boss@company.com", emails.get(0).getValueAsString());
      Assert.assertEquals("boss2@company.com", emails.get(1).getValueAsString());

    } finally {
      source.destroy();
      runner.runDestroy();
    }
  }

  public static <C extends PushSource> void testProduceLessThanFile(Class<C> clazz, SpoolDirBaseSource source, WrappedFile file) throws Exception {
    PushSourceRunner runner = new PushSourceRunner.Builder(clazz, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
      SpoolDirRunnable runnable = source.getSpoolDirRunnable(threadNumber, batchSize, null);
      String offset = runnable.generateBatch(file, null, 1, batchMaker);
      Assert.assertNotEquals("-1", offset);
      StageRunner.Output output = SourceRunner.getOutput(batchMaker);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(1, records.size());
      Record e1Record = records.get(0);
      Assert.assertTrue(e1Record.has("/name"));
      Assert.assertEquals("a", e1Record.get("/name").getValueAsString());
      Assert.assertTrue(e1Record.has("/age"));
      Assert.assertEquals(30, e1Record.get("/age").getValueAsInteger());
      Assert.assertTrue(e1Record.has("/emails"));
      Assert.assertTrue(e1Record.get("/emails").getValueAsList() instanceof List);
      List<Field> emails = e1Record.get("/emails").getValueAsList();
      Assert.assertEquals(2, emails.size());
      Assert.assertEquals("a@company.com", emails.get(0).getValueAsString());
      Assert.assertEquals("a2@company.com", emails.get(1).getValueAsString());
      Assert.assertTrue(e1Record.has("/boss"));
      Assert.assertTrue(e1Record.get("/boss").getValueAsMap() instanceof Map);
      Assert.assertTrue(e1Record.has("/boss/name"));
      Assert.assertEquals("boss", e1Record.get("/boss/name").getValueAsString());
      Assert.assertTrue(e1Record.has("/boss/age"));
      Assert.assertEquals(60, e1Record.get("/boss/age").getValueAsInteger());
      Assert.assertTrue(e1Record.has("/boss/emails"));
      Assert.assertTrue(e1Record.get("/boss/emails").getValueAsList() instanceof List);
      emails = e1Record.get("/boss/emails").getValueAsList();
      Assert.assertEquals(2, emails.size());
      Assert.assertEquals("boss@company.com", emails.get(0).getValueAsString());
      Assert.assertEquals("boss2@company.com", emails.get(1).getValueAsString());

      batchMaker = SourceRunner.createTestBatchMaker("lane");
      offset = runnable.generateBatch(file, null, 1, batchMaker);
      Assert.assertNotEquals("-1", offset);
      output = SourceRunner.getOutput(batchMaker);
      records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(1, records.size());

      Record e2Record = records.get(0);
      Assert.assertTrue(e2Record.has("/name"));
      Assert.assertEquals("b", e2Record.get("/name").getValueAsString());
      Assert.assertTrue(e2Record.has("/age"));
      Assert.assertEquals(40, e2Record.get("/age").getValueAsInteger());
      Assert.assertTrue(e2Record.has("/emails"));
      Assert.assertTrue(e2Record.get("/emails").getValueAsList() instanceof List);
      emails = e2Record.get("/emails").getValueAsList();
      Assert.assertEquals(2, emails.size());
      Assert.assertEquals("b@company.com", emails.get(0).getValueAsString());
      Assert.assertEquals("b2@company.com", emails.get(1).getValueAsString());
      Assert.assertTrue(e2Record.has("/boss"));
      Assert.assertTrue(e2Record.get("/boss").getValueAsMap() instanceof Map);
      Assert.assertTrue(e2Record.has("/boss/name"));
      Assert.assertEquals("boss", e2Record.get("/boss/name").getValueAsString());
      Assert.assertTrue(e2Record.has("/boss/age"));
      Assert.assertEquals(60, e2Record.get("/boss/age").getValueAsInteger());
      Assert.assertTrue(e2Record.has("/boss/emails"));
      Assert.assertTrue(e2Record.get("/boss/emails").getValueAsList() instanceof List);
      emails = e2Record.get("/boss/emails").getValueAsList();
      Assert.assertEquals(2, emails.size());
      Assert.assertEquals("boss@company.com", emails.get(0).getValueAsString());
      Assert.assertEquals("boss2@company.com", emails.get(1).getValueAsString());

      batchMaker = SourceRunner.createTestBatchMaker("lane");
      offset = runnable.generateBatch(file, null, 1, batchMaker);
      Assert.assertNotEquals("-1", offset);
      output = SourceRunner.getOutput(batchMaker);
      records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(1, records.size());

      Record e3Record = records.get(0);
      Assert.assertTrue(e3Record.has("/name"));
      Assert.assertEquals("c", e3Record.get("/name").getValueAsString());
      Assert.assertTrue(e3Record.has("/age"));
      Assert.assertEquals(50, e3Record.get("/age").getValueAsInteger());
      Assert.assertTrue(e3Record.has("/emails"));
      Assert.assertTrue(e3Record.get("/emails").getValueAsList() instanceof List);
      emails = e3Record.get("/emails").getValueAsList();
      Assert.assertEquals(2, emails.size());
      Assert.assertEquals("c@company.com", emails.get(0).getValueAsString());
      Assert.assertEquals("c2@company.com", emails.get(1).getValueAsString());
      Assert.assertTrue(e3Record.has("/boss"));
      Assert.assertTrue(e3Record.get("/boss").getValueAsMap() instanceof Map);
      Assert.assertTrue(e3Record.has("/boss/name"));
      Assert.assertEquals("boss", e3Record.get("/boss/name").getValueAsString());
      Assert.assertTrue(e3Record.has("/boss/age"));
      Assert.assertEquals(60, e3Record.get("/boss/age").getValueAsInteger());
      Assert.assertTrue(e3Record.has("/boss/emails"));
      Assert.assertTrue(e3Record.get("/boss/emails").getValueAsList() instanceof List);
      emails = e3Record.get("/boss/emails").getValueAsList();
      Assert.assertEquals(2, emails.size());
      Assert.assertEquals("boss@company.com", emails.get(0).getValueAsString());
      Assert.assertEquals("boss2@company.com", emails.get(1).getValueAsString());

      batchMaker = SourceRunner.createTestBatchMaker("lane");
      offset = runnable.generateBatch(file, null, 1, batchMaker);
      Assert.assertEquals("-1", offset);
      output = SourceRunner.getOutput(batchMaker);
      records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(0, records.size());

    } finally {
      source.destroy();
      runner.runDestroy();
    }
  }
}
