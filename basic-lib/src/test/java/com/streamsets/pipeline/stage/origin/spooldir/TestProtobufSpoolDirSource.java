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

import com.google.common.io.Resources;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.Compression;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.OnParseError;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.lib.dirspooler.LocalFileSystem;
import com.streamsets.pipeline.lib.dirspooler.Offset;
import com.streamsets.pipeline.lib.dirspooler.PathMatcherMode;
import com.streamsets.pipeline.lib.dirspooler.SpoolDirConfigBean;
import com.streamsets.pipeline.lib.dirspooler.SpoolDirRunnable;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestProtobufSpoolDirSource {
  private static final int threadNumber = 0;
  private static final int batchSize = 10;
  private static final Map<String, Offset> lastSourceOffset = new HashMap<>();

  private static String TEST_PROTOBUF_FILE = Resources.getResource("TestProtobuf3.ser").getPath();
  private static String TEST_DESCRIPTOR_FILE = Resources.getResource("TestRecordProtobuf3.desc").getPath();

  private SpoolDirSource createSource() {
    SpoolDirConfigBean conf = new SpoolDirConfigBean();
    conf.dataFormat = DataFormat.PROTOBUF;
    conf.spoolDir = new File(TEST_PROTOBUF_FILE).getParent();
    conf.batchSize = 10;
    conf.overrunLimit = 100;
    conf.poolingTimeoutSecs = 10;
    conf.filePattern = "*.ser";
    conf.pathMatcherMode = PathMatcherMode.GLOB;
    conf.maxSpoolFiles = 10;
    conf.initialFileToProcess = null;
    conf.dataFormatConfig.compression = Compression.NONE;
    conf.dataFormatConfig.filePatternInArchive = "*";
    conf.postProcessing = PostProcessingOptions.NONE;
    conf.dataFormatConfig.protoDescriptorFile = TEST_DESCRIPTOR_FILE;
    conf.dataFormatConfig.messageType = "TestRecord";
    conf.dataFormatConfig.isDelimited = true;
    conf.dataFormatConfig.onParseError = OnParseError.ERROR;
    conf.dataFormatConfig.maxStackTraceLines = 0;

    return new SpoolDirSource(conf);
  }

  @Test
  public void testProduceFullFile() throws Exception {
    SpoolDirSource source = createSource();
    PushSourceRunner runner = new PushSourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
      SpoolDirRunnable runnable = source.getSpoolDirRunnable(threadNumber, batchSize, lastSourceOffset);
      assertEquals("-1", runnable.generateBatch(new LocalFileSystem("*", PathMatcherMode.GLOB).getFile(TEST_PROTOBUF_FILE), null, 10, batchMaker));
      StageRunner.Output output = SourceRunner.getOutput(batchMaker);
      List<Record> records = output.getRecords().get("lane");
      assertNotNull(records);
      assertEquals(1, records.size());
      Record record = records.get(0);

      assertTrue(record.has("/first_name"));
      assertTrue(record.has("/full_name"));
      assertTrue(record.has("/test_map"));
      assertTrue(record.has("/samples"));

      assertEquals("Adam", record.get("/first_name").getValueAsString());
      assertEquals("", record.get("/full_name").getValue());
      List<Field> samples = record.get("/samples").getValueAsList();
      assertEquals(2, samples.size());
      assertEquals(1, samples.get(0).getValueAsInteger());
      assertEquals(2, samples.get(1).getValueAsInteger());
      Map<String, Field> testMap = record.get("/test_map").getValueAsMap();
      assertEquals(2, testMap.size());
      assertTrue(testMap.containsKey("hello"));
      assertTrue(testMap.containsKey("bye"));
      assertEquals("world", testMap.get("hello").getValueAsString());
      assertEquals("earth", testMap.get("bye").getValueAsString());
    } finally {
      source.destroy();
      runner.runDestroy();
    }
  }
}
