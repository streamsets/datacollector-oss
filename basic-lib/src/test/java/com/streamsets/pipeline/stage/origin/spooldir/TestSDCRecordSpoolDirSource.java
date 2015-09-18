/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.RecordWriter;
import com.streamsets.pipeline.config.CsvRecordType;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.FileCompression;
import com.streamsets.pipeline.config.OnParseError;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.util.List;
import java.util.UUID;

public class TestSDCRecordSpoolDirSource {

  private String createTestDir() {
    File f = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(f.mkdirs());
    return f.getAbsolutePath();
  }

  private File createErrorRecordsFile() throws Exception {
    File f = new File(createTestDir(), "sdc-records-000000");
    Source.Context sourceContext = ContextInfoCreator.createSourceContext("myInstance", false, OnRecordError.TO_ERROR,
      ImmutableList.of("lane"));

    FileOutputStream fileOutputStream = new FileOutputStream(f);
    RecordWriter jsonRecordWriter = ((ContextExtensions) sourceContext).createRecordWriter(fileOutputStream);

    Record r = RecordCreator.create("s", "c::1");
    r.set(Field.create("Hello"));
    jsonRecordWriter.write(r);
    r.set(Field.create("Bye"));
    jsonRecordWriter.write(r);
    jsonRecordWriter.close();
    return f;
  }

  private SpoolDirSource createSource(String dir) {
    return new SpoolDirSource(DataFormat.SDC_JSON, "UTF-8", false, 100, createTestDir(), 10, 1, "*", 10, null,
                              FileCompression.NONE, null,
                              PostProcessingOptions.ARCHIVE, dir, 10, null, null, -1, '^', '^', '^', null, 0, 10, null,
                              0, null, 0, false, null, null, null, null, null, false, null, OnParseError.ERROR, -1,
                              null, CsvRecordType.LIST);
  }

  @Test
  public void testProduceFullFile() throws Exception {
    File errorRecordsFile = createErrorRecordsFile();
    SpoolDirSource source = createSource(errorRecordsFile.getParent());
    SourceRunner runner = new SourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
      Assert.assertEquals("-1", source.produce(errorRecordsFile, "0", 10, batchMaker));
      StageRunner.Output output = SourceRunner.getOutput(batchMaker);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(2, records.size());
      Record r = RecordCreator.create("s", "c::1");
      r.set(Field.create("Hello"));
      Assert.assertEquals(r, records.get(0));
      r.set(Field.create("Bye"));
      Assert.assertEquals(r, records.get(1));
    } finally {
      runner.runDestroy();
    }
  }

}
