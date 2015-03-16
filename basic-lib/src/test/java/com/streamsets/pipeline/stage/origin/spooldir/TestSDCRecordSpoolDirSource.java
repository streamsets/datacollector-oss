/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.spooldir;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.JsonRecordWriter;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.UUID;

public class TestSDCRecordSpoolDirSource {

  private String createTestDir() {
    File f = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(f.mkdirs());
    return f.getAbsolutePath();
  }

  private File createErrorRecordsFile() throws Exception {
    File f = new File(createTestDir(), "errorrecords-0000.json");
    Source.Context sourceContext = ContextInfoCreator.createSourceContext("myInstance", false, OnRecordError.TO_ERROR,
      ImmutableList.of("lane"));
    JsonRecordWriter jsonRecordWriter = ((ContextExtensions) sourceContext).createJsonRecordWriter(new OutputStreamWriter(new FileOutputStream(f)));

    Record r = RecordCreator.create("s", "c::1");
    r.set(Field.create("Hello"));
    jsonRecordWriter.write(r);
    r.set(Field.create("Bye"));
    jsonRecordWriter.write(r);
    jsonRecordWriter.close();
    return f;
  }

  private SpoolDirSource createSource(String dir) {
    return new SpoolDirSource(DataFormat.SDC_JSON, "UTF-8", 100, createTestDir(), 10, 1, null, 10, null, null,
                              PostProcessingOptions.ARCHIVE, dir, 10, null, null, -1, null, 0, 10, null, 0);
  }

  @Test
  public void testProduceFullFile() throws Exception {
    File errorRecordsFile = createErrorRecordsFile();
    SpoolDirSource source = createSource(errorRecordsFile.getParent());
    SourceRunner runner = new SourceRunner.Builder(source).addOutputLane("lane").build();
    runner.runInit();
    try {
      BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
      Assert.assertEquals(-1, source.produce(errorRecordsFile, 0, 10, batchMaker));
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
