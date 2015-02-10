/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.spooldir.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.dirspooler.DirectorySpooler;
import com.streamsets.pipeline.lib.stage.source.spooldir.FileDataType;
import com.streamsets.pipeline.lib.stage.source.spooldir.SpoolDirSource;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.List;
import java.util.UUID;

public class TestErrorRecordSpoolDirSource {

  private String createTestDir() {
    File f = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(f.mkdirs());
    return f.getAbsolutePath();
  }

  private File createErrorRecordsFile() throws Exception {
    ObjectWriter jsonWriter = new ObjectMapper().writer();
    File f = new File(createTestDir(), "errorrecords-0000.json");
    Writer writer = new FileWriter(f);
    Record r = RecordCreator.create("s", "c::1");
    r.set(Field.create("Hello"));
    writer.write(jsonWriter.writeValueAsString(r));
    r.set(Field.create("Bye"));
    writer.write(jsonWriter.writeValueAsString(r));
    writer.close();
    return f;
  }

  @Test
  public void testProduceFullFile() throws Exception {
    File errorRecordsFile = createErrorRecordsFile();
    SourceRunner runner = new SourceRunner.Builder(SpoolDirSource.class)
        .addConfiguration("postProcessing", DirectorySpooler.FilePostProcessing.ARCHIVE)
        .addConfiguration("batchSize", 10)
        .addConfiguration("spoolDir", errorRecordsFile.getParent())
        .addConfiguration("archiveDir", createTestDir())
        .addConfiguration("retentionTimeMins", 10)
        .addConfiguration("poolingTimeoutSecs", 0)
        .addConfiguration("errorArchiveDir", null)
        .addConfiguration("fileDataType", FileDataType.SDC_RECORDS)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {
      SpoolDirSource source = (SpoolDirSource) runner.getStage();
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
