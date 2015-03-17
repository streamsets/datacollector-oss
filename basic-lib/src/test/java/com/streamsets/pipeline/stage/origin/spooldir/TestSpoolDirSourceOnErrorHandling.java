/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.spooldir;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.UUID;

public class TestSpoolDirSourceOnErrorHandling {

  private String createTestDir() {
    File f = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(f.mkdirs());
    return f.getAbsolutePath();
  }

  private SpoolDirSource createSource() throws Exception {
    String dir = createTestDir();
    File file1 = new File(dir, "file-0.csv").getAbsoluteFile();
    Writer writer = new FileWriter(file1);
    IOUtils.write("a,b\ncccc,dddd\ne,f\n", writer);
    writer.close();
    File file2 = new File(dir, "file-1.csv").getAbsoluteFile();
    writer = new FileWriter(file2);
    IOUtils.write("x,y", writer);
    writer.close();
    return new SpoolDirSource(DataFormat.DELIMITED, "UTF-8", 100, dir, 10, 1, "file-[0-9].csv", 10, null, null,
                              PostProcessingOptions.ARCHIVE, dir, 10, CsvMode.RFC4180, CsvHeader.NO_HEADER,
                              5, null, 0, 10, null, 0, null, 0, false);
  }

  @Test
  public void testOnErrorDiscardMaxObjectLen() throws Exception {
    SpoolDirSource source = createSource();
    SourceRunner runner = new SourceRunner.Builder(source).addOutputLane("lane")
                                                          .setOnRecordError(OnRecordError.DISCARD).build();
    runner.runInit();
    try {
      StageRunner.Output output = runner.runProduce(source.createSourceOffset("file-0.csv", 0), 10);
      Assert.assertEquals(source.createSourceOffset("file-0.csv", -1), output.getNewOffset());
      Assert.assertEquals(2, output.getRecords().get("lane").size());
      Assert.assertEquals("a", output.getRecords().get("lane").get(0).get("[0]/value").getValueAsString());
      Assert.assertEquals("e", output.getRecords().get("lane").get(1).get("[0]/value").getValueAsString());
      Assert.assertEquals(0, runner.getErrorRecords().size());

      output = runner.runProduce(output.getNewOffset(), 10);
      Assert.assertEquals(source.createSourceOffset("file-1.csv", -1), output.getNewOffset());
      Assert.assertEquals(1, output.getRecords().get("lane").size());
      Assert.assertEquals("x", output.getRecords().get("lane").get(0).get("[0]/value").getValueAsString());
      Assert.assertEquals(0, runner.getErrorRecords().size());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testOnErrorToErrorMaxObjectLen() throws Exception {
    SpoolDirSource source = createSource();
    SourceRunner runner = new SourceRunner.Builder(source).addOutputLane("lane")
                                                          .setOnRecordError(OnRecordError.TO_ERROR).build();
    runner.runInit();
    try {
      StageRunner.Output output = runner.runProduce(source.createSourceOffset("file-0.csv", 0), 10);
      Assert.assertEquals(source.createSourceOffset("file-0.csv", -1), output.getNewOffset());
      Assert.assertEquals(2, output.getRecords().get("lane").size());
      Assert.assertEquals("a", output.getRecords().get("lane").get(0).get("[0]/value").getValueAsString());
      Assert.assertEquals("e", output.getRecords().get("lane").get(1).get("[0]/value").getValueAsString());
      Assert.assertEquals(0, runner.getErrorRecords().size());
      Assert.assertEquals(1, runner.getErrors().size());

      runner.clearErrors();
      output = runner.runProduce(output.getNewOffset(), 10);
      Assert.assertEquals(source.createSourceOffset("file-1.csv", -1), output.getNewOffset());
      Assert.assertEquals(1, output.getRecords().get("lane").size());
      Assert.assertEquals("x", output.getRecords().get("lane").get(0).get("[0]/value").getValueAsString());
      Assert.assertEquals(0, runner.getErrorRecords().size());
      Assert.assertEquals(0, runner.getErrors().size());

    } finally {
      runner.runDestroy();
    }
  }

  @Test(expected = StageException.class)
  public void testOnErrorLenPipelineMaxObjectLen() throws Exception {
    SpoolDirSource source = createSource();
    SourceRunner runner = new SourceRunner.Builder(source).addOutputLane("lane")
                                                          .setOnRecordError(OnRecordError.STOP_PIPELINE).build();
    runner.runInit();
    try {
      runner.runProduce(source.createSourceOffset("file-0.csv", 0), 10);
    } finally {
      runner.runDestroy();
    }
  }

  private SpoolDirSource createSourceIOEx() throws Exception {
    String dir = createTestDir();
    File file1 = new File(dir, "file-0.json").getAbsoluteFile();
    Writer writer = new FileWriter(file1);
    IOUtils.write("[1,", writer);
    writer.close();
    File file2 = new File(dir, "file-1.json").getAbsoluteFile();
    writer = new FileWriter(file2);
    IOUtils.write("[2]", writer);
    writer.close();
    return new SpoolDirSource(DataFormat.JSON, "UTF-8", 100, dir, 10, 1, "file-[0-9].json", 10, null, null,
                              PostProcessingOptions.ARCHIVE, dir, 10, null, null,
                              5, JsonMode.ARRAY_OBJECTS, 100, 10, null, 0, null, 0, false);
  }

  @Test
  public void testOnErrorDiscardIOEx() throws Exception {
    SpoolDirSource source = createSourceIOEx();
    SourceRunner runner = new SourceRunner.Builder(source).addOutputLane("lane")
                                                          .setOnRecordError(OnRecordError.DISCARD).build();
    runner.runInit();
    try {
      StageRunner.Output output = runner.runProduce(source.createSourceOffset("file-0.json", 0), 10);
      Assert.assertEquals(source.createSourceOffset("file-0.json", -1), output.getNewOffset());
      Assert.assertEquals(1, output.getRecords().get("lane").size());
      Assert.assertEquals(1, output.getRecords().get("lane").get(0).get("").getValueAsInteger());
      Assert.assertEquals(0, runner.getErrorRecords().size());

      output = runner.runProduce(output.getNewOffset(), 10);
      Assert.assertEquals(source.createSourceOffset("file-1.json", -1), output.getNewOffset());
      Assert.assertEquals(1, output.getRecords().get("lane").size());
      Assert.assertEquals(2, output.getRecords().get("lane").get(0).get("").getValueAsInteger());
      Assert.assertEquals(0, runner.getErrorRecords().size());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testOnErrorToErrorIOEx() throws Exception {
    SpoolDirSource source = createSourceIOEx();
    SourceRunner runner = new SourceRunner.Builder(source).addOutputLane("lane")
                                                          .setOnRecordError(OnRecordError.TO_ERROR).build();
    runner.runInit();
    try {
      StageRunner.Output output = runner.runProduce(source.createSourceOffset("file-0.json", 0), 10);
      Assert.assertEquals(source.createSourceOffset("file-0.json", -1), output.getNewOffset());
      Assert.assertEquals(1, output.getRecords().get("lane").size());
      Assert.assertEquals(1, output.getRecords().get("lane").get(0).get("").getValueAsInteger());
      Assert.assertEquals(0, runner.getErrorRecords().size());
      Assert.assertEquals(1, runner.getErrors().size());

      runner.clearErrors();
      output = runner.runProduce(output.getNewOffset(), 10);
      Assert.assertEquals(source.createSourceOffset("file-1.json", -1), output.getNewOffset());
      Assert.assertEquals(1, output.getRecords().get("lane").size());
      Assert.assertEquals(2, output.getRecords().get("lane").get(0).get("").getValueAsInteger());
      Assert.assertEquals(0, runner.getErrorRecords().size());
      Assert.assertEquals(0, runner.getErrors().size());

    } finally {
      runner.runDestroy();
    }
  }

  @Test(expected = StageException.class)
  public void testOnErrorPipelineIOEx() throws Exception {
    SpoolDirSource source = createSourceIOEx();
    SourceRunner runner = new SourceRunner.Builder(source).addOutputLane("lane")
                                                          .setOnRecordError(OnRecordError.STOP_PIPELINE).build();
    runner.runInit();
    try {
      runner.runProduce(source.createSourceOffset("file-0.json", 0), 10);
    } finally {
      runner.runDestroy();
    }
  }

}
