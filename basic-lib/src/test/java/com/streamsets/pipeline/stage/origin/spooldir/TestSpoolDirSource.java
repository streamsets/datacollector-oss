/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.spooldir;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.UUID;

public class TestSpoolDirSource {

  private String createTestDir() {
    File f = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(f.mkdirs());
    return f.getAbsolutePath();
  }

  public static class TSpoolDirSource extends SpoolDirSource {
    File file;
    long offset;
    int maxBatchSize;
    long offsetIncrement;
    boolean produceCalled;

    @Override
    public long produce(File file, long offset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      produceCalled = true;
      Assert.assertEquals(this.file, file);
      Assert.assertEquals(this.offset, offset);
      Assert.assertEquals(this.maxBatchSize, maxBatchSize);
      Assert.assertNotNull(batchMaker);
      return offset + offsetIncrement;
    }
  }

  private SourceRunner createSourceRunner(SpoolDirSource source, String initialFile) {
    return new SourceRunner.Builder(source)
        .addConfiguration("dataFormat", DataFormat.TEXT)
        .addConfiguration("maxLogLineLength", 1024)
        .addConfiguration("setTruncated", false)
        .addConfiguration("postProcessing", PostProcessingOptions.ARCHIVE)
        .addConfiguration("filePattern", "file-[0-9].log")
        .addConfiguration("batchSize", 10)
        .addConfiguration("maxSpoolFiles", 10)
        .addConfiguration("spoolDir", createTestDir())
        .addConfiguration("archiveDir", createTestDir())
        .addConfiguration("retentionTimeMins", 10)
        .addConfiguration("initialFileToProcess", initialFile)
        .addConfiguration("poolingTimeoutSecs", 0)
        .addConfiguration("errorArchiveDir", null)
        .addOutputLane("lane")
        .build();
  }
  @Test
  public void testInitDestroy() throws Exception {
    TSpoolDirSource source = new TSpoolDirSource();
    SourceRunner runner = createSourceRunner(source, "file-0.log");
    runner.runInit();
    try {
      Assert.assertTrue(source.getSpooler().isRunning());
      Assert.assertEquals(runner.getContext(), source.getSpooler().getContext());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void getOffsetMethods() throws Exception {
    TSpoolDirSource source = new TSpoolDirSource();
    createSourceRunner(source, "file-0.log");
    Assert.assertNull(source.getFileFromSourceOffset(null));
    Assert.assertEquals("x", source.getFileFromSourceOffset("x"));
    Assert.assertEquals(0, source.getOffsetFromSourceOffset(null));
    Assert.assertEquals(0, source.getOffsetFromSourceOffset("x"));
    Assert.assertEquals("x", source.getFileFromSourceOffset(source.createSourceOffset("x", 1)));
    Assert.assertEquals(1, source.getOffsetFromSourceOffset(source.createSourceOffset("x", 1)));
  }

  @Test
  public void testProduceNoInitialFileNoFileInSpoolDirNullOffset() throws Exception {
    TSpoolDirSource source = new TSpoolDirSource();
    SourceRunner runner = createSourceRunner(source, null);
    runner.runInit();
    try {
      StageRunner.Output output = runner.runProduce(null, 10);
      Assert.assertEquals(null, output.getNewOffset());
      Assert.assertFalse(source.produceCalled);

    } finally {
      runner.runDestroy();
    }
  }


  @Test
  public void testProduceNoInitialFileWithFileInSpoolDirNullOffset() throws Exception {
    TSpoolDirSource source = new TSpoolDirSource();
    SourceRunner runner = createSourceRunner(source, "file-0.log");
    File file = new File(source.spoolDir, "file-0.log").getAbsoluteFile();
    Files.createFile(file.toPath());
    runner.runInit();
    source.file = file;
    source.offset = 0;
    source.maxBatchSize = 10;
    try {
      StageRunner.Output output = runner.runProduce(null, 10);
      Assert.assertEquals(source.createSourceOffset("file-0.log", 0), output.getNewOffset());
      Assert.assertTrue(source.produceCalled);
    } finally {
      runner.runDestroy();
    }
  }


  @Test
  public void testProduceNoInitialFileNoFileInSpoolDirNotNullOffset() throws Exception {
    TSpoolDirSource source = new TSpoolDirSource();
    SourceRunner runner = createSourceRunner(source, null);
    runner.runInit();
    try {
      StageRunner.Output output = runner.runProduce("file-0.log", 10);
      Assert.assertEquals(source.createSourceOffset("file-0.log", 0), output.getNewOffset());
      Assert.assertFalse(source.produceCalled);
    } finally {
      runner.runDestroy();
    }
  }


  @Test
  public void testProduceNoInitialFileWithFileInSpoolDirNotNullOffset() throws Exception {
    TSpoolDirSource source = new TSpoolDirSource();
    SourceRunner runner = createSourceRunner(source, "file-0.log");
    File file = new File(source.spoolDir, "file-0.log").getAbsoluteFile();
    Files.createFile(file.toPath());
    runner.runInit();
    source.file = file;
    source.offset = 0;
    source.maxBatchSize = 10;
    try {
      StageRunner.Output output = runner.runProduce("file-0.log", 10);
      Assert.assertEquals(source.createSourceOffset("file-0.log", 0), output.getNewOffset());
      Assert.assertTrue(source.produceCalled);
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testProduceNoInitialFileWithFileInSpoolDirNonZeroOffset() throws Exception {
    TSpoolDirSource source = new TSpoolDirSource();
    SourceRunner runner = createSourceRunner(source, null);
    File file = new File(source.spoolDir, "file-0.log").getAbsoluteFile();
    Files.createFile(file.toPath());
    runner.runInit();
    source.file = file;
    source.offset = 1;
    source.maxBatchSize = 10;
    try {
      StageRunner.Output output = runner.runProduce(source.createSourceOffset("file-0.log", 1), 10);
      Assert.assertEquals(source.createSourceOffset("file-0.log", 1), output.getNewOffset());
      Assert.assertTrue(source.produceCalled);
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testAdvanceToNextSpoolFile() throws Exception {
    TSpoolDirSource source = new TSpoolDirSource();
    SourceRunner runner = createSourceRunner(source, null);
    File file1 = new File(source.spoolDir, "file-0.log").getAbsoluteFile();
    Files.createFile(file1.toPath());
    File file2 = new File(source.spoolDir, "file-1.log").getAbsoluteFile();
    Files.createFile(file2.toPath());
    runner.runInit();
    source.file = file1;
    source.offset = 0;
    source.maxBatchSize = 10;
    try {
      StageRunner.Output output = runner.runProduce(source.createSourceOffset("file-0.log", 0), 10);
      Assert.assertEquals(source.createSourceOffset("file-0.log", 0), output.getNewOffset());
      Assert.assertTrue(source.produceCalled);

      source.produceCalled = false;
      source.offsetIncrement = -1;
      output = runner.runProduce(output.getNewOffset(), 10);
      Assert.assertEquals(source.createSourceOffset("file-0.log", -1), output.getNewOffset());
      Assert.assertTrue(source.produceCalled);

      source.file = file2;
      output = runner.runProduce(output.getNewOffset(), 10);
      source.produceCalled = false;
      source.offset = 0;
      source.offsetIncrement = 0;
      output = runner.runProduce(output.getNewOffset(), 10);
      Assert.assertEquals(source.createSourceOffset("file-1.log", -1), output.getNewOffset());
      Assert.assertFalse(source.produceCalled);
    } finally {
      runner.runDestroy();
    }
  }

}
