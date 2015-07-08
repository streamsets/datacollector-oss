/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.spooldir;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.FileCompression;
import com.streamsets.pipeline.config.OnParseError;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.List;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class TestCompressionSpoolDirSource {

  private String createTestDir() {
    File f = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(f.mkdirs());
    return f.getAbsolutePath();
  }

  private final static String LINE1 = "A";
  private final static String LINE2 = "B";
  private final static String LINE3 = "C";

  private void createFile(File file, FileCompression compression) throws Exception {
    OutputStream os = new FileOutputStream(file);
    switch (compression) {
      case NONE:
        break;
      case GZIP:
        os = new GZIPOutputStream(os);
        break;
      case ZIP:
        ZipOutputStream zos = new ZipOutputStream(os);
        zos.putNextEntry(new ZipEntry("file"));
        os = zos;
        break;
    }
    Writer writer = new OutputStreamWriter(os);
    writer.write(LINE1 + "\n");
    writer.write(LINE2 + "\n");
    writer.write(LINE3 + "\n");
    writer.close();
  }

  private SpoolDirSource createSource(String dir, FileCompression compression) {
    return new SpoolDirSource(DataFormat.TEXT, "UTF-8", false, 100, dir, 10, 1, "*", 10,
                              null, compression, null, PostProcessingOptions.NONE, createTestDir(), 10,
                              null, null, 0,
                              ' ', ' ', ' ', null, 0, 10, null, 0, null, 0, false, null, null, null, null,
                              null, false, null, OnParseError.ERROR, -1, null);
  }

  @Test
  public void testZip() throws Exception {
    String dir = createTestDir();
    createFile(new File(dir, "1.zip"), FileCompression.ZIP);
    SpoolDirSource source = createSource(dir, FileCompression.ZIP);
    SourceRunner runner = new SourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      StageRunner.Output output = runner.runProduce(null, 10);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(3, records.size());
      Assert.assertEquals("A", records.get(0).get("/text").getValueAsString());
      Assert.assertEquals("B", records.get(1).get("/text").getValueAsString());
      Assert.assertEquals("C", records.get(2).get("/text").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testCompressedNonZeroOffset() throws Exception {
    String dir = createTestDir();
    createFile(new File(dir, "1.zip"), FileCompression.ZIP);
    SpoolDirSource source = createSource(dir, FileCompression.ZIP);
    SourceRunner runner = new SourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      StageRunner.Output output = runner.runProduce("1.zip::2", 10);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(2, records.size());
      Assert.assertEquals("B", records.get(0).get("/text").getValueAsString());
      Assert.assertEquals("C", records.get(1).get("/text").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testGzip() throws Exception {
    String dir = createTestDir();
    createFile(new File(dir, "1.gz"), FileCompression.GZIP);
    SpoolDirSource source = createSource(dir, FileCompression.GZIP);
    SourceRunner runner = new SourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      StageRunner.Output output = runner.runProduce(null, 10);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(3, records.size());
      Assert.assertEquals("A", records.get(0).get("/text").getValueAsString());
      Assert.assertEquals("B", records.get(1).get("/text").getValueAsString());
      Assert.assertEquals("C", records.get(2).get("/text").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testAutomatic() throws Exception {
    String dir = createTestDir();
    createFile(new File(dir, "1.gz"), FileCompression.GZIP);
    createFile(new File(dir, "1.gzip"), FileCompression.GZIP);
    createFile(new File(dir, "1.ZIP"), FileCompression.ZIP);
    createFile(new File(dir, "1.txt"), FileCompression.NONE);
    SpoolDirSource source = createSource(dir, FileCompression.AUTOMATIC);
    SourceRunner runner = new SourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      String offset = null;
      for (int i = 0; i < 4; i++) {
        StageRunner.Output output = runner.runProduce(offset, 10);
        offset = output.getNewOffset();
        List<Record> records = output.getRecords().get("lane");
        Assert.assertNotNull(records);
        Assert.assertEquals(3, records.size());
        Assert.assertEquals("A", records.get(0).get("/text").getValueAsString());
        Assert.assertEquals("B", records.get(1).get("/text").getValueAsString());
        Assert.assertEquals("C", records.get(2).get("/text").getValueAsString());
      }
    } finally {
      runner.runDestroy();
    }
  }

}
