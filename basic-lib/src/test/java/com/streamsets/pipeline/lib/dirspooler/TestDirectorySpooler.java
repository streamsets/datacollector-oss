/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.dirspooler;

import com.codahale.metrics.Meter;
import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class TestDirectorySpooler {
  private File spoolDir;
  private File archiveDir;

  private Source.Context context;

  @Before
  public void setUp() {
    File dir = new File("target", UUID.randomUUID().toString());
    spoolDir = new File(dir, "spool");
    archiveDir = new File(dir, "archive");
    context = ContextInfoCreator.createSourceContext("s", false, ImmutableList.of("a"));
  }

  @Test(expected = IllegalStateException.class)
  public void testNoSpoolDir() {
    DirectorySpooler.Builder builder = DirectorySpooler.builder();
    DirectorySpooler spooler = builder.setContext(context).setDir(spoolDir.getAbsolutePath()).
        setFilePattern("x[0-9]*.log").setMaxSpoolFiles(1).build();
    spooler.init("x2");
    spooler.destroy();
  }

  @Test
  public void testEmptySpoolDir() throws Exception {
    Assert.assertTrue(spoolDir.mkdirs());
    DirectorySpooler.Builder builder = DirectorySpooler.builder();
    DirectorySpooler spooler = builder.setContext(context).setDir(spoolDir.getAbsolutePath()).
        setFilePattern("x[0-9]*.log").setMaxSpoolFiles(1).build();
    spooler.init("x2");
    Assert.assertNull(spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    spooler.destroy();
  }

  @Test
  public void testEmptySpoolDirNoInitialFile() throws Exception {
    Assert.assertTrue(spoolDir.mkdirs());
    DirectorySpooler.Builder builder = DirectorySpooler.builder();
    DirectorySpooler spooler = builder.setContext(context).setDir(spoolDir.getAbsolutePath()).
        setFilePattern("x[0-9]*.log").setMaxSpoolFiles(1).build();
    spooler.init(null);
    Assert.assertNull(spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    spooler.destroy();
  }

  @Test
  public void testEmptySpoolDirNoInitialFileThenFile() throws Exception {
    Assert.assertTrue(spoolDir.mkdirs());
    DirectorySpooler.Builder builder = DirectorySpooler.builder();
    DirectorySpooler spooler = builder.setContext(context).setDir(spoolDir.getAbsolutePath()).
        setFilePattern("x[0-9]*.log").setMaxSpoolFiles(1).build();
    spooler.init(null);
    new FileWriter(new File(spoolDir, "x0.log")).close();
    spooler.finder.run();
    spooler.finder.run();
    Assert.assertNotNull(spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    spooler.destroy();
  }

  @Test
  public void testMatchingFileSpoolDir() throws Exception {
    Assert.assertTrue(spoolDir.mkdirs());
    File logFile = new File(spoolDir, "x2.log").getAbsoluteFile();
    new FileWriter(logFile).close();
    DirectorySpooler.Builder builder = DirectorySpooler.builder();
    DirectorySpooler spooler = builder.setContext(context).setDir(spoolDir.getAbsolutePath()).
        setFilePattern("x[0-9]*.log").setMaxSpoolFiles(1).build();
    spooler.init("x1.log");
    Assert.assertEquals(logFile, spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    Assert.assertNull(spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    Meter meter = context.getMetrics().getMeters().values().iterator().next();
    Assert.assertNotNull(meter);
    Assert.assertTrue(meter.getCount() > 0);
    spooler.destroy();
  }

  @Test
  public void testOlderMatchingFileSpoolDir() throws Exception {
    Assert.assertTrue(spoolDir.mkdirs());
    File logFile = new File(spoolDir, "x2.log").getAbsoluteFile();
    new FileWriter(logFile).close();
    DirectorySpooler.Builder builder = DirectorySpooler.builder();
    DirectorySpooler spooler = builder.setContext(context).setDir(spoolDir.getAbsolutePath()).
        setFilePattern("x[0-9]*.log").setMaxSpoolFiles(1).build();
    spooler.init("x3.log");
    Assert.assertNull(spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    spooler.destroy();
  }

  @Test
  public void testSpoolingOutOfOrderOK() throws Exception {
    Assert.assertTrue(spoolDir.mkdirs());
    File logFile3 = new File(spoolDir, "x3.log").getAbsoluteFile();
    new FileWriter(logFile3).close();
    File logFile1 = new File(spoolDir, "x1.log").getAbsoluteFile();
    new FileWriter(logFile1).close();
    DirectorySpooler.Builder builder = DirectorySpooler.builder();
    DirectorySpooler spooler = builder.setContext(context).setDir(spoolDir.getAbsolutePath()).
        setFilePattern("x[0-9]*.log").setMaxSpoolFiles(3).build();
    spooler.init("x1.log");
    Assert.assertEquals(logFile1, spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    File logFile2 = new File(spoolDir, "x2.log").getAbsoluteFile();
    new FileWriter(logFile2).close();
    spooler.finder.run();
    Assert.assertEquals(logFile2, spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    spooler.finder.run();
    Assert.assertEquals(logFile3, spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    spooler.finder.run();
    Assert.assertNull(spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    spooler.destroy();
  }

  @Test
  public void testOlderMultipleNewerMatchingFileSpoolDir() throws Exception {
    Assert.assertTrue(spoolDir.mkdirs());
    File logFile3 = new File(spoolDir, "x3.log").getAbsoluteFile();
    new FileWriter(logFile3).close();
    File logFile1 = new File(spoolDir, "x1.log").getAbsoluteFile();
    new FileWriter(logFile1).close();
    File logFile2 = new File(spoolDir, "x2.log").getAbsoluteFile();
    new FileWriter(logFile2).close();
    DirectorySpooler.Builder builder = DirectorySpooler.builder();
    DirectorySpooler spooler = builder.setContext(context).setDir(spoolDir.getAbsolutePath()).
        setFilePattern("x[0-9]*.log").setMaxSpoolFiles(3).build();
    spooler.init("x1.log");
    Assert.assertEquals(logFile1, spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    Assert.assertEquals(logFile2, spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    Assert.assertEquals(logFile3, spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    Assert.assertNull(spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    spooler.destroy();
  }

  @Test
  public void testDelete() throws Exception {
    Assert.assertTrue(spoolDir.mkdirs());
    File logFile3 = new File(spoolDir, "x3.log").getAbsoluteFile();
    new FileWriter(logFile3).close();
    File logFile1 = new File(spoolDir, "x1.log").getAbsoluteFile();
    new FileWriter(logFile1).close();
    File logFile2 = new File(spoolDir, "x2.log").getAbsoluteFile();
    new FileWriter(logFile2).close();
    DirectorySpooler.Builder builder = DirectorySpooler.builder();
    DirectorySpooler spooler = builder.setContext(context).setDir(spoolDir.getAbsolutePath()).
        setFilePattern("x[0-9]*.log").setMaxSpoolFiles(3).setPostProcessing(DirectorySpooler.FilePostProcessing.DELETE).
        build();

    Assert.assertEquals(3, spoolDir.list().length);
    Assert.assertTrue(logFile1.exists());
    Assert.assertTrue(logFile2.exists());
    Assert.assertTrue(logFile3.exists());

    spooler.init("x2.log");
    Assert.assertEquals(2, spoolDir.list().length);
    Assert.assertFalse(logFile1.exists());
    Assert.assertTrue(logFile2.exists());
    Assert.assertTrue(logFile3.exists());

    Assert.assertEquals(logFile2, spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    Assert.assertEquals(2, spoolDir.list().length);
    Assert.assertTrue(logFile2.exists());
    Assert.assertTrue(logFile3.exists());

    Assert.assertEquals(logFile3, spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    Assert.assertEquals(1, spoolDir.list().length);
    Assert.assertTrue(logFile3.exists());

    Assert.assertNull(spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    Assert.assertEquals(0, spoolDir.list().length);

    spooler.destroy();
  }

  @Test
  public void testArchive() throws Exception {
    Assert.assertTrue(spoolDir.mkdirs());
    Assert.assertTrue(archiveDir.mkdirs());
    File logFile3 = new File(spoolDir, "x3.log").getAbsoluteFile();
    new FileWriter(logFile3).close();
    File logFile1 = new File(spoolDir, "x1.log").getAbsoluteFile();
    new FileWriter(logFile1).close();
    File logFile2 = new File(spoolDir, "x2.log").getAbsoluteFile();
    new FileWriter(logFile2).close();
    DirectorySpooler.Builder builder = DirectorySpooler.builder();
    DirectorySpooler spooler = builder.setContext(context).setDir(spoolDir.getAbsolutePath()).
        setFilePattern("x[0-9]*.log").setMaxSpoolFiles(3).
        setPostProcessing(DirectorySpooler.FilePostProcessing.ARCHIVE).setArchiveDir(archiveDir.getAbsolutePath()).
        build();

    Assert.assertEquals(3, spoolDir.list().length);
    Assert.assertEquals(0, archiveDir.list().length);
    Assert.assertTrue(logFile1.exists());
    Assert.assertTrue(logFile2.exists());
    Assert.assertTrue(logFile3.exists());

    spooler.init("x2.log");
    Assert.assertEquals(2, spoolDir.list().length);
    Assert.assertEquals(1, archiveDir.list().length);
    Assert.assertFalse(logFile1.exists());
    Assert.assertTrue(logFile2.exists());
    Assert.assertTrue(logFile3.exists());
    Assert.assertTrue(new File(archiveDir, "x1.log").exists());

    Assert.assertEquals(logFile2, spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    Assert.assertEquals(2, spoolDir.list().length);
    Assert.assertEquals(1, archiveDir.list().length);
    Assert.assertTrue(logFile2.exists());
    Assert.assertTrue(logFile3.exists());
    Assert.assertTrue(new File(archiveDir, "x1.log").exists());

    Assert.assertEquals(logFile3, spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    Assert.assertEquals(1, spoolDir.list().length);
    Assert.assertEquals(2, archiveDir.list().length);
    Assert.assertTrue(logFile3.exists());
    Assert.assertTrue(new File(archiveDir, "x1.log").exists());
    Assert.assertTrue(new File(archiveDir, "x2.log").exists());

    Assert.assertNull(spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    Assert.assertEquals(0, spoolDir.list().length);
    Assert.assertEquals(3, archiveDir.list().length);
    Assert.assertTrue(new File(archiveDir, "x1.log").exists());
    Assert.assertTrue(new File(archiveDir, "x2.log").exists());
    Assert.assertTrue(new File(archiveDir, "x3.log").exists());

    spooler.destroy();
  }

  @Test
  public void testRetentionPurging() throws Exception {
    Assert.assertTrue(spoolDir.mkdirs());
    Assert.assertTrue(archiveDir.mkdirs());
    File logFile3 = new File(spoolDir, "x3.log").getAbsoluteFile();
    new FileWriter(logFile3).close();
    File logFile1 = new File(spoolDir, "x1.log").getAbsoluteFile();
    new FileWriter(logFile1).close();
    File logFile2 = new File(spoolDir, "x2.log").getAbsoluteFile();
    new FileWriter(logFile2).close();
    DirectorySpooler.Builder builder = DirectorySpooler.builder();
    DirectorySpooler spooler = builder.setContext(context).setDir(spoolDir.getAbsolutePath()).
        setFilePattern("x[0-9]*.log").setMaxSpoolFiles(3).
        setPostProcessing(DirectorySpooler.FilePostProcessing.ARCHIVE).setArchiveDir(archiveDir.getAbsolutePath()).
        setArchiveRetention(1000, TimeUnit.MILLISECONDS).build();

    Assert.assertEquals(3, spoolDir.list().length);

    spooler.init("x2.log");
    Assert.assertEquals(logFile2, spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    Assert.assertEquals(logFile3, spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    Assert.assertNull(spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    Assert.assertEquals(0, spoolDir.list().length);
    Assert.assertEquals(3, archiveDir.list().length);
    Assert.assertTrue(new File(archiveDir, "x1.log").exists());
    Assert.assertTrue(new File(archiveDir, "x2.log").exists());
    Assert.assertTrue(new File(archiveDir, "x3.log").exists());

    // no purging
    spooler.purger.run();
    Assert.assertEquals(3, archiveDir.list().length);

    // purging
    Thread.sleep(1100);
    spooler.purger.run();
    Assert.assertEquals(0, archiveDir.list().length);

    spooler.destroy();
  }

}