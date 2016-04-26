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
package com.streamsets.pipeline.lib.dirspooler;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@RunWith(PowerMockRunner.class)
@PrepareForTest(DirectorySpooler.class)
public class TestDirectorySpooler {
  private File spoolDir;
  private File archiveDir;

  private Source.Context context;

  @Before
  public void setUp() {
    File dir = new File("target", UUID.randomUUID().toString());
    spoolDir = new File(dir, "spool");
    archiveDir = new File(dir, "archive");
    context = ContextInfoCreator.createSourceContext("s", false, OnRecordError.TO_ERROR, ImmutableList.of("a"));
  }

  private DirectorySpooler.Builder initializeAndGetBuilder() {
    return DirectorySpooler.builder()
        .setContext(context)
        .setDir(spoolDir.getAbsolutePath())
        .setFilePattern("x[0-9]*.log");
  }

  @Test(expected = IllegalStateException.class)
  public void testNoSpoolDirWithoutWaiting() {
    DirectorySpooler.Builder builder = initializeAndGetBuilder()
        .setMaxSpoolFiles(1);
    DirectorySpooler spooler = builder.build();
    spooler.init("x2");
    spooler.destroy();
  }

  @Test
  public void testNoSpoolDirWithWaiting() throws Exception{
    DirectorySpooler.Builder builder = initializeAndGetBuilder()
        .setMaxSpoolFiles(1)
        .waitForPathAppearance(true);

    final DirectorySpooler spooler = builder.build();
    spooler.init("x2");
    ScheduledExecutorService schedService = new SafeScheduledExecutorService(1, "One Time pooler");
    boolean test_passed = false;
    try {
      Callable<Boolean> task = new Callable<Boolean>(){
        public Boolean call() {
          try {
            return (spooler.poolForFile(0, TimeUnit.MILLISECONDS) != null);
          }
          catch (InterruptedException e) {
            //Task Interrupted as it did not finish the task.
          }
          return false;
        }
      };
      ScheduledFuture<Boolean> test_status = schedService.schedule(task, 0, TimeUnit.MILLISECONDS);
      Assert.assertTrue(spoolDir.mkdirs());

      File logFile = new File(spoolDir, "x2.log").getAbsoluteFile();
      new FileWriter(logFile).close();
      //Wait for 10 secs at max and then report false;
      test_passed = test_status.get(10000, TimeUnit.MILLISECONDS);

    } finally {
      schedService.shutdownNow();
    }
    Assert.assertTrue("Test did not pass, Spooler did not find files", test_passed);
  }


  @Test
  public void testEmptySpoolDir() throws Exception {
    Assert.assertTrue(spoolDir.mkdirs());
    DirectorySpooler.Builder builder = initializeAndGetBuilder()
        .setMaxSpoolFiles(1);
    final DirectorySpooler spooler = builder.build();
    spooler.init("x2");
    Assert.assertNull(spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    spooler.destroy();
  }

  @Test
  public void testEmptySpoolDirNoInitialFile() throws Exception {
    Assert.assertTrue(spoolDir.mkdirs());
    DirectorySpooler.Builder builder = initializeAndGetBuilder()
        .setMaxSpoolFiles(1);
    DirectorySpooler spooler = builder.build();

    spooler.init(null);
    Assert.assertNull(spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    spooler.destroy();
  }

  @Test
  public void testEmptySpoolDirNoInitialFileThenFile() throws Exception {
    Assert.assertTrue(spoolDir.mkdirs());
    DirectorySpooler.Builder builder = initializeAndGetBuilder()
        .setMaxSpoolFiles(1);
    DirectorySpooler spooler = builder.build();

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
    DirectorySpooler.Builder builder = initializeAndGetBuilder()
        .setMaxSpoolFiles(1);
    DirectorySpooler spooler = builder.build();

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

    DirectorySpooler.Builder builder = initializeAndGetBuilder()
        .setMaxSpoolFiles(1);
    DirectorySpooler spooler = builder.build();

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

    DirectorySpooler.Builder builder = initializeAndGetBuilder()
        .setMaxSpoolFiles(3);
    DirectorySpooler spooler = builder.build();

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
    DirectorySpooler.Builder builder = initializeAndGetBuilder()
        .setMaxSpoolFiles(3);
    DirectorySpooler spooler = builder.build();

    spooler.init("x1.log");
    Assert.assertEquals(logFile1, spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    Assert.assertEquals(logFile2, spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    Assert.assertEquals(logFile3, spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    Assert.assertNull(spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    spooler.destroy();
  }

  @Test
  public void testOlderMultipleNewerMatchingFileSpoolDirTimestamp() throws Exception {
    Assert.assertTrue(spoolDir.mkdirs());
    File logFile3 = new File(spoolDir, "x3.log").getAbsoluteFile();
    new FileWriter(logFile3).close();
    long baseTime = 1461867389000L;
    Assert.assertTrue(logFile3.setLastModified(baseTime + 1000));
    File logFile1 = new File(spoolDir, "x1.log").getAbsoluteFile();
    new FileWriter(logFile1).close();
    Assert.assertTrue(logFile1.setLastModified(baseTime + 2000));
    System.out.println("Last Modified: " + logFile1.lastModified());
    File logFile2 = new File(spoolDir, "x2.log").getAbsoluteFile();
    new FileWriter(logFile2).close();
    Assert.assertTrue(logFile2.setLastModified(baseTime + 3000));
    DirectorySpooler.Builder builder = initializeAndGetBuilder()
        .setMaxSpoolFiles(3);
    DirectorySpooler spooler = builder.setUseLastModifiedTimestamp(true).build();

    spooler.init("x1.log");
    Assert.assertEquals(logFile1, spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    Assert.assertEquals(logFile2, spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    Assert.assertNull(spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    spooler.destroy();
  }

  @Test
  public void testOlderMultipleNewerMatchingFileSpoolDirSameTimestamp() throws Exception {
    Assert.assertTrue(spoolDir.mkdirs());
    File logFile3 = new File(spoolDir, "x3.log").getAbsoluteFile();
    new FileWriter(logFile3).close();
    long baseTime = 1461867389000L;
    Assert.assertTrue(logFile3.setLastModified(baseTime + 1000));
    File logFile1 = new File(spoolDir, "x1.log").getAbsoluteFile();
    new FileWriter(logFile1).close();
    Assert.assertTrue(logFile1.setLastModified(baseTime + 2000));
    System.out.println("Last Modified: " + logFile1.lastModified());
    File logFile2 = new File(spoolDir, "x2.log").getAbsoluteFile();
    new FileWriter(logFile2).close();
    Assert.assertTrue(logFile2.setLastModified(baseTime + 3000));
    File logFile4 = new File(spoolDir, "x4.log").getAbsoluteFile();
    new FileWriter(logFile4).close();
    Assert.assertTrue(logFile4.setLastModified(baseTime + 3000));
    DirectorySpooler.Builder builder = initializeAndGetBuilder()
        .setMaxSpoolFiles(4);
    DirectorySpooler spooler = builder.setUseLastModifiedTimestamp(true).build();

    spooler.init("x1.log");
    Assert.assertEquals(logFile1, spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    Assert.assertEquals(logFile2, spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    Assert.assertEquals(logFile4, spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    Assert.assertNull(spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    spooler.destroy();
  }

  @Test
  public void testOlderMultipleNewerMatchingFileSpoolDirNewFiles() throws Exception {
    Assert.assertTrue(spoolDir.mkdirs());
    File logFile3 = new File(spoolDir, "x3.log").getAbsoluteFile();
    new FileWriter(logFile3).close();
    long baseTime = 1461867389000L;
    Assert.assertTrue(logFile3.setLastModified(baseTime + 1000));
    File logFile1 = new File(spoolDir, "x1.log").getAbsoluteFile();
    new FileWriter(logFile1).close();
    Assert.assertTrue(logFile1.setLastModified(baseTime + 2000));
    System.out.println("Last Modified: " + logFile1.lastModified());
    File logFile2 = new File(spoolDir, "x2.log").getAbsoluteFile();
    new FileWriter(logFile2).close();
    Assert.assertTrue(logFile2.setLastModified(baseTime + 3000));
    DirectorySpooler.Builder builder = initializeAndGetBuilder()
        .setMaxSpoolFiles(4);
    DirectorySpooler spooler = builder.setUseLastModifiedTimestamp(true).build();

    spooler.init("x1.log");
    Assert.assertEquals(logFile1, spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    Assert.assertEquals(logFile2, spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    Assert.assertNull(spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    //x4 and x5 have same timestamp, so order should be exactly the same.
    File logFile5 = new File(spoolDir, "x5.log").getAbsoluteFile();
    new FileWriter(logFile5).close();
    Assert.assertTrue(logFile5.setLastModified(baseTime + 3000));
    File logFile4 = new File(spoolDir, "x4.log").getAbsoluteFile();
    new FileWriter(logFile4).close();
    Assert.assertTrue(logFile4.setLastModified(baseTime + 3000));
    spooler.finder.run();
    Assert.assertEquals(logFile4, spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    Assert.assertEquals(logFile5, spooler.poolForFile(0, TimeUnit.MILLISECONDS));
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

    DirectorySpooler.Builder builder = initializeAndGetBuilder()
        .setMaxSpoolFiles(3)
        .setPostProcessing(DirectorySpooler.FilePostProcessing.DELETE);
    DirectorySpooler spooler = builder.build();


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

    DirectorySpooler.Builder builder = initializeAndGetBuilder()
        .setMaxSpoolFiles(3)
        .setPostProcessing(DirectorySpooler.FilePostProcessing.ARCHIVE)
        .setArchiveDir(archiveDir.getAbsolutePath());
    DirectorySpooler spooler = builder.build();

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

    DirectorySpooler.Builder builder = initializeAndGetBuilder()
        .setMaxSpoolFiles(3)
        .setPostProcessing(DirectorySpooler.FilePostProcessing.ARCHIVE)
        .setArchiveDir(archiveDir.getAbsolutePath())
        .setArchiveRetention(1000, TimeUnit.MILLISECONDS);

    DirectorySpooler spooler = builder.build();
    Assert.assertEquals(3, spoolDir.list().length);

    spooler.init("x2.log");
    Assert.assertEquals(logFile2, spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    Assert.assertEquals(logFile3, spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    Assert.assertNull(spooler.poolForFile(0, TimeUnit.MILLISECONDS));
    Assert.assertEquals(0, spoolDir.list().length);
    Assert.assertEquals(3, archiveDir.list().length);
    File archiveLog1 = new File(archiveDir, "x1.log");
    File archiveLog2 = new File(archiveDir, "x2.log");
    File archiveLog3 = new File(archiveDir, "x3.log");
    Assert.assertTrue(archiveLog1.exists());
    Assert.assertTrue(archiveLog2.exists());
    Assert.assertTrue(archiveLog3.exists());

    // Be paranoid and update the mtimes to ensure the first purge is < 1 second from mtime.
    Assert.assertTrue(archiveLog1.setLastModified(System.currentTimeMillis()));
    Assert.assertTrue(archiveLog2.setLastModified(System.currentTimeMillis()));
    Assert.assertTrue(archiveLog3.setLastModified(System.currentTimeMillis()));
    // no purging
    spooler.purger.run();
    Assert.assertEquals(3, archiveDir.list().length);

    // purging
    Thread.sleep(1100);
    spooler.purger.run();
    Assert.assertEquals(0, archiveDir.list().length);

    spooler.destroy();
  }

  @Test
  public void testSpoolQueueMetrics() throws Exception {
    Assert.assertTrue(spoolDir.mkdirs());
    Assert.assertTrue(archiveDir.mkdirs());

    DirectorySpooler.Builder builder = initializeAndGetBuilder()
        .setMaxSpoolFiles(10)
        .setPostProcessing(DirectorySpooler.FilePostProcessing.ARCHIVE)
        .setArchiveDir(archiveDir.getAbsolutePath())
        .setArchiveRetention(1000, TimeUnit.MILLISECONDS);

    DirectorySpooler spooler = PowerMockito.spy(builder.build());

    final List<File> files = Arrays.asList(
        new File(spoolDir, "x1.log"),
        new File(spoolDir, "x2.log"),
        new File(spoolDir, "x3.log"),
        new File(spoolDir, "x4.log"),
        new File(spoolDir, "x5.log"),
        new File(spoolDir, "x6.log"),
        new File(spoolDir, "x7.log"),
        new File(spoolDir, "x8.log")
    );
    for (File file : files) {
      new FileWriter(file.getAbsoluteFile()).close();
    }

    //First file is x2.log
    spooler.init("x2.log");

    //None of the files are pooled for processing, starting from x2 till x8 there are 7 files.
    Counter spoolQueueCounter = (Counter) Whitebox.getInternalState(spooler, "pendingFilesCounter");
    Assert.assertEquals(7L, spoolQueueCounter.getCount());


    //We would get x2.log for processing, leaving x3-x8 as pending files (6 files)
    spooler.poolForFile(1000, TimeUnit.MILLISECONDS);

    spoolQueueCounter = (Counter) Whitebox.getInternalState(spooler, "pendingFilesCounter");
    Assert.assertEquals(6L, spoolQueueCounter.getCount());


    for (int i = 0 ;i < 6 ;i++) {
      spooler.poolForFile(1000, TimeUnit.MILLISECONDS);
    }

    //All files are pooled, pending files should be 0.
    spoolQueueCounter = (Counter) Whitebox.getInternalState(spooler, "pendingFilesCounter");
    Assert.assertEquals(0L, spoolQueueCounter.getCount());

    Thread.sleep(1200);
    Assert.assertEquals(7L, archiveDir.list().length);

    //Purge everything.
    spooler.purger.run();
    Assert.assertEquals(0, archiveDir.list().length);
    spooler.destroy();
  }
}