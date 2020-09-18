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
package com.streamsets.pipeline.lib.dirspooler;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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

import static com.streamsets.pipeline.lib.dirspooler.PathMatcherMode.GLOB;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest(DirectorySpooler.class)
@PowerMockIgnore({
    "jdk.internal.reflect.*"
})
public class TestDirectorySpooler {
  private File spoolDir;
  private File archiveDir;

  private PushSource.Context context;
  private PushSource.Context contextInPreview;

  private WrappedFileSystem fs = new LocalFileSystem("*", GLOB);

  private long intervalMillis = 5000;

  @Before
  public void setUp() {
    File dir = new File("target", UUID.randomUUID().toString());
    spoolDir = new File(dir, "spool");
    archiveDir = new File(dir, "archive");
    context = (PushSource.Context) ContextInfoCreator.createSourceContext("s", false, OnRecordError.TO_ERROR, ImmutableList.of("a"));
    contextInPreview = (PushSource.Context) ContextInfoCreator.createSourceContext("s", true, OnRecordError.TO_ERROR, ImmutableList.of("a"));
  }

  private DirectorySpooler.Builder initializeAndGetBuilder() throws IOException {
    return new DirectorySpooler.Builder()
        .setContext(context)
        .setWrappedFileSystem(new LocalFileSystem("x[0-9]*.log", GLOB))
        .setDir(spoolDir.getAbsolutePath())
        .setFilePattern("x[0-9]*.log");
  }

  @Test(expected = IllegalStateException.class)
  public void testNoSpoolDirWithoutWaiting() throws IOException {
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
            return (spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS) != null);
          }
          catch (InterruptedException e) {
            //Task Interrupted as it did not finish the task.
          }
          return false;
        }
      };
      ScheduledFuture<Boolean> test_status = schedService.schedule(task, 0, TimeUnit.MILLISECONDS);
      assertTrue(spoolDir.mkdirs());

      File logFile = new File(spoolDir, "x2.log").getAbsoluteFile();
      new FileWriter(logFile).close();
      //Wait for 10 secs at max and then report false;
      test_passed = test_status.get(10000, TimeUnit.MILLISECONDS);

    } finally {
      spooler.destroy();
      schedService.shutdownNow();
    }
    assertTrue("Test did not pass, Spooler did not find files", test_passed);
  }


  @Test
  public void testEmptySpoolDir() throws Exception {
    assertTrue(spoolDir.mkdirs());
    DirectorySpooler.Builder builder = initializeAndGetBuilder()
        .setMaxSpoolFiles(1);
    final DirectorySpooler spooler = builder.build();
    spooler.init("x2");
    Assert.assertNull(spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS));
    spooler.destroy();
  }


  @Test
  public void testEmptySpoolDirNoInitialFile() throws Exception {
    assertTrue(spoolDir.mkdirs());
    DirectorySpooler.Builder builder = initializeAndGetBuilder()
        .setMaxSpoolFiles(1);
    DirectorySpooler spooler = builder.build();

    spooler.init(null);
    Assert.assertNull(spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS));
    spooler.destroy();
  }

  @Test
  public void testEmptySpoolDirNoInitialFileThenFile() throws Exception {
    assertTrue(spoolDir.mkdirs());
    DirectorySpooler.Builder builder = initializeAndGetBuilder()
        .setMaxSpoolFiles(1);
    DirectorySpooler spooler = builder.build();

    spooler.init(null);
    new FileWriter(new File(spoolDir, "x0.log")).close();
    spooler.finder.run();
    spooler.finder.run();
    Assert.assertNotNull(spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS));
    spooler.destroy();
  }

  @Test
  public void testMatchingFileSpoolDir() throws Exception {
    assertTrue(spoolDir.mkdirs());
    File logFile = new File(spoolDir, "x2.log").getAbsoluteFile();
    new FileWriter(logFile).close();
    DirectorySpooler.Builder builder = initializeAndGetBuilder()
        .setMaxSpoolFiles(1);
    DirectorySpooler spooler = builder.build();

    spooler.init("x1.log");
    Assert.assertEquals(logFile.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    Assert.assertNull(spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS));
    Meter meter = context.getMetrics().getMeters().values().iterator().next();
    Assert.assertNotNull(meter);
    assertTrue(meter.getCount() > 0);
    spooler.destroy();
  }

  @Test
  public void testOlderMatchingFileSpoolDir() throws Exception {
    assertTrue(spoolDir.mkdirs());
    File logFile = new File(spoolDir, "x2.log").getAbsoluteFile();
    new FileWriter(logFile).close();

    DirectorySpooler.Builder builder = initializeAndGetBuilder()
        .setMaxSpoolFiles(1);
    DirectorySpooler spooler = builder.build();

    spooler.init("x3.log");
    Assert.assertNull(spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS));
    spooler.destroy();
  }

  @Test
  public void testSpoolingOutOfOrderOK() throws Exception {
    assertTrue(spoolDir.mkdirs());
    File logFile3 = new File(spoolDir, "x3.log").getAbsoluteFile();
    new FileWriter(logFile3).close();
    File logFile1 = new File(spoolDir, "x1.log").getAbsoluteFile();
    new FileWriter(logFile1).close();

    DirectorySpooler.Builder builder = initializeAndGetBuilder()
        .setMaxSpoolFiles(3);
    DirectorySpooler spooler = builder.build();

    spooler.init("x1.log");
    Assert.assertEquals(logFile1.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    File logFile2 = new File(spoolDir, "x2.log").getAbsoluteFile();
    new FileWriter(logFile2).close();
    spooler.finder.run();
    Assert.assertEquals(logFile2.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    spooler.finder.run();
    Assert.assertEquals(logFile3.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    spooler.finder.run();
    Assert.assertNull(spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS));
    spooler.destroy();
  }

  @Test
  public void testOlderMultipleNewerMatchingFileSpoolDir() throws Exception {
    assertTrue(spoolDir.mkdirs());
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
    Assert.assertEquals(logFile1.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    Assert.assertEquals(logFile2.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    Assert.assertEquals(logFile3.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    Assert.assertNull(spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS));
    spooler.destroy();
  }

  @Test
  public void testOlderMultipleNewerMatchingFileSpoolDirTimestamp() throws Exception {
    assertTrue(spoolDir.mkdirs());
    File logFile3 = new File(spoolDir, "x3.log").getAbsoluteFile();
    new FileWriter(logFile3).close();
    // for ctime delays, there's no way to set ctime (change timestamp) explicitly by rule
    Thread.sleep(1000L);

    File logFile1 = new File(spoolDir, "x1.log").getAbsoluteFile();
    new FileWriter(logFile1).close();
    // for ctime delays, there's no way to set ctime (change timestamp) explicitly by rule
    Thread.sleep(1000L);

    File logFile2 = new File(spoolDir, "x2.log").getAbsoluteFile();
    new FileWriter(logFile2).close();

    DirectorySpooler.Builder builder = initializeAndGetBuilder()
        .setMaxSpoolFiles(3);
    DirectorySpooler spooler = builder.setUseLastModifiedTimestamp(true).build();

    spooler.init("x1.log");
    Assert.assertEquals(logFile1.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    Assert.assertEquals(logFile2.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    Assert.assertNull(spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS));
    spooler.destroy();
  }

  @Test
  public void testOlderMultipleNewerMatchingFileSpoolDirSameTimestamp() throws Exception {
    assertTrue(spoolDir.mkdirs());
    File logFile3 = new File(spoolDir, "x3.log").getAbsoluteFile();
    new FileWriter(logFile3).close();
    // for ctime delays, there's no way to set ctime (change timestamp) explicitly by rule
    Thread.sleep(1000L);

    File logFile1 = new File(spoolDir, "x1.log").getAbsoluteFile();
    new FileWriter(logFile1).close();
    // for ctime delays, there's no way to set ctime (change timestamp) explicitly by rule
    Thread.sleep(500L);

    File logFile2 = new File(spoolDir, "x2.log").getAbsoluteFile();
    new FileWriter(logFile2).close();
    // for ctime delays, there's no way to set ctime (change timestamp) explicitly by rule
    Thread.sleep(500L);

    File logFile4 = new File(spoolDir, "x4.log").getAbsoluteFile();
    new FileWriter(logFile4).close();

    DirectorySpooler.Builder builder = initializeAndGetBuilder()
        .setMaxSpoolFiles(4);
    DirectorySpooler spooler = builder.setUseLastModifiedTimestamp(true).build();

    spooler.init("x1.log");
    Assert.assertEquals(logFile1.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    Assert.assertEquals(logFile2.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    Assert.assertEquals(logFile4.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    Assert.assertNull(spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS));
    spooler.destroy();
  }

  @Test
  public void testOlderMultipleNewerMatchingFileSpoolDirNewFiles() throws Exception {
    assertTrue(spoolDir.mkdirs());
    File logFile3 = new File(spoolDir, "x3.log").getAbsoluteFile();
    new FileWriter(logFile3).close();

    // for ctime delays, there's no way to set ctime (change timestamp) explicitly by rule
    Thread.sleep(1000L);

    File logFile1 = new File(spoolDir, "x1.log").getAbsoluteFile();
    new FileWriter(logFile1).close();

    // for ctime delays, there's no way to set ctime (change timestamp) explicitly by rule
    Thread.sleep(100L);

    File logFile2 = new File(spoolDir, "x2.log").getAbsoluteFile();
    new FileWriter(logFile2).close();

    DirectorySpooler.Builder builder = initializeAndGetBuilder()
        .setMaxSpoolFiles(4);
    DirectorySpooler spooler = builder.setUseLastModifiedTimestamp(true).build();

    spooler.init("x1.log");
    Assert.assertEquals(logFile1.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    Assert.assertEquals(logFile2.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    Assert.assertNull(spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS));
    //x4 and x5 have same timestamp, so order should be exactly the same.
    File logFile5 = new File(spoolDir, "x5.log").getAbsoluteFile();
    new FileWriter(logFile5).close();
    File logFile4 = new File(spoolDir, "x4.log").getAbsoluteFile();
    new FileWriter(logFile4).close();

    spooler.finder.run();
    Assert.assertEquals(logFile4.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    Assert.assertEquals(logFile5.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    Assert.assertNull(spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS));
    spooler.destroy();
  }

  @Test
  public void testDelete() throws Exception {
    assertTrue(spoolDir.mkdirs());
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
    assertTrue(logFile1.exists());
    assertTrue(logFile2.exists());
    assertTrue(logFile3.exists());

    spooler.init("x2.log");
    Assert.assertEquals(3, spoolDir.list().length);
    assertTrue(logFile1.exists());
    assertTrue(logFile2.exists());
    assertTrue(logFile3.exists());

    Assert.assertEquals(logFile2.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    Assert.assertEquals(3, spoolDir.list().length);
    assertTrue(logFile1.exists());
    assertTrue(logFile2.exists());
    assertTrue(logFile3.exists());

    WrappedFileSystem fs = new LocalFileSystem("*", GLOB);

    Assert.assertEquals(logFile3.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    spooler.doPostProcessing(fs.getFile(logFile2.toPath().toString()));
    Assert.assertEquals(2, spoolDir.list().length);
    assertTrue(logFile1.exists());
    assertFalse(logFile2.exists());
    assertTrue(logFile3.exists());

    Assert.assertNull(spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS));
    spooler.doPostProcessing(fs.getFile(logFile3.toPath().toString()));
    Assert.assertEquals(1, spoolDir.list().length);
    assertTrue(logFile1.exists());
    assertFalse(logFile2.exists());
    assertFalse(logFile3.exists());

    spooler.destroy();
  }

  @Test
  public void testDeleteInPreview() throws Exception {
    assertTrue(spoolDir.mkdirs());
    File logFile3 = new File(spoolDir, "x3.log").getAbsoluteFile();
    new FileWriter(logFile3).close();
    File logFile1 = new File(spoolDir, "x1.log").getAbsoluteFile();
    new FileWriter(logFile1).close();
    File logFile2 = new File(spoolDir, "x2.log").getAbsoluteFile();
    new FileWriter(logFile2).close();

    DirectorySpooler.Builder builder = initializeAndGetBuilder()
      .setContext(contextInPreview)
      .setMaxSpoolFiles(3)
      .setPostProcessing(DirectorySpooler.FilePostProcessing.DELETE);
    DirectorySpooler spooler = builder.build();


    Assert.assertEquals(3, spoolDir.list().length);
    assertTrue(logFile1.exists());
    assertTrue(logFile2.exists());
    assertTrue(logFile3.exists());

    spooler.init("x2.log");
    Assert.assertEquals(3, spoolDir.list().length);
    assertTrue(logFile1.exists());
    assertTrue(logFile2.exists());
    assertTrue(logFile3.exists());

    Assert.assertEquals(logFile2.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    Assert.assertEquals(3, spoolDir.list().length);
    assertTrue(logFile1.exists());
    assertTrue(logFile2.exists());
    assertTrue(logFile3.exists());

    spooler.destroy();
  }

  @Test
  public void testArchiveWithoutStartingFile() throws Exception {
    assertTrue(spoolDir.mkdirs());
    assertTrue(archiveDir.mkdirs());

    File logFile1 = new File(spoolDir, "x1.log").getAbsoluteFile();
    new FileWriter(logFile1).close();

    File logFile2 = new File(spoolDir, "x2.log").getAbsoluteFile();
    new FileWriter(logFile2).close();

    File logFile3 = new File(spoolDir, "x3.log").getAbsoluteFile();
    new FileWriter(logFile3).close();

    long currentMills = System.currentTimeMillis();

    //Set the last modified time to be different seconds for each file
    Files.setLastModifiedTime(
        Paths.get(logFile1.getAbsolutePath()),
        FileTime.from(currentMills - 3000, TimeUnit.MILLISECONDS)
    );
    Files.setLastModifiedTime(
        Paths.get(logFile2.getAbsolutePath()),
        FileTime.from(currentMills - 2000, TimeUnit.MILLISECONDS)
    );
    Files.setLastModifiedTime(
        Paths.get(logFile3.getAbsolutePath()),
        FileTime.from(currentMills - 1000, TimeUnit.MILLISECONDS)
    );

    //Directory has the most recent timestamp, because with every new file directory's modified time changes.
    Files.setLastModifiedTime(
        Paths.get(spoolDir.getAbsolutePath()),
        FileTime.from(currentMills, TimeUnit.MILLISECONDS)
    );

    DirectorySpooler.Builder builder = initializeAndGetBuilder()
        .setMaxSpoolFiles(5)
        .setUseLastModifiedTimestamp(true)
        .setFilePattern("*")
        .setPostProcessing(DirectorySpooler.FilePostProcessing.ARCHIVE)
        .setArchiveDir(archiveDir.getAbsolutePath());
    DirectorySpooler spooler = builder.build();

    Assert.assertEquals(3, spoolDir.list().length);
    Assert.assertEquals(0, archiveDir.list().length);
    assertTrue(logFile1.exists());
    assertTrue(logFile2.exists());
    assertTrue(logFile3.exists());

    //No starting file
    spooler.init("");
    //Nothing should be archived.
    Assert.assertEquals(3, spoolDir.list().length);
    Assert.assertEquals(0, archiveDir.list().length);
    assertTrue(logFile1.exists());
    assertTrue(logFile2.exists());
    assertTrue(logFile3.exists());

    spooler.destroy();
  }

  @Test
  public void testArchive() throws Exception {
    assertTrue(spoolDir.mkdirs());
    assertTrue(archiveDir.mkdirs());
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
    assertTrue(logFile1.exists());
    assertTrue(logFile2.exists());
    assertTrue(logFile3.exists());

    spooler.init("x2.log");
    Assert.assertEquals(3, spoolDir.list().length);
    Assert.assertEquals(0, archiveDir.list().length);
    assertTrue(logFile1.exists());
    assertTrue(logFile2.exists());
    assertTrue(logFile3.exists());

    Assert.assertEquals(logFile2.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    Assert.assertEquals(3, spoolDir.list().length);
    Assert.assertEquals(0, archiveDir.list().length);
    assertTrue(logFile1.exists());
    assertTrue(logFile2.exists());
    assertTrue(logFile3.exists());
    WrappedFileSystem fs = new LocalFileSystem("*", GLOB);

    spooler.doPostProcessing(fs.getFile(logFile2.toPath().toString()));

    Assert.assertEquals(logFile3.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    Assert.assertEquals(2, spoolDir.list().length);
    Assert.assertEquals(1, archiveDir.list().length);
    assertTrue(logFile1.exists());
    assertTrue(logFile3.exists());
    assertTrue(new File(archiveDir, "x2.log").exists());
    spooler.doPostProcessing(fs.getFile(logFile3.toPath().toString()));

    Assert.assertNull(spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS));
    Assert.assertEquals(1, spoolDir.list().length);
    Assert.assertEquals(2, archiveDir.list().length);
    assertTrue(logFile1.exists());
    assertTrue(new File(archiveDir, "x2.log").exists());
    assertTrue(new File(archiveDir, "x3.log").exists());

    spooler.destroy();
  }

  // Fails without SDC-3496 (with NoSuchFileException)
  @Test
  public void testMissingFileDoesNotThrow() throws Exception {
    assertTrue(spoolDir.mkdirs());

    File logFile2 = new File(spoolDir, "x2.log").getAbsoluteFile();
    new FileWriter(logFile2).close();
    logFile2.setLastModified(System.currentTimeMillis() - 10000);
    File logFile3 = new File(spoolDir, "x3.log").getAbsoluteFile();
    new FileWriter(logFile3).close();
    logFile2.setLastModified(System.currentTimeMillis() - 9000);

    DirectorySpooler.Builder builder = initializeAndGetBuilder()
        .setUseLastModifiedTimestamp(true)
        .setMaxSpoolFiles(3);
    DirectorySpooler spooler = builder.build();

    Assert.assertEquals(2, spoolDir.list().length);
    assertTrue(logFile2.exists());
    assertTrue(logFile3.exists());

    spooler.init("x1.log");
    Assert.assertEquals(logFile2.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    Assert.assertEquals(logFile3.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    Assert.assertNull(spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS));
    spooler.destroy();
  }

  @Test
  public void testRetentionPurging() throws Exception {
    assertTrue(spoolDir.mkdirs());
    assertTrue(archiveDir.mkdirs());
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
    Assert.assertEquals(logFile2.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    spooler.doPostProcessing(fs.getFile(logFile2.toPath().toString()));
    Assert.assertEquals(logFile3.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    spooler.doPostProcessing(fs.getFile(logFile3.toPath().toString()));
    Assert.assertNull(spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS));
    spooler.doPostProcessing(fs.getFile(logFile3.toPath().toString()));
    Assert.assertEquals(1, spoolDir.list().length);
    Assert.assertEquals(2, archiveDir.list().length);
    File archiveLog2 = new File(archiveDir, "x2.log");
    File archiveLog3 = new File(archiveDir, "x3.log");
    assertTrue(logFile1.exists());
    assertTrue(archiveLog2.exists());
    assertTrue(archiveLog3.exists());

    // Be paranoid and update the mtimes to ensure the first purge is < 1 second from mtime.
    assertTrue(archiveLog2.setLastModified(System.currentTimeMillis()));
    assertTrue(archiveLog3.setLastModified(System.currentTimeMillis()));
    // no purging
    spooler.purger.run();
    Assert.assertEquals(2, archiveDir.list().length);

    // purging
    Thread.sleep(1100);
    spooler.purger.run();
    Assert.assertEquals(0, archiveDir.list().length);

    spooler.destroy();
  }

  @Test
  public void testSpoolQueueMetrics() throws Exception {
    assertTrue(spoolDir.mkdirs());
    assertTrue(archiveDir.mkdirs());

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
      WrappedFile file = spooler.poolForFile(1000, TimeUnit.MILLISECONDS);
      spooler.doPostProcessing(file);
    }

    //All files are pooled, pending files should be 0.
    spoolQueueCounter = (Counter) Whitebox.getInternalState(spooler, "pendingFilesCounter");
    Assert.assertEquals(0L, spoolQueueCounter.getCount());

    Thread.sleep(1200);
    Assert.assertEquals(6L, archiveDir.list().length);

    //Purge everything.
    spooler.purger.run();
    Assert.assertEquals(0, archiveDir.list().length);
    spooler.destroy();
  }

  @Test
  public void testCreatePathMatcher() throws Exception {
    WrappedFileSystem fs = new LocalFileSystem("*.[!a][!b][!c]", PathMatcherMode.GLOB);
    //PathMatcher glob = DirectorySpooler.createPathMatcher("*.[!a][!b][!c]", PathMatcherMode.GLOB);

    //FileSystem fs = FileSystems.getDefault();

    assertTrue(fs.patternMatches("name.txt"));
    assertFalse(fs.patternMatches("name.abc"));

    fs = new LocalFileSystem(".+(?<!abc)$", PathMatcherMode.REGEX);
    //PathMatcher regex = DirectorySpooler.createPathMatcher(".+(?<!abc)$", PathMatcherMode.REGEX);
    assertTrue(fs.patternMatches("name.txt"));
    assertFalse(fs.patternMatches("name.abc"));
  }

  @Test
  public void testPostProcessInLastModifiedTimeStamp() throws Exception {
    assertTrue(spoolDir.mkdirs());

    // only the pattern matching file is post-process
    File logFile1 = new File(spoolDir, "x1").getAbsoluteFile();
    logFile1.setLastModified(System.currentTimeMillis() - 1000000);
    new FileWriter(logFile1).close();

    File logFile2 = new File(spoolDir, "x2.log").getAbsoluteFile();
    logFile2.setLastModified(System.currentTimeMillis() - 1000000);
    new FileWriter(logFile2).close();

    DirectorySpooler.Builder builder = initializeAndGetBuilder()
        .setUseLastModifiedTimestamp(true)
        .setPostProcessing(DirectorySpooler.FilePostProcessing.DELETE)
        .setMaxSpoolFiles(3);
    DirectorySpooler spooler = builder.build();

    spooler.init("");
    Assert.assertEquals(logFile2.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    Assert.assertEquals(null, spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS));
    Assert.assertTrue(logFile1.exists());
    spooler.destroy();
  }

  @Test
  public void testPostProcessInLastModifiedTimeStampWithMultipleThreads() throws Exception {
    assertTrue(spoolDir.mkdirs());

    // only the pattern matching file is post-process
    File logFile1 = new File(spoolDir, "x1").getAbsoluteFile();
    logFile1.setLastModified(System.currentTimeMillis() - 1000000);
    new FileWriter(logFile1).close();

    File logFile2 = new File(spoolDir, "x2.log").getAbsoluteFile();
    logFile2.setLastModified(System.currentTimeMillis() - 1000000);
    new FileWriter(logFile2).close();

    DirectorySpooler.Builder builder = initializeAndGetBuilder()
        .setUseLastModifiedTimestamp(true)
        .setPostProcessing(DirectorySpooler.FilePostProcessing.DELETE)
        .setMaxSpoolFiles(10);
    DirectorySpooler spooler = builder.build();

    // if the starting file is archived, should proceed the next file
    spooler.init("x0.log");
    Assert.assertEquals(logFile2.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    Assert.assertEquals(null, spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS));
    Assert.assertTrue(logFile1.exists());
    spooler.destroy();
  }

  @Test
  public void testLargeMaximumFilesConfig() throws Exception {
    assertTrue(spoolDir.mkdirs());

    File logFile1 = new File(spoolDir, "x1.log").getAbsoluteFile();
    new FileWriter(logFile1).close();

    File logFile2 = new File(spoolDir, "x2.log").getAbsoluteFile();
    new FileWriter(logFile2).close();

    DirectorySpooler.Builder builder = initializeAndGetBuilder()
        .setUseLastModifiedTimestamp(true)
        .setPostProcessing(DirectorySpooler.FilePostProcessing.DELETE)
        .setMaxSpoolFiles(Integer.MAX_VALUE);

    DirectorySpooler spooler = builder.build();

    spooler.init("");
    Assert.assertEquals(logFile1.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    Assert.assertEquals(logFile2.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    spooler.destroy();
  }

  @Test
  public void testReachedMaximumFiles() throws Exception {
    assertTrue(spoolDir.mkdirs());

    final int spoolingTime = 1; // 1 sec

    File logFile1 = new File(spoolDir, "x1.log").getAbsoluteFile();
    new FileWriter(logFile1).close();

    File logFile2 = new File(spoolDir, "x2.log").getAbsoluteFile();
    new FileWriter(logFile2).close();

    DirectorySpooler.Builder builder = initializeAndGetBuilder()
        .setUseLastModifiedTimestamp(true)
        .setPostProcessing(DirectorySpooler.FilePostProcessing.DELETE)
        .setMaxSpoolFiles(1)
        .setSpoolingPeriodSec(spoolingTime);

    DirectorySpooler spooler = builder.build();

    spooler.init("");

    // add 1 more
    File logFile3 = new File(spoolDir, "x3.log").getAbsoluteFile();
    new FileWriter(logFile3).close();

    spooler.finder.run();

    // later added file is being ignored because queue reached the maximum, 1
    Assert.assertEquals(logFile1.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    Assert.assertEquals(logFile2.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    Assert.assertEquals(null, spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS));

    spooler.finder.run();

    // put rest of the file since queue <= maximum
    Assert.assertEquals(logFile3.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());

    spooler.destroy();
  }
}
