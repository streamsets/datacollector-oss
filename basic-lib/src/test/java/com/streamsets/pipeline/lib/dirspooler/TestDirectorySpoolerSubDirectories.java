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
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.streamsets.pipeline.lib.dirspooler.PathMatcherMode.GLOB;

@RunWith(PowerMockRunner.class)
@PrepareForTest(DirectorySpooler.class)
@PowerMockIgnore({
    "jdk.internal.reflect.*"
})
public class TestDirectorySpoolerSubDirectories {
  private File spoolDir;
  private File archiveDir;

  private PushSource.Context context;

  private WrappedFileSystem fs = new LocalFileSystem("*", GLOB);

  private long intervalMillis = 5000;

  @Before
  public void setUp() {
    File dir = new File("target", UUID.randomUUID().toString());
    spoolDir = new File(dir, "spool");
    archiveDir = new File(dir, "archive");
    context = (PushSource.Context) ContextInfoCreator.createSourceContext("s", false, OnRecordError.TO_ERROR, ImmutableList
        .of("a"));
  }

  private DirectorySpooler.Builder initializeAndGetBuilder() throws IOException {
    return new DirectorySpooler.Builder()
        .setContext(context)
        .setWrappedFileSystem(fs)
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
      Assert.assertTrue(spoolDir.mkdirs());

      File logFile = new File(spoolDir, "x2.log").getAbsoluteFile();
      new FileWriter(logFile).close();
      //Wait for 10 secs at max and then report false;
      test_passed = test_status.get(10000, TimeUnit.MILLISECONDS);

    } finally {
      spooler.destroy();
      schedService.shutdownNow();
    }
    Assert.assertTrue("Test did not pass, Spooler did not find files", test_passed);
  }


  @Test
  public void testEmptySpoolDir() throws Exception {
    Assert.assertTrue(spoolDir.mkdirs());
    Assert.assertTrue(new File((spoolDir + "/dir1")).mkdirs());
    File logFile1 = new File(spoolDir + "/dir1/", "x1");
    DirectorySpooler.Builder builder = initializeAndGetBuilder()
        .setMaxSpoolFiles(1);
    final DirectorySpooler spooler = builder.build();
    spooler.init("dir1/x1");
    Assert.assertNull(spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS));
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
    Assert.assertNull(spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS));
    spooler.destroy();
  }

  @Test
  public void testOlderMultipleNewerMatchingFileSpoolDirTimestamp() throws Exception {
    Assert.assertTrue(spoolDir.mkdirs());
    File dir1 = new File(spoolDir, "/dir1");
    File dir2 = new File(spoolDir, "/dir2");
    Assert.assertTrue(dir1.mkdirs());
    Assert.assertTrue(dir2.mkdirs());

    File logFile3 = new File(dir2, "x3.log").getAbsoluteFile();
    new FileWriter(logFile3).close();

    // for ctime delays, there's no way to set ctime (change timestamp) explicitly by rule
    Thread.sleep(1000L);

    File logFile1 = new File(dir1, "x1.log").getAbsoluteFile();
    new FileWriter(logFile1).close();

    // for ctime delays, there's no way to set ctime (change timestamp) explicitly by rule
    Thread.sleep(1000L);

    File logFile2 = new File(dir1, "x2.log").getAbsoluteFile();
    new FileWriter(logFile2).close();

    DirectorySpooler.Builder builder = initializeAndGetBuilder()
        .processSubdirectories(true)
        .setMaxSpoolFiles(3);
    DirectorySpooler spooler = builder.setUseLastModifiedTimestamp(true).build();

    spooler.init("dir1/x1.log");

    Assert.assertEquals(logFile1.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    Assert.assertEquals(logFile2.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    Assert.assertNull(spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS));

    spooler.destroy();
  }

  @Test
  public void testOlderMultipleNewerMatchingFileSpoolDirSameTimestamp() throws Exception {
    Assert.assertTrue(spoolDir.mkdirs());
    File dir1 = new File(spoolDir, "/dir1");
    File dir2 = new File(spoolDir, "/dir2");
    Assert.assertTrue(dir1.mkdirs());
    Assert.assertTrue(dir2.mkdirs());

    File logFile3 = new File(dir2, "x3.log").getAbsoluteFile();
    new FileWriter(logFile3).close();

    // for ctime delays, there's no way to set ctime (change timestamp) explicitly by rule
    Thread.sleep(1000L);

    File logFile1 = new File(dir1, "x1.log").getAbsoluteFile();
    new FileWriter(logFile1).close();

    // for ctime delays, there's no way to set ctime (change timestamp) explicitly by rule
    Thread.sleep(500L);

    // logFile2 and logFile4 have the same timestamp
    File logFile2 = new File(dir1, "x2.log").getAbsoluteFile();
    new FileWriter(logFile2).close();
    File logFile4 = new File(dir2, "x4.log").getAbsoluteFile();
    new FileWriter(logFile4).close();

    DirectorySpooler.Builder builder = initializeAndGetBuilder()
        .setMaxSpoolFiles(4)
        .processSubdirectories(true);
    DirectorySpooler spooler = builder.setUseLastModifiedTimestamp(true).build();

    spooler.init("dir1/x1.log");
    Assert.assertEquals(logFile1.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    Assert.assertEquals(logFile2.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    Assert.assertEquals(logFile4.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    Assert.assertNull(spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS));
    spooler.destroy();
  }

  @Test
  public void testDelete() throws Exception {
    Assert.assertTrue(spoolDir.mkdirs());
    File dir1 = new File(spoolDir, "/dir1");
    File dir2 = new File(spoolDir, "/dir2");
    Assert.assertTrue(dir1.mkdirs());
    Assert.assertTrue(dir2.mkdirs());
    File logFile1 = new File(dir1, "x1.log").getAbsoluteFile();
    new FileWriter(logFile1).close();
    File logFile2 = new File(dir1, "x2.log").getAbsoluteFile();
    new FileWriter(logFile2).close();
    File logFile3 = new File(dir2, "x3.log").getAbsoluteFile();
    new FileWriter(logFile3).close();

    DirectorySpooler.Builder builder = initializeAndGetBuilder()
        .setMaxSpoolFiles(3)
        .processSubdirectories(true)
        .setUseLastModifiedTimestamp(true)
        .setPostProcessing(DirectorySpooler.FilePostProcessing.DELETE);
    DirectorySpooler spooler = builder.build();

    Assert.assertEquals(3, countFilesInTree(spoolDir));
    Assert.assertTrue(logFile1.exists());
    Assert.assertTrue(logFile2.exists());
    Assert.assertTrue(logFile3.exists());

    spooler.init(logFile2.getParentFile().getName() + "/" + logFile2.getName());
    Assert.assertEquals(3, countFilesInTree(spoolDir));
    Assert.assertTrue(logFile1.exists());
    Assert.assertTrue(logFile2.exists());
    Assert.assertTrue(logFile3.exists());

    Assert.assertEquals(logFile2.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    Assert.assertEquals(3, countFilesInTree(spoolDir));
    Assert.assertTrue(logFile1.exists());
    Assert.assertTrue(logFile2.exists());
    Assert.assertTrue(logFile3.exists());
    spooler.doPostProcessing(fs.getFile(logFile2.toPath().toString()));

    Assert.assertEquals(logFile3.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    Assert.assertEquals(2, countFilesInTree(spoolDir));
    Assert.assertTrue(logFile1.exists());
    Assert.assertFalse(logFile2.exists());
    Assert.assertTrue(logFile3.exists());
    spooler.doPostProcessing(fs.getFile(logFile3.toPath().toString()));

    Assert.assertNull(spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS));
    Assert.assertEquals(1, countFilesInTree(spoolDir));
    Assert.assertTrue(logFile1.exists());
    Assert.assertFalse(logFile2.exists());
    Assert.assertFalse(logFile3.exists());

    spooler.destroy();
  }

  @Test
  public void testArchiveWithoutStartingFile() throws Exception {
    Assert.assertTrue(spoolDir.mkdirs());
    Assert.assertTrue(archiveDir.mkdirs());
    File dir1 = new File(spoolDir, "/dir1");
    File dir2 = new File(spoolDir, "/dir2");
    Assert.assertTrue(dir1.mkdirs());
    Assert.assertTrue(dir2.mkdirs());

    File logFile1 = new File(dir1, "x1.log").getAbsoluteFile();
    new FileWriter(logFile1).close();

    File logFile2 = new File(dir1, "x2.log").getAbsoluteFile();
    new FileWriter(logFile2).close();

    File logFile3 = new File(dir2, "x3.log").getAbsoluteFile();
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
        .processSubdirectories(true)
        .setUseLastModifiedTimestamp(true)
        .setFilePattern("*")
        .setPostProcessing(DirectorySpooler.FilePostProcessing.ARCHIVE)
        .setArchiveDir(archiveDir.getAbsolutePath());
    DirectorySpooler spooler = builder.build();

    Assert.assertEquals(3, countFilesInTree(spoolDir));
    Assert.assertEquals(0, countFilesInTree(archiveDir));
    Assert.assertTrue(logFile1.exists());
    Assert.assertTrue(logFile2.exists());
    Assert.assertTrue(logFile3.exists());

    //No starting file
    spooler.init("");
    //Nothing should be archived.
    Assert.assertEquals(3, countFilesInTree(spoolDir));
    Assert.assertEquals(0, countFilesInTree(archiveDir));
    Assert.assertTrue(logFile1.exists());
    Assert.assertTrue(logFile2.exists());
    Assert.assertTrue(logFile3.exists());

    spooler.destroy();
  }

  @Test
  public void testArchive() throws Exception {
    Assert.assertTrue(spoolDir.mkdirs());
    Assert.assertTrue(archiveDir.mkdirs());
    File dir1 = new File(spoolDir, "/dir1");
    File dir2 = new File(spoolDir, "/dir2");
    Assert.assertTrue(dir1.mkdirs());
    Assert.assertTrue(dir2.mkdirs());
    File logFile3 = new File(dir2, "x3.log").getAbsoluteFile();
    new FileWriter(logFile3).close();
    File logFile1 = new File(dir1, "x1.log").getAbsoluteFile();
    new FileWriter(logFile1).close();
    File logFile2 = new File(dir1, "x2.log").getAbsoluteFile();
    new FileWriter(logFile2).close();

    File arch2 = new File(archiveDir.toString() + "/dir1/", "x2.log");
    File arch3 = new File(archiveDir.toString() + "/dir2/", "x3.log");

    DirectorySpooler.Builder builder = initializeAndGetBuilder()
        .setMaxSpoolFiles(3)
        .processSubdirectories(true)
        .setUseLastModifiedTimestamp(true)
        .setPostProcessing(DirectorySpooler.FilePostProcessing.ARCHIVE)
        .setArchiveDir(archiveDir.getAbsolutePath());
    DirectorySpooler spooler = builder.build();

    Assert.assertEquals(3, countFilesInTree(spoolDir));
    Assert.assertEquals(0, countFilesInTree(archiveDir));
    Assert.assertTrue(logFile1.exists());
    Assert.assertTrue(logFile2.exists());
    Assert.assertTrue(logFile3.exists());

    spooler.init("dir1/x2.log");
    Assert.assertEquals(3, countFilesInTree(spoolDir));
    Assert.assertEquals(0, countFilesInTree(archiveDir));
    Assert.assertTrue(logFile1.exists());
    Assert.assertTrue(logFile2.exists());
    Assert.assertTrue(logFile3.exists());

    Assert.assertEquals(logFile2.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    Assert.assertEquals(3, countFilesInTree(spoolDir));
    Assert.assertEquals(0, countFilesInTree(archiveDir));
    Assert.assertTrue(logFile1.exists());
    Assert.assertTrue(logFile2.exists());
    Assert.assertTrue(logFile3.exists());
    spooler.doPostProcessing(fs.getFile(logFile2.toPath().toString()));

    Assert.assertEquals(logFile3.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    Assert.assertEquals(2, countFilesInTree(spoolDir));
    Assert.assertEquals(1, countFilesInTree(archiveDir));
    Assert.assertTrue(logFile1.exists());
    Assert.assertTrue(logFile3.exists());
    Assert.assertTrue(arch2.exists());
    spooler.doPostProcessing(fs.getFile(logFile3.toPath().toString()));

    Assert.assertNull(spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS));
    Assert.assertEquals(1, countFilesInTree(spoolDir));
    Assert.assertEquals(2, countFilesInTree(archiveDir));
    Assert.assertTrue(logFile1.exists());
    Assert.assertTrue(arch2.exists());
    Assert.assertTrue(arch3.exists());

    spooler.destroy();
  }

  @Test
  public void testRetentionPurging() throws Exception {
    Assert.assertTrue(spoolDir.mkdirs());
    Assert.assertTrue(archiveDir.mkdirs());

    File dir1 = new File(spoolDir, "/dir1");
    Assert.assertTrue(dir1.mkdirs());
    File dir2 = new File(spoolDir, "/dir2");
    Assert.assertTrue(dir2.mkdirs());

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
    Assert.assertEquals(3, countFilesInTree(spoolDir));

    spooler.init("x2.log");
    Assert.assertEquals(logFile2.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    spooler.doPostProcessing(fs.getFile(logFile2.toPath().toString()));
    Assert.assertEquals(logFile3.getAbsolutePath(), spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS).getAbsolutePath());
    spooler.doPostProcessing(fs.getFile(logFile3.toPath().toString()));
    Assert.assertNull(spooler.poolForFile(intervalMillis, TimeUnit.MILLISECONDS));
    Assert.assertEquals(1, countFilesInTree(spoolDir));
    Assert.assertEquals(2, countFilesInTree(archiveDir));
    File archiveLog2 = new File(archiveDir, "x2.log");
    File archiveLog3 = new File(archiveDir, "x3.log");
    Assert.assertTrue(logFile1.exists());
    Assert.assertTrue(archiveLog2.exists());
    Assert.assertTrue(archiveLog3.exists());

    // Be paranoid and update the mtimes to ensure the first purge is < 1 second from mtime.
    Assert.assertTrue(archiveLog2.setLastModified(System.currentTimeMillis()));
    Assert.assertTrue(archiveLog3.setLastModified(System.currentTimeMillis()));
    // no purging
    spooler.purger.run();
    Assert.assertEquals(2, countFilesInTree(archiveDir));

    // purging
    Thread.sleep(1100);
    spooler.purger.run();
    Assert.assertEquals(0, countFilesInTree(archiveDir));

    spooler.destroy();
  }

  @Test
  public void testSpoolQueueMetrics() throws Exception {
    Assert.assertTrue(spoolDir.mkdirs());

    File dir1 = new File(spoolDir, "/dir1");
    File dir2 = new File(spoolDir, "/dir2");
    File dir3 = new File(spoolDir, "/dir3");
    File dir4 = new File(spoolDir, "/dir4");
    Assert.assertTrue(dir1.mkdirs());
    Assert.assertTrue(dir2.mkdirs());
    Assert.assertTrue(dir3.mkdirs());
    Assert.assertTrue(dir4.mkdirs());

    Assert.assertTrue(archiveDir.mkdirs());

    DirectorySpooler.Builder builder = initializeAndGetBuilder()
        .setMaxSpoolFiles(10)
        .processSubdirectories(true)
        .setUseLastModifiedTimestamp(true)
        .setPostProcessing(DirectorySpooler.FilePostProcessing.ARCHIVE)
        .setArchiveDir(archiveDir.getAbsolutePath())
        .setArchiveRetention(1000, TimeUnit.MILLISECONDS);

    DirectorySpooler spooler = PowerMockito.spy(builder.build());

    final List<File> files = Arrays.asList(
        new File(dir1, "x1.log").getAbsoluteFile(),
        new File(dir1, "x2.log").getAbsoluteFile(),
        new File(dir2, "x3.log").getAbsoluteFile(),
        new File(dir2, "x4.log").getAbsoluteFile(),
        new File(dir3, "x5.log").getAbsoluteFile(),
        new File(dir3, "x6.log").getAbsoluteFile(),
        new File(dir4, "x7.log").getAbsoluteFile(),
        new File(dir4, "x8.log").getAbsoluteFile()
    );

    for (File file : files) {
      new FileWriter(file).close();
    }

    //First file is x2.log
    spooler.init("dir1/x2.log");

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
    Assert.assertEquals(6L, countFilesInTree(archiveDir));

    //Purge everything.
    spooler.purger.run();
    Assert.assertEquals(0, countFilesInTree(archiveDir));
    spooler.destroy();
  }

  int rVal;
  private int countFilesInTree(final File file) {
    final ArrayList<Path> toProcess = new ArrayList<>();
    rVal = 0;
    EnumSet<FileVisitOption> opts = EnumSet.of(FileVisitOption.FOLLOW_LINKS);
    try {
      Files.walkFileTree(file.toPath(), opts, Integer.MAX_VALUE, new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult visitFile(
            Path dirPath, BasicFileAttributes attributes
        ) throws IOException {
          rVal++;
          return FileVisitResult.CONTINUE;
        }
      });
    } catch (Exception ex) {
    }
    return rVal;
  }
}
