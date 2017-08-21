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
package com.streamsets.pipeline.lib.io;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

public class TestLiveDirectoryScanner {
  private File testDir;

  @Before
  public void setUp() throws IOException {
    testDir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(testDir.mkdirs());
  }

  @Test
  public void testNoFilesInSpoolDir() throws Exception {
    LiveDirectoryScanner spooler = new LiveDirectoryScanner(testDir.getAbsolutePath(), null,
                                                            LogRollModeFactory.REVERSE_COUNTER.get("my.log", ""));
    Assert.assertNull(spooler.scan(null));
  }

  @Test
  public void testLiveFileOnlyInSpoolDir() throws Exception {
    Path file = new File(testDir, "my.log").toPath();
    Files.createFile(file);
    LiveDirectoryScanner spooler = new LiveDirectoryScanner(testDir.getAbsolutePath(), null,
                                                            LogRollModeFactory.REVERSE_COUNTER.get(file.getFileName().toString(), ""));
    LiveFile lf = spooler.scan(null);
    Assert.assertNotNull(lf);
    Assert.assertEquals(new LiveFile(file), lf);
  }

  @Test
  public void testDirectoryMatchingName() throws Exception {
    Path file = new File(testDir, "my.log").toPath();
    Files.createDirectories(file);
    LiveDirectoryScanner spooler = new LiveDirectoryScanner(testDir.getAbsolutePath(), null,
                                                            LogRollModeFactory.REVERSE_COUNTER.get(file.getFileName().toString(), ""));
    Assert.assertNull(spooler.scan(null));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetUsingLiveFile() throws Exception {
    Path file = new File(testDir, "my.log").toPath();
    Files.createFile(file);
    LiveDirectoryScanner spooler = new LiveDirectoryScanner(testDir.getAbsolutePath(), null,
                                                            LogRollModeFactory.REVERSE_COUNTER.get(file.getFileName().toString(), ""));
    spooler.scan(new LiveFile(file));
  }

  @Test
  public void testWithRolledFileAndNoLiveFileInSpoolDir() throws Exception {
    Path rolledFile = new File(testDir, "my.log.1").toPath();
    Files.createFile(rolledFile);
    Path liveFile = new File(testDir, "my.log").toPath();
    LiveDirectoryScanner spooler = new LiveDirectoryScanner(testDir.getAbsolutePath(), null,
                                                            LogRollModeFactory.REVERSE_COUNTER.get(liveFile.getFileName().toString(), ""));
    LiveFile lf = spooler.scan(null);
    Assert.assertNotNull(lf);
    Assert.assertEquals(new LiveFile(rolledFile), lf);
    lf = spooler.scan(lf);
    Assert.assertNull(lf);
  }

  @Test
  public void testWithRolledFileAndLiveFileInSpoolDir() throws Exception {
    Path rolledFile = new File(testDir, "my.log.1").toPath();
    Files.createFile(rolledFile);
    Path liveFile = new File(testDir, "my.log").toPath();
    Files.createFile(liveFile);
    LiveDirectoryScanner spooler = new LiveDirectoryScanner(testDir.getAbsolutePath(), null,
                                                            LogRollModeFactory.REVERSE_COUNTER.get(liveFile.getFileName().toString(), ""));
    LiveFile lf = spooler.scan(null);
    Assert.assertNotNull(lf);
    Assert.assertEquals(new LiveFile(rolledFile), lf);
    lf = spooler.scan(lf);
    Assert.assertNotNull(lf);
    Assert.assertEquals(new LiveFile(liveFile), lf);
  }

  @Test
  public void testRolledFilesOrderReverseNumberInSpoolDir() throws Exception {
    Path rolledFile1 = new File(testDir, "my.log.12").toPath();
    Path rolledFile2 = new File(testDir, "my.log.2").toPath();
    Files.createFile(rolledFile1);
    Files.createFile(rolledFile2);
    Path liveFile = new File(testDir, "my.log").toPath();
    Files.createFile(liveFile);
    LiveDirectoryScanner spooler = new LiveDirectoryScanner(testDir.getAbsolutePath(), null,
                                                            LogRollModeFactory.REVERSE_COUNTER.get(liveFile.getFileName().toString(), ""));
    LiveFile lf = spooler.scan(null);
    Assert.assertNotNull(lf);
    Assert.assertEquals(new LiveFile(rolledFile1), lf);
    lf = spooler.scan(lf);
    Assert.assertNotNull(lf);
    Assert.assertEquals(new LiveFile(rolledFile2), lf);
    lf = spooler.scan(lf);
    Assert.assertNotNull(lf);
    Assert.assertEquals(new LiveFile(liveFile), lf);
  }

  @Test
  public void testRolledFilesOrderAlphabeticalInSpoolDir() throws Exception {
    Path rolledFile1 = new File(testDir, "my.log.13").toPath();
    Path rolledFile2 = new File(testDir, "my.log.2").toPath();
    Files.createFile(rolledFile1);
    Files.createFile(rolledFile2);
    Path liveFile = new File(testDir, "my.log").toPath();
    Files.createFile(liveFile);
    LiveDirectoryScanner spooler = new LiveDirectoryScanner(testDir.getAbsolutePath(), null,
                                                            LogRollModeFactory.ALPHABETICAL.get(liveFile.getFileName().toString(), ""));
    LiveFile lf = spooler.scan(null);
    Assert.assertNotNull(lf);
    Assert.assertEquals(new LiveFile(rolledFile1), lf);
    lf = spooler.scan(lf);
    Assert.assertNotNull(lf);
    Assert.assertEquals(new LiveFile(rolledFile2), lf);
    lf = spooler.scan(lf);
    Assert.assertNotNull(lf);
    Assert.assertEquals(new LiveFile(liveFile), lf);
  }

  @Test
  public void testRefreshedRolledFiles() throws Exception {
    Path rolledFile1 = new File(testDir, "my.log.2").toPath();
    Path rolledFile2 = new File(testDir, "my.log.1").toPath();
    Files.createFile(rolledFile1);
    Files.createFile(rolledFile2);
    Path liveFile = new File(testDir, "my.log").toPath();
    Files.createFile(liveFile);
    LiveDirectoryScanner spooler = new LiveDirectoryScanner(testDir.getAbsolutePath(), null,
                                                            LogRollModeFactory.REVERSE_COUNTER.get(liveFile.getFileName().toString(), ""));

    LiveFile lf = spooler.scan(null);
    Assert.assertNotNull(lf);
    //got my.log.2
    Assert.assertEquals(new LiveFile(rolledFile1), lf);

    //shifting files 2 -> 3, 1 - >2
    Path rolledFile0 = new File(testDir, "my.log.3").toPath();
    Files.move(rolledFile1, rolledFile0);
    Files.move(rolledFile2, rolledFile1);

    // a refresh should get us to my.log.3
    lf = lf.refresh();
    Assert.assertEquals(rolledFile0.toAbsolutePath(), lf.getPath());

    // getting the file should get us the new 2
    lf = spooler.scan(lf);
    Assert.assertNotNull(lf);
    Assert.assertEquals(new LiveFile(rolledFile1), lf);
  }

  @Test
  public void testUsingFirstFile() throws Exception {
    Path rolledFile1 = new File(testDir, "my.log.13").toPath();
    Path rolledFile2 = new File(testDir, "my.log.2").toPath();
    Files.createFile(rolledFile1);
    Files.createFile(rolledFile2);
    Path liveFile = new File(testDir, "my.log").toPath();
    Files.createFile(liveFile);
    LiveDirectoryScanner spooler = new LiveDirectoryScanner(testDir.getAbsolutePath(),
                                                            rolledFile2.getFileName().toString(),
                                                            LogRollModeFactory.ALPHABETICAL.get(liveFile.getFileName().toString(), ""));
    LiveFile lf = spooler.scan(null);
    Assert.assertEquals(new LiveFile(rolledFile2), lf);
    lf = spooler.scan(lf);
    Assert.assertNotNull(lf);
    Assert.assertEquals(new LiveFile(liveFile), lf);
  }

  @Test
  public void testPendingFilesWithReverseCounter() throws Exception {
    testPendingFiles("2", "1", LogRollModeFactory.REVERSE_COUNTER);
  }

  @Test
  public void testPendingFilesWithAlphabetical() throws Exception {
    testPendingFiles("a", "b", LogRollModeFactory.ALPHABETICAL);
  }

  @Test
  public void testPendingFilesWithDATE_YYYY_MM() throws Exception {
    testPendingFiles("2017-05", "2017-06", LogRollModeFactory.DATE_YYYY_MM);
  }

  @Test
  public void testPendingFilesWithDATE_YYYY_MM_DD() throws Exception {
    testPendingFiles("2017-05-06", "2017-05-07", LogRollModeFactory.DATE_YYYY_MM_DD);

  }

  @Test
  public void testPendingFilesWithDATE_YYYY_MM_DD_HH() throws Exception {
    testPendingFiles("2017-05-06-01", "2017-05-06-02", LogRollModeFactory.DATE_YYYY_MM_DD_HH);
  }

  @Test
  public void testPendingFilesWithDATE_YYYY_MM_DD_HH_MM() throws Exception {
    testPendingFiles("2017-05-06-01-01", "2017-05-06-01-02", LogRollModeFactory.DATE_YYYY_MM_DD_HH_MM);
  }


  private void testPendingFiles(
      String rolledFileNameSuffix1,
      String rolledFileNameSuffix2,
      LogRollModeFactory factory
  ) throws Exception {
    Path rolledFile1 = new File(testDir, "my.log." + rolledFileNameSuffix1).toPath();
    Path rolledFile2 = new File(testDir, "my.log." + rolledFileNameSuffix2).toPath();
    Files.createFile(rolledFile1);
    Files.createFile(rolledFile2);
    Path liveFile = new File(testDir, "my.log").toPath();
    Files.createFile(liveFile);
    LiveDirectoryScanner spooler = new LiveDirectoryScanner(
        testDir.getAbsolutePath(),
        null,
        factory.get(liveFile.getFileName().toString(), "")
    );
    LiveFile lf = spooler.scan(null);
    Assert.assertNotNull(lf);
    //got my.log.2, Only pending file with reverse counter is my.log.1
    Assert.assertEquals(new LiveFile(rolledFile1), lf);
    Assert.assertEquals(1, spooler.getPendingFiles(lf));

    lf = spooler.scan(lf);
    //got my.log.1, all files with reverse counter are processed
    Assert.assertEquals(new LiveFile(rolledFile2), lf);
    Assert.assertEquals(0, spooler.getPendingFiles(lf));

    lf = spooler.scan(lf);
    Assert.assertEquals(new LiveFile(liveFile), lf);
    Assert.assertEquals(0, spooler.getPendingFiles(lf));
  }
}
