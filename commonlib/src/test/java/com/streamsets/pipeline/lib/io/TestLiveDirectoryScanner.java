/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.io;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.Map;
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
    LiveDirectoryScanner spooler = new LiveDirectoryScanner(testDir.getAbsolutePath(), "my.log", null,
                                                            LiveDirectoryScanner.RolledFilesMode.REVERSE_COUNTER);
    Assert.assertNull(spooler.scan(null));
  }

  @Test
  public void testLiveFileOnlyInSpoolDir() throws Exception {
    Path file = new File(testDir, "my.log").toPath();
    Files.createFile(file);
    LiveDirectoryScanner spooler = new LiveDirectoryScanner(testDir.getAbsolutePath(), file.getFileName().toString(),
                                                            null, LiveDirectoryScanner.RolledFilesMode.REVERSE_COUNTER);
    LiveFile lf = spooler.scan(null);
    Assert.assertNotNull(lf);
    Assert.assertEquals(new LiveFile(file), lf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetUsingLiveFile() throws Exception {
    Path file = new File(testDir, "my.log").toPath();
    Files.createFile(file);
    LiveDirectoryScanner spooler = new LiveDirectoryScanner(testDir.getAbsolutePath(), file.getFileName().toString(),
                                                            null, LiveDirectoryScanner.RolledFilesMode.REVERSE_COUNTER);
    spooler.scan(new LiveFile(file));
  }

  @Test
  public void testWithRolledFileAndNoLiveFileInSpoolDir() throws Exception {
    Path rolledFile = new File(testDir, "my.log.1").toPath();
    Files.createFile(rolledFile);
    Path liveFile = new File(testDir, "my.log").toPath();
    LiveDirectoryScanner spooler = new LiveDirectoryScanner(testDir.getAbsolutePath(), liveFile.getFileName().toString(),
                                                            null, LiveDirectoryScanner.RolledFilesMode.REVERSE_COUNTER);
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
    LiveDirectoryScanner spooler = new LiveDirectoryScanner(testDir.getAbsolutePath(), liveFile.getFileName().toString(),
                                                            null, LiveDirectoryScanner.RolledFilesMode.REVERSE_COUNTER);
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
    LiveDirectoryScanner spooler = new LiveDirectoryScanner(testDir.getAbsolutePath(), liveFile.getFileName().toString(),
                                                            null, LiveDirectoryScanner.RolledFilesMode.REVERSE_COUNTER);
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
    LiveDirectoryScanner spooler = new LiveDirectoryScanner(testDir.getAbsolutePath(), liveFile.getFileName().toString(),
                                                            null, LiveDirectoryScanner.RolledFilesMode.ALPHABETICAL);
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
    LiveDirectoryScanner spooler = new LiveDirectoryScanner(testDir.getAbsolutePath(), liveFile.getFileName().toString(),
                                                            null, LiveDirectoryScanner.RolledFilesMode.REVERSE_COUNTER);

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
    LiveDirectoryScanner spooler = new LiveDirectoryScanner(testDir.getAbsolutePath(), liveFile.getFileName().toString(),
                                                            rolledFile2.getFileName().toString(),
                                                            LiveDirectoryScanner.RolledFilesMode.ALPHABETICAL);
    LiveFile lf = spooler.scan(null);
    Assert.assertEquals(new LiveFile(rolledFile2), lf);
    lf = spooler.scan(lf);
    Assert.assertNotNull(lf);
    Assert.assertEquals(new LiveFile(liveFile), lf);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testRolledFilesModePatterns() throws Exception {
    String name = new File("target/" + UUID.randomUUID().toString(), "my.log").getAbsolutePath();

    Map<LiveDirectoryScanner.RolledFilesMode, String> MATCH = (Map) ImmutableMap.builder()
        .put(LiveDirectoryScanner.RolledFilesMode.ALPHABETICAL, name + ".a")
        .put(LiveDirectoryScanner.RolledFilesMode.REVERSE_COUNTER, name + ".124")
        .put(LiveDirectoryScanner.RolledFilesMode.DATE_YYYY_MM, name + ".2015-12")
        .put(LiveDirectoryScanner.RolledFilesMode.DATE_YYYY_MM_DD, name + ".2015-12-01")
        .put(LiveDirectoryScanner.RolledFilesMode.DATE_YYYY_MM_DD_HH, name + ".2015-12-01-23")
        .put(LiveDirectoryScanner.RolledFilesMode.DATE_YYYY_MM_DD_HH_MM, name + ".2015-12-01-23-59")
        .put(LiveDirectoryScanner.RolledFilesMode.DATE_YYYY_WW, name + ".2015-40")
        .build();

    Map<LiveDirectoryScanner.RolledFilesMode, String> NO_MATCH = (Map) ImmutableMap.builder()
        .put(LiveDirectoryScanner.RolledFilesMode.ALPHABETICAL, name)
        .put(LiveDirectoryScanner.RolledFilesMode.REVERSE_COUNTER, name + ".124x")
        .put(LiveDirectoryScanner.RolledFilesMode.DATE_YYYY_MM, name + ".2015-13")
        .put(LiveDirectoryScanner.RolledFilesMode.DATE_YYYY_MM_DD, name + ".2015-12-01x")
        .put(LiveDirectoryScanner.RolledFilesMode.DATE_YYYY_MM_DD_HH, name + ".2015-12-x1-23")
        .put(LiveDirectoryScanner.RolledFilesMode.DATE_YYYY_MM_DD_HH_MM, name + ".2015-2-01-23-59")
        .put(LiveDirectoryScanner.RolledFilesMode.DATE_YYYY_WW, name + "2015-40")
        .build();

    for (Map.Entry<LiveDirectoryScanner.RolledFilesMode, String> entry : MATCH.entrySet()) {
      Path path = new File(entry.getValue()).toPath();
      PathMatcher fileMatcher = FileSystems.getDefault().getPathMatcher(entry.getKey().getPattern(name));
      Assert.assertTrue(fileMatcher.matches(path));
    }

    for (Map.Entry<LiveDirectoryScanner.RolledFilesMode, String> entry : NO_MATCH.entrySet()) {
      Path path = new File(entry.getValue()).toPath();
      PathMatcher fileMatcher = FileSystems.getDefault().getPathMatcher(entry.getKey().getPattern(name));
      Assert.assertFalse(fileMatcher.matches(path));
    }

  }
}
