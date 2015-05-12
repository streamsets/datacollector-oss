/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.io;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.UUID;

public class TestLiveFile {
  private File testDir;

  @Before
  public void setUp() {
    testDir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(testDir.mkdirs());
  }

  @Test
  public void testFileExistAndGetters() throws IOException {
    Path path = Files.createFile(new File(testDir, "1.txt").toPath());
    String inode = Files.readAttributes(path, BasicFileAttributes.class).fileKey().toString();
    LiveFile lf = new LiveFile(path);
    Assert.assertEquals(path.toAbsolutePath(), lf.getPath());
    Assert.assertEquals(inode, lf.getINode());
    Assert.assertTrue(lf.toString().contains(path.toString()));
  }

  @Test(expected = IOException.class)
  public void testFileDoesNotExist() throws IOException {
    Path path = new File(testDir, "1.txt").toPath();
    new LiveFile(path);
  }

  @Test
  public void testHashCodeEquals() throws IOException {
    Path path = Files.createFile(new File(testDir, "1.txt").toPath());
    LiveFile lf = new LiveFile(path);
    LiveFile lf2 = new LiveFile(path);
    Assert.assertEquals(lf, lf2);
    Assert.assertEquals(lf2, lf);
    Assert.assertEquals(lf.hashCode(), lf2.hashCode());
  }

  @Test
  public void testSerDeser() throws IOException {
    Path path = Files.createFile(new File(testDir, "1.txt").toPath());
    LiveFile inf = new LiveFile(path);
    String ser = inf.serialize();
    LiveFile inf2 = LiveFile.deserialize(ser);
    Assert.assertEquals(inf, inf2);
    Assert.assertEquals(inf.hashCode(), inf2.hashCode());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSerialization() throws IOException {
    LiveFile.deserialize("foo");
  }

  @Test
  public void testSerDeserNotFound() throws IOException {
    Path path = Files.createFile(new File(testDir, "1.txt").toPath());
    LiveFile inf = new LiveFile(path);
    String ser = inf.serialize();
    Files.delete(path);
    Assert.assertEquals(inf, LiveFile.deserialize(ser).refresh());
  }

  @Test
  public void testSerDeserRenamed() throws IOException {
    Path path = Files.createFile(new File(testDir, "1.txt").toPath());
    LiveFile inf = new LiveFile(path);
    String ser = inf.serialize();
    Path path2 = new File(testDir, "2.txt").getAbsoluteFile().toPath();
    Files.move(path, path2);

    LiveFile inf2 = LiveFile.deserialize(ser);

    Assert.assertNotEquals(inf, inf2.refresh());
    Assert.assertEquals(path2, inf2.refresh().getPath());
  }

  @Test
  public void testRefreshSameName() throws IOException {
    Path path = Files.createFile(new File(testDir, "1.txt").toPath());
    LiveFile lf = new LiveFile(path);
    Assert.assertEquals(lf.refresh(), lf);
  }

  @Test
  public void testRefreshMoved() throws IOException {
    Path path = Files.createFile(new File(testDir, "1.txt").toPath());
    LiveFile lf = new LiveFile(path);
    Path path2 = Files.move(path, new File(testDir, "2.txt").toPath());

    Assert.assertNotEquals(lf.refresh(), lf);
    Assert.assertNotEquals(path2, lf.refresh().getPath());

  }

  @Test
  public void testRefreshDeleted() throws IOException {
    Path path = Files.createFile(new File(testDir, "1.txt").toPath());
    LiveFile lf = new LiveFile(path);
    Files.delete(path);
    Assert.assertEquals(lf, lf.refresh());
  }

}
