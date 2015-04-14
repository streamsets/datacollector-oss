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
    Assert.assertTrue(lf.isLive());
    Assert.assertEquals(inode, lf.getINode());
    Assert.assertTrue(lf.toString().contains(path.toString()));

    lf = new LiveFile(path, false);
    Assert.assertEquals(path.toAbsolutePath(), lf.getPath());
    Assert.assertFalse(lf.isLive());
    Assert.assertEquals(inode, lf.getINode());
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

    inf = new LiveFile(path, false);
    ser = inf.serialize();
    inf2 = LiveFile.deserialize(ser);
    Assert.assertEquals(inf, inf2);
    Assert.assertEquals(inf.hashCode(), inf2.hashCode());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSerialization() throws IOException {
    LiveFile.deserialize("foo");
  }

  @Test(expected = NoSuchFileException.class)
  public void testSerDeserNotFound() throws IOException {
    Path path = Files.createFile(new File(testDir, "1.txt").toPath());
    LiveFile inf = new LiveFile(path);
    String ser = inf.serialize();
    Files.delete(path);
    LiveFile.deserialize(ser);
  }

  @Test
  public void testSerDeserRenamed() throws IOException {
    Path path = Files.createFile(new File(testDir, "1.txt").toPath());
    LiveFile inf = new LiveFile(path);
    String ser = inf.serialize();
    Path path2 = new File(testDir, "2.txt").getAbsoluteFile().toPath();
    Files.move(path, path2);

    LiveFile inf2 = LiveFile.deserialize(ser);

    Assert.assertNotEquals(inf, inf2);
    Assert.assertEquals(path2, inf2.getPath());
    Assert.assertFalse(inf2.isLive());
  }

  @Test
  public void testRefreshSameName() throws IOException {
    Path path = Files.createFile(new File(testDir, "1.txt").toPath());
    LiveFile lf = new LiveFile(path);
    Assert.assertFalse(lf.refresh());
    Assert.assertTrue(lf.isLive());
  }

  @Test
  public void testRefreshMoved() throws IOException {
    Path path = Files.createFile(new File(testDir, "1.txt").toPath());
    LiveFile lf = new LiveFile(path);
    Path path2 = Files.move(path, new File(testDir, "2.txt").toPath());

    Assert.assertTrue(lf.refresh());
    Assert.assertFalse(lf.isLive());
    Assert.assertNotEquals(path2, lf.getPath());

  }

  @Test(expected = NoSuchFileException.class)
  public void testRefreshDeleted() throws IOException {
    Path path = Files.createFile(new File(testDir, "1.txt").toPath());
    LiveFile lf = new LiveFile(path);
    Files.delete(path);
    lf.refresh();
  }

}
