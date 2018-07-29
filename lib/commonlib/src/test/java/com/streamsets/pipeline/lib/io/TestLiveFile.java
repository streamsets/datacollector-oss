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

import com.streamsets.pipeline.sdk.DataCollectorServicesUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.UUID;

public class TestLiveFile {
  private File testDir;

  @BeforeClass
  public static void setUpClass() {
    DataCollectorServicesUtils.loadDefaultServices();
  }

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

  //simulating ext4 behavior that inodes are used on file delete
  @Test
  public void testInodeReused() throws IOException {
    Path path = new File(testDir, "1.txt").toPath();
    Files.write(path, Arrays.asList("Hello"), StandardCharsets.UTF_8);
    LiveFile lf1 = new LiveFile(path);
    Files.write(path, Arrays.asList("Hola"), StandardCharsets.UTF_8);
    LiveFile lf2 = new LiveFile(path);
    Assert.assertNotEquals(lf1, lf2);
    Assert.assertEquals(lf1, lf1.refresh());
  }

  @Test(expected = NoSuchFileException.class)
  public void testPathIsDir() throws IOException {
    new LiveFile(testDir.toPath());
  }

}
