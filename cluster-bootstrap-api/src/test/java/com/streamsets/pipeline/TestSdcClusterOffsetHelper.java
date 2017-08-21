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
package com.streamsets.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.DataInput;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

public class TestSdcClusterOffsetHelper {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static FileSystem fs;

  private Path checkpointPath;

  @BeforeClass
  public static void createFileSystem() throws Exception {
    try {
      fs = FileSystem.get(new URI("file:///"), new HdfsConfiguration());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Before
  public void setupCheckpointPath() throws Exception {
    this.checkpointPath = new Path("target", UUID.randomUUID().toString());
  }

  @After
  public void deleteCheckPointPath() throws Exception {
    fs.delete(checkpointPath, true);
  }

  @AfterClass
  public static void closeFileSystem() throws Exception {
    fs.close();
  }

  private Path getCheckpointFilePath() {
    return new Path(checkpointPath, "offset.json");
  }
  private Path getBackupCheckpointFilePath() {
    return new Path(checkpointPath, "backup_offset.json");
  }
  private Path getCheckpointMarkerFilePath() {
    return new Path(checkpointPath, "offset_marker");
  }

  @Test
  public void testSerializeOffset() throws Exception {
    SdcClusterOffsetHelper sdcClusterOffsetHelper = new SdcClusterOffsetHelper(checkpointPath, fs, -1);
    sdcClusterOffsetHelper.saveOffsets(ImmutableMap.of(0, 1L, 1, 2L));

    Path checkpointFilePath = getCheckpointFilePath();

    Assert.assertTrue(fs.exists(checkpointFilePath));

    ClusterSourceOffsetJson clusterSourceOffsetJson = OBJECT_MAPPER.readValue(
        (DataInput) fs.open(checkpointFilePath),
        ClusterSourceOffsetJson.class
    );
    Assert.assertEquals("1", clusterSourceOffsetJson.getVersion());
    Assert.assertEquals(OBJECT_MAPPER.writeValueAsString(ImmutableMap.of(0, 1L, 1, 2L)), clusterSourceOffsetJson.getOffset());
  }

  @Test
  public void testDeserializeOffset() throws Exception {
    ClusterSourceOffsetJson clusterSourceOffsetJson = new ClusterSourceOffsetJson(OBJECT_MAPPER.writeValueAsString(ImmutableMap.of(0, 1, 1, 2)), "1");
    Path checkpointFilePath = getCheckpointFilePath();
    try (OutputStream os = fs.create(checkpointFilePath)) {
      OBJECT_MAPPER.writeValue(os, clusterSourceOffsetJson);
    }
    SdcClusterOffsetHelper sdcClusterOffsetHelper = new SdcClusterOffsetHelper(checkpointPath, fs, -1);
    Map<Integer, Long> readOffsets = sdcClusterOffsetHelper.readOffsets(2);
    Assert.assertTrue(readOffsets.keySet().containsAll(ImmutableSet.of(0, 1)));
    Assert.assertEquals(1L, readOffsets.get(0).longValue());
    Assert.assertEquals(2L, readOffsets.get(1).longValue());
  }

  @Test
  public void testSerializeAndDeserialize() throws Exception {
    SdcClusterOffsetHelper sdcClusterOffsetHelper = new SdcClusterOffsetHelper(checkpointPath, fs, -1);
    sdcClusterOffsetHelper.saveOffsets(ImmutableMap.of(0, 1L, 1, 2L));
    Map<Integer, Long> readOffsets = sdcClusterOffsetHelper.readOffsets(2);
    Assert.assertTrue(readOffsets.keySet().containsAll(ImmutableSet.of(0, 1)));
    Assert.assertEquals(1L, readOffsets.get(0).longValue());
    Assert.assertEquals(2L, readOffsets.get(1).longValue());
  }

  @Test
  public void testClusterOffsetMainAndBackupFile() throws Exception {
    SdcClusterOffsetHelper sdcClusterOffsetHelper = new SdcClusterOffsetHelper(checkpointPath, fs, -1);
    sdcClusterOffsetHelper.saveOffsets(ImmutableMap.of(0, 1L, 1, 2L));
    Assert.assertTrue(fs.exists(getCheckpointFilePath()));
    Assert.assertFalse(fs.exists(getBackupCheckpointFilePath()));

    Map<Integer, Long> readOffsets = sdcClusterOffsetHelper.readOffsets(2);
    Assert.assertTrue(readOffsets.keySet().containsAll(ImmutableSet.of(0, 1)));
    Assert.assertEquals(1L, readOffsets.get(0).longValue());
    Assert.assertEquals(2L, readOffsets.get(1).longValue());

    sdcClusterOffsetHelper.saveOffsets(ImmutableMap.of(0, 2L, 1, 3L));
    Assert.assertTrue(fs.exists(getCheckpointFilePath()));
    Assert.assertTrue(fs.exists(getBackupCheckpointFilePath()));

    Map<Integer, Long> readOffsetsFromMainOffsetFile = sdcClusterOffsetHelper.readOffsets(2);
    Assert.assertTrue(readOffsets.keySet().containsAll(ImmutableSet.of(0, 1)));
    Assert.assertEquals(2L, readOffsetsFromMainOffsetFile.get(0).longValue());
    Assert.assertEquals(3L, readOffsetsFromMainOffsetFile.get(1).longValue());

    //Leave the marker file and Corrupt the mainOffsetFile by having no offset information
    try(OutputStream os = fs.create(getCheckpointMarkerFilePath(), true)) {
    }
    Path checkpointFilePath = getCheckpointFilePath();
    try (OutputStream os = fs.create(checkpointFilePath)) {
      OBJECT_MAPPER.writeValue(os, new ClusterSourceOffsetJson(OBJECT_MAPPER.writeValueAsString(Collections.<Integer, Long>emptyMap()), "1"));
    }

    Map<Integer, Long> readOffsetsFromBackupOffsetFile = sdcClusterOffsetHelper.readOffsets(2);
    Assert.assertTrue(readOffsets.keySet().containsAll(ImmutableSet.of(0, 1)));
    Assert.assertEquals(1L, readOffsetsFromBackupOffsetFile.get(0).longValue());
    Assert.assertEquals(2L, readOffsetsFromBackupOffsetFile.get(1).longValue());

    //Now also check the main offset file is updated with right offset after we recovered reading the offsets from backup file
    ClusterSourceOffsetJson clusterSourceOffsetJson = OBJECT_MAPPER.readValue(
        (DataInput) fs.open(checkpointFilePath),
        ClusterSourceOffsetJson.class
    );
    Assert.assertEquals("1", clusterSourceOffsetJson.getVersion());
    Assert.assertEquals(OBJECT_MAPPER.writeValueAsString(ImmutableMap.of(0, 1L, 1, 2L)), clusterSourceOffsetJson.getOffset());
  }

  @Test
  public void testMoreKafkaPartitions() throws Exception {
    SdcClusterOffsetHelper sdcClusterOffsetHelper = new SdcClusterOffsetHelper(checkpointPath, fs, -1);
    sdcClusterOffsetHelper.saveOffsets(ImmutableMap.of(0, 1L, 1, 2L));
    Map<Integer, Long> readOffsets = sdcClusterOffsetHelper.readOffsets(3);
    Assert.assertTrue(readOffsets.keySet().containsAll(ImmutableSet.of(0, 1)));
    Assert.assertEquals(1L, readOffsets.get(0).longValue());
    Assert.assertEquals(2L, readOffsets.get(1).longValue());
    Assert.assertEquals(0L, readOffsets.get(2).longValue());
  }

  @Test(expected = IllegalStateException.class)
  public void testLessKafkaPartitions() throws Exception {
    SdcClusterOffsetHelper sdcClusterOffsetHelper = new SdcClusterOffsetHelper(checkpointPath, fs, -1);
    sdcClusterOffsetHelper.saveOffsets(ImmutableMap.of(0, 1L, 1, 2L));
    sdcClusterOffsetHelper.readOffsets(1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWrongOffset() throws Exception {
    ClusterSourceOffsetJson clusterSourceOffsetJson = new ClusterSourceOffsetJson(OBJECT_MAPPER.writeValueAsString(ImmutableMap.of("a", "a", "b", "b")), "1");
    Path checkpointFilePath = getCheckpointFilePath();
    try (OutputStream os = fs.create(checkpointFilePath)) {
      OBJECT_MAPPER.writeValue(os, clusterSourceOffsetJson);
    }
    SdcClusterOffsetHelper sdcClusterOffsetHelper = new SdcClusterOffsetHelper(checkpointPath, fs, -1);
    sdcClusterOffsetHelper.readOffsets(2);
  }
}
