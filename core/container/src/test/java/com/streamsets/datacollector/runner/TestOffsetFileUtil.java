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
package com.streamsets.datacollector.runner;

import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.runner.production.OffsetFileUtil;
import com.streamsets.datacollector.util.PipelineDirectoryUtil;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.nio.file.Files;
import java.util.Map;

public class TestOffsetFileUtil {

  @Rule
  public TemporaryFolder tempFolder= new TemporaryFolder();

  @Test
  public void testSaveAndGetOffsets() throws Exception {
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    File offsetFolder = tempFolder.newFolder();
    Mockito.when(runtimeInfo.getDataDir()).thenReturn(offsetFolder.getPath());
    Files.createDirectories(PipelineDirectoryUtil.getPipelineDir(runtimeInfo, "foo", "1").toPath());
    OffsetFileUtil.saveIfEmpty(runtimeInfo, "foo", "1");
    OffsetFileUtil.saveOffsets(runtimeInfo, "foo", "1", ImmutableMap.of("a", "b", "c", "d"));
    Map<String, String> offsets = OffsetFileUtil.getOffsets(runtimeInfo, "foo", "1");
    Assert.assertNotNull(offsets);
    Assert.assertEquals(2, offsets.size());
    Assert.assertTrue(offsets.containsKey("a"));
    Assert.assertEquals("b", offsets.get("a"));
    Assert.assertTrue(offsets.containsKey("c"));
    Assert.assertEquals("d", offsets.get("c"));
  }

  @Test
  public void testResetOffsets() throws Exception {
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    File offsetFolder = tempFolder.newFolder();
    Mockito.when(runtimeInfo.getDataDir()).thenReturn(offsetFolder.getPath());
    Files.createDirectories(PipelineDirectoryUtil.getPipelineDir(runtimeInfo, "foo", "1").toPath());
    OffsetFileUtil.resetOffsets(runtimeInfo, "foo", "1");
    Assert.assertEquals(0, OffsetFileUtil.getOffsets(runtimeInfo, "foo", "1").size());
  }
}
