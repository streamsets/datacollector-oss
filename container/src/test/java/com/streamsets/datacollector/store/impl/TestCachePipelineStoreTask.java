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
package com.streamsets.datacollector.store.impl;

import com.streamsets.datacollector.util.LockCache;
import dagger.ObjectGraph;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;

public class TestCachePipelineStoreTask extends TestFilePipelineStoreTask {

  @Override
  @Before
  public void setUp() throws IOException {
    samplePipelinesDir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(samplePipelinesDir.mkdirs());
    String samplePipeline = new File(samplePipelinesDir, "helloWorldPipeline.json").getAbsolutePath();
    OutputStream os = new FileOutputStream(samplePipeline);
    IOUtils.copy(getClass().getClassLoader().getResourceAsStream("helloWorldPipeline.json"), os);
    dagger = ObjectGraph.create(new Module());
    store = new CachePipelineStoreTask(dagger.get(FilePipelineStoreTask.class), new LockCache<>());
  }
}
