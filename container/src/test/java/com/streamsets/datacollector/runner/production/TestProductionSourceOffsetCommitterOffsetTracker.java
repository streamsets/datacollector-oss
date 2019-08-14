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
package com.streamsets.datacollector.runner.production;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.main.StandaloneRuntimeInfo;
import com.streamsets.datacollector.runner.production.OffsetFileUtil;
import com.streamsets.datacollector.runner.production.ProductionSourceOffsetCommitterOffsetTracker;
import com.streamsets.datacollector.util.PipelineDirectoryUtil;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

public class TestProductionSourceOffsetCommitterOffsetTracker {
  private static final String PIPELINE_NAME = "myPipeline";
  private static final String PIPELINE_REV = "2.0";

  @BeforeClass
  public static void beforeClass() throws IOException {
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR, "./target/var");
    File f = new File(System.getProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR));
    FileUtils.deleteDirectory(f);
  }

  @AfterClass
  public static void afterClass() {
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR);
  }

  @Test
  public void testProductionSourceOffsetCommitterOffsetTracker() throws Exception {
    RuntimeInfo info = new StandaloneRuntimeInfo(
        RuntimeInfo.SDC_PRODUCT,
        RuntimeModule.SDC_PROPERTY_PREFIX,
        new MetricRegistry(),
        Arrays.asList(getClass().getClassLoader())
    );
    Files.createDirectories(PipelineDirectoryUtil.getPipelineDir(info, PIPELINE_NAME, PIPELINE_REV).toPath());
    ProductionSourceOffsetCommitterOffsetTracker offsetTracker = new ProductionSourceOffsetCommitterOffsetTracker(
      PIPELINE_NAME, PIPELINE_REV, info, new OffsetCommitter() {
      @Override
      public void commit(String offset) throws StageException {
        //no-op
      }
    });

    Assert.assertEquals(false, offsetTracker.isFinished());
    Assert.assertEquals("", offsetTracker.getOffsets().get(Source.POLL_SOURCE_OFFSET_KEY));

    offsetTracker.commitOffset(Source.POLL_SOURCE_OFFSET_KEY, "abc");
    Assert.assertEquals("abc", offsetTracker.getOffsets().get(Source.POLL_SOURCE_OFFSET_KEY));

    Assert.assertEquals(OffsetFileUtil.getPipelineOffsetFile(info, PIPELINE_NAME, PIPELINE_REV).lastModified(),
      offsetTracker.getLastBatchTime());
  }
}
