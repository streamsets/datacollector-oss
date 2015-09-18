/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.execution.runner.common;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.runner.production.OffsetFileUtil;
import com.streamsets.datacollector.runner.production.ProductionSourceOffsetTracker;
import com.streamsets.pipeline.api.impl.Utils;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class TestProdSourceOffsetTracker {
  private static Logger LOG = LoggerFactory.getLogger(TestProdSourceOffsetTracker.class);

  private static final String PIPELINE_NAME = "myPipeline";
  private static final String PIPELINE_REV = "2.0";

  @BeforeClass
  public static void beforeClass() throws IOException {
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR, "./target/var");
    File f = new File(System.getProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR));
    try {
      FileUtils.deleteDirectory(f);
    } catch (Exception ex) {
      LOG.info(Utils.format("Got exception while deleting directory: {}", f.getAbsolutePath()), ex);
    }
  }

  @AfterClass
  public static void afterClass() {
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR);
  }

  @Test
  public void testProductionSourceOffsetTracker() {

    RuntimeInfo info = new RuntimeInfo(RuntimeModule.SDC_PROPERTY_PREFIX, new MetricRegistry(),
      Arrays.asList(getClass().getClassLoader()));
    ProductionSourceOffsetTracker offsetTracker = new ProductionSourceOffsetTracker(PIPELINE_NAME, PIPELINE_REV, info);

    Assert.assertEquals(false, offsetTracker.isFinished());
    Assert.assertEquals(null, offsetTracker.getOffset());

    offsetTracker.setOffset("abc");
    offsetTracker.commitOffset();
    Assert.assertEquals("abc", offsetTracker.getOffset());

    Assert.assertEquals(OffsetFileUtil.getPipelineOffsetFile(info, PIPELINE_NAME, PIPELINE_REV).lastModified(),
      offsetTracker.getLastBatchTime());
  }


}
