/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.lib.stage.devtest;

import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.stage.devtest.RandomSource;
import com.streamsets.pipeline.sdk.testharness.internal.Constants;
import com.streamsets.pipeline.sdk.testharness.SourceRunner;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class TestRandomSource {

  @Test
  public void testRandomSource() {
    try {

      Map<String, List<Record>> run = new SourceRunner.Builder<RandomSource>()
        .addSource(RandomSource.class)
        .sourceOffset(null)
        .maxBatchSize(25)
        .configure("fields", "name,age,employer,city,state,country")
        .outputLanes(ImmutableSet.of("lane"))
        .build().run();

      Assert.assertNotNull(run);
      Assert.assertNotNull(run.get("lane"));
      Assert.assertTrue(run.get("lane").size() == 25);
    } catch (StageException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testRandomSourceWithDefaultConfig() {
    try {
      Map<String, List<Record>> run = new SourceRunner.Builder<RandomSource>()
        .addSource(RandomSource.class)
        .configure("fields", "name,age,employer,city,state,country")
        .build().run();
      Assert.assertNotNull(run);
      Assert.assertNotNull(run.get(Constants.DEFAULT_LANE));
      Assert.assertTrue(run.get(Constants.DEFAULT_LANE).size() == 10);
    } catch (StageException e) {
      e.printStackTrace();
    }
  }
}
