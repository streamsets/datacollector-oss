/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
      Assert.assertTrue(run.get("lane").size() <= 25);
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
      Assert.assertTrue(run.get(Constants.DEFAULT_LANE).size() <= 10);
    } catch (StageException e) {
      e.printStackTrace();
    }
  }
}
