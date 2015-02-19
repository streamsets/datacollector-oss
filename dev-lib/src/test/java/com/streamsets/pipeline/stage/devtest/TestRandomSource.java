/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.devtest;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TestRandomSource {

  @Test
  public void testRandomSource() throws Exception{
    SourceRunner runner = new SourceRunner.Builder(RandomSource.class)
      .addConfiguration("fields", "a,b")
      .addConfiguration("delay", 0)
      .addOutputLane("a")
      .build();
    runner.runInit();
    try {
      StageRunner.Output output = runner.runProduce(null, 25);
      List<Record> records = output.getRecords().get("a");
      Assert.assertTrue(records.size() <= 25);
      if (!records.isEmpty()) {
        Assert.assertNotNull(records.get(0).get("/a"));
        Assert.assertNotNull(records.get(0).get("/b"));
        Assert.assertEquals(Field.Type.INTEGER, records.get(0).get("/a").getType());
        Assert.assertEquals(Field.Type.INTEGER, records.get(0).get("/b").getType());
      }
    } finally {
      runner.runDestroy();
    }
  }

}
