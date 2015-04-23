/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.identity;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class TestIdentityProcessor {

  @Test
  @SuppressWarnings("unchecked")
  public void testProcessor() throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(IdentityProcessor.class).addOutputLane("a").build();
    runner.runInit();
    try {
      Record record = RecordCreator.create();
      record.set(Field.create(true));
      StageRunner.Output output = runner.runProcess(Arrays.asList(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Assert.assertEquals(true, output.getRecords().get("a").get(0).get().getValueAsBoolean());
    } finally {
      runner.runDestroy();
    }
  }

}
