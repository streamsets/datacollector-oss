/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.toerror;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TestToErrorTarget {


  @Test
  public void testOutErr() throws Exception {
    Target target = new ToErrorTarget();
    TargetRunner runner = new TargetRunner.Builder(ToErrorDTarget.class, target)
        .build();
    runner.runInit();
    try {

      Record record = RecordCreator.create();
      record.set(Field.create("Hello"));
      List<Record> input = ImmutableList.of(record);
      runner.runWrite(input);
      Assert.assertEquals(1, runner.getErrorRecords().size());
      Assert.assertEquals("Hello", runner.getErrorRecords().get(0).get().getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }
}
