/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.clipper.stage.test;

import com.clipper.stage.ConsoleTarget;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.testharness.RecordProducer;
import com.streamsets.pipeline.sdk.testharness.TargetRunner;
import org.junit.Test;

public class TestTarget {

  @Test
  public void testConsoleTarget() throws StageException {
    //Build record producer
    RecordProducer rp = new RecordProducer();
    rp.addFiled("transactionLog", RecordProducer.Type.STRING);
    rp.addFiled("transactionFare", RecordProducer.Type.STRING);
    rp.addFiled("TotalClaim", RecordProducer.Type.DOUBLE);

    new TargetRunner.Builder<ConsoleTarget>(rp)
      .addTarget(ConsoleTarget.class).build().run();

  }
}
