/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.clipper.stage.test;

import com.clipper.stage.FareCalculatorProcessor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.testharness.ProcessorRunner;
import com.streamsets.pipeline.sdk.testharness.RecordProducer;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class TestProcessor {

  @Test
  public void testFareCalcProc() throws StageException {
    //Build record producer
    RecordProducer rp = new RecordProducer();
    rp.addFiled("transactionLog", RecordProducer.Type.STRING);

    Map<String, List<Record>> run = new
      ProcessorRunner.Builder<FareCalculatorProcessor>(rp)
      .addProcessor(FareCalculatorProcessor.class)
      .maxBatchSize(2)
      .build().run();

    System.out.println("Fin. Number of lines read :"
      + run.get("lane").size());
    for(Record r : run.get("lane")) {
      System.out.println(r.toString());
    }
  }

  @Test
  public void testIdentityProc() throws StageException {
    //Build record producer
    RecordProducer rp = new RecordProducer();
    rp.addFiled("transactionLog", RecordProducer.Type.STRING);
    rp.addFiled("transactionFare", RecordProducer.Type.DOUBLE);
    rp.addFiled("transactionDay", RecordProducer.Type.INTEGER);

    Map<String, List<Record>> run = new
      ProcessorRunner.Builder<FareCalculatorProcessor>(rp)
      .addProcessor(FareCalculatorProcessor.class)
      .build().run();

    System.out.println("Fin. Number of lines read :"
      + run.get("lane").size());
    for(Record r : run.get("lane")) {
      System.out.println(r.toString());
    }
  }
}
