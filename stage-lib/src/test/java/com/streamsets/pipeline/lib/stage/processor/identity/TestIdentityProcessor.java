/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.processor.identity;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.testharness.internal.Constants;
import com.streamsets.pipeline.sdk.testharness.ProcessorRunner;
import com.streamsets.pipeline.sdk.testharness.RecordCreator;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

public class TestIdentityProcessor {

  private static final String DEFAULT_LANE = "lane";

  @Test
  public void testProcessor() throws Exception {
    Record record1 = Mockito.mock(Record.class);
    Record record2 = Mockito.mock(Record.class);
    Batch batch = Mockito.mock(Batch.class);
    Mockito.when(batch.getRecords()).thenReturn(ImmutableSet.of(record1, record2).iterator());
    BatchMaker batchMaker = Mockito.mock(BatchMaker.class);

    Processor identity = new IdentityProcessor();
    identity.process(batch, batchMaker);

    ArgumentCaptor<Record> recordCaptor = ArgumentCaptor.forClass(Record.class);
    ArgumentCaptor<String> laneCaptor = ArgumentCaptor.forClass(String.class);
    Mockito.verify(batchMaker, Mockito.times(2)).addRecord(recordCaptor.capture(), laneCaptor.capture());

    Assert.assertEquals(ImmutableList.of(record1, record2), recordCaptor.getAllValues());
   // Assert.assertEquals(ImmutableList.of(null, null), laneCaptor.getAllValues());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testIdentityProcDefaultConfig() throws StageException {
    //Build record producer
    RecordCreator rp = new RecordCreator();
//    rp.set("transactionLog", RecordProducer.Type.STRING);
//    rp.set("transactionFare", RecordProducer.Type.DOUBLE);
//    rp.set("transactionDay", RecordProducer.Type.INTEGER);

    Map<String, List<Record>> result = new ProcessorRunner.Builder<IdentityProcessor>(rp)
      .addProcessor(IdentityProcessor.class)
      .build()
      .run();

    Assert.assertNotNull(result);
    Assert.assertNotNull(result.get(DEFAULT_LANE));
    Assert.assertTrue(result.get(DEFAULT_LANE).size() == 10);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testIdentityProc() throws StageException {
    //Build record producer
    RecordCreator rp = new RecordCreator();
//    rp.set("transactionLog", RecordProducer.Type.STRING);
//    rp.set("transactionFare", RecordProducer.Type.DOUBLE);
//    rp.set("transactionDay", RecordProducer.Type.INTEGER);

    Map<String, List<Record>> result = new ProcessorRunner.Builder<IdentityProcessor>(rp)
      .addProcessor(IdentityProcessor.class)
      .maxBatchSize(35)
      .outputLanes(ImmutableSet.of("outputLane"))
      .sourceOffset(Constants.DEFAULT_SOURCE_OFFSET)
      .build()
      .run();

    Assert.assertNotNull(result);

    //No records in default lane
    Assert.assertNull(result.get(DEFAULT_LANE));
    //All records in the configured lane
    Assert.assertNotNull(result.get("outputLane"));
    Assert.assertTrue(result.get("outputLane").size() == 35);
  }


}
