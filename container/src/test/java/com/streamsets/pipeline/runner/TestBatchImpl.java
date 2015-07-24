/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Iterator;
import java.util.List;

public class TestBatchImpl {

  @Test
  public void testBatch() {
    SourceOffsetTracker offsetTracker = Mockito.mock(SourceOffsetTracker.class);
    Mockito.when(offsetTracker.getOffset()).thenReturn("offset");
    List<Record> records = ImmutableList.of(Mockito.mock(Record.class), Mockito.mock(Record.class));
    Batch batch = new BatchImpl("i", offsetTracker.getOffset(), records);

    Assert.assertEquals("offset", batch.getSourceOffset());
    Iterator<Record> it = batch.getRecords();
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(records.get(0), it.next());
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(records.get(1), it.next());
    Assert.assertFalse(it.hasNext());
    try {
      it.remove();
      Assert.fail();
    } catch (UnsupportedOperationException ex) {
      //expected
    }
  }

}
