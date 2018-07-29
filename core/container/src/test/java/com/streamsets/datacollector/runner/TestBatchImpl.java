/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.runner;

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
    List<Record> records = ImmutableList.of(Mockito.mock(Record.class), Mockito.mock(Record.class));
    Batch batch = new BatchImpl("i", "entity", "offset", records);

    Assert.assertEquals("entity", batch.getSourceEntity());
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
