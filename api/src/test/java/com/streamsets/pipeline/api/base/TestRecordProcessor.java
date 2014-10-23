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
package com.streamsets.pipeline.api.base;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TestRecordProcessor {

  @Test
  public void testProcessor() throws Exception {
    Record record1 = Mockito.mock(Record.class);
    Record record2 = Mockito.mock(Record.class);
    Batch batch = Mockito.mock(Batch.class);
    Mockito.when(batch.getLanes()).thenReturn(ImmutableSet.of("l1", "l2"));
    Mockito.when(batch.getRecords(Mockito.eq("l1"))).thenReturn(ImmutableSet.of(record1).iterator());
    Mockito.when(batch.getRecords(Mockito.eq("l2"))).thenReturn(ImmutableSet.of(record2).iterator());
    final BatchMaker batchMaker = Mockito.mock(BatchMaker.class);

    final Map<String, List<Record>> got = new LinkedHashMap<String, List<Record>>();

    final int[] invocations = new int[1];

    Processor processor = new RecordProcessor() {
      @Override
      protected void process(String lane, Record record, BatchMaker bm) throws StageException {
        invocations[0]++;
        Assert.assertEquals(batchMaker, bm);
        if (!got.containsKey(lane)) {
          got.put(lane, new ArrayList<Record>());
        }
        List<Record> list = got.get(lane);
        list.add(record);
      }
    };
    processor.process(batch, batchMaker);

    Assert.assertEquals(2, invocations[0]);
    Assert.assertEquals(ImmutableList.of(record1), got.get("l1"));
    Assert.assertEquals(ImmutableList.of(record2), got.get("l2"));
  }

}
