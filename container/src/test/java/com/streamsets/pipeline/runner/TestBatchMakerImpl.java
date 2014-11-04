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
package com.streamsets.pipeline.runner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.StageConfiguration;
import com.streamsets.pipeline.record.RecordImpl;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestBatchMakerImpl {

  @SuppressWarnings("unchecked")
  private StagePipe createStagePipe(List<String> outputs) {
    StageConfiguration stageConfiguration = new StageConfiguration("i", "l", "n", "1",
                                                                   Collections.EMPTY_LIST,
                                                                   Collections.EMPTY_MAP,
                                                                   ImmutableList.of("i"),
                                                                   outputs);
    StageRuntime stageRuntime = Mockito.mock(StageRuntime.class);
    Stage.Info stageInfo = Mockito.mock(Stage.Info.class);
    Mockito.when(stageInfo.getInstanceName()).thenReturn("i");
    Mockito.when(stageInfo.getName()).thenReturn("n");
    Mockito.when(stageInfo.getVersion()).thenReturn("1");
    Mockito.when(stageRuntime.getInfo()).thenReturn(stageInfo);
    Mockito.when(stageRuntime.getConfiguration()).thenReturn(stageConfiguration);
    List<String> pipeInput = LaneResolver.getPostFixed(ImmutableList.of("a"), LaneResolver.COMBINER_OUT);
    List<String> pipeOutputs = new ArrayList<String>();
    for (String output : outputs) {
      pipeOutputs.add(LaneResolver.createLane(output, "x"));
    }
    List<String> pipeOutput = LaneResolver.getPostFixed(pipeOutputs, LaneResolver.STAGE_OUT);
    return new StagePipe(stageRuntime, pipeInput, pipeOutput);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBatchMakerSingleOutputNoSnapshot() {
    StagePipe pipe = createStagePipe(ImmutableList.of("o"));
    BatchMakerImpl batchMaker = new BatchMakerImpl(pipe, false);
    Assert.assertEquals(pipe, batchMaker.getStagePipe());
    Assert.assertEquals(1, batchMaker.getStageOutput().size());
    Assert.assertEquals(ImmutableMap.of("o", Collections.EMPTY_LIST), batchMaker.getStageOutput());
    Assert.assertNull(batchMaker.getStageOutputSnapshot());
    Assert.assertEquals(ImmutableSet.of("o"), batchMaker.getLanes());

    Record record1 = new RecordImpl("i", "source", null, null);
    record1.getHeader().setAttribute("r", "1");
    batchMaker.addRecord(record1);
    Record record2 = new RecordImpl("i", "source", null, null);
    record2.getHeader().setAttribute("r", "2");
    batchMaker.addRecord(record2, "o");
    Record record3 = new RecordImpl("i", "source", null, null);
    record3.getHeader().setAttribute("r", "3");
    try {
      batchMaker.addRecord(record3, "x");
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      //expected
    }
    try {
      batchMaker.addRecord(record3, "o", "o");
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      //expected
    }
    List<Record> records = batchMaker.getStageOutput().get("o");
    Assert.assertEquals(2, records.size());
    Assert.assertEquals("1", records.get(0).getHeader().getAttribute("r"));
    Assert.assertEquals("2", records.get(1).getHeader().getAttribute("r"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBatchMakerSingleOutputWithSnapshot() {
    StagePipe pipe = createStagePipe(ImmutableList.of("o"));
    BatchMakerImpl batchMaker = new BatchMakerImpl(pipe, true);
    Assert.assertNotNull(batchMaker.getStageOutputSnapshot());

    Record record1 = new RecordImpl("i", "source", null, null);
    record1.getHeader().setAttribute("r", "1");
    batchMaker.addRecord(record1);
    Record record2 = new RecordImpl("i", "source", null, null);
    record2.getHeader().setAttribute("r", "2");
    batchMaker.addRecord(record2, "o");
    List<Record> records = batchMaker.getStageOutputSnapshot().get("o");
    Assert.assertEquals(2, records.size());
    Assert.assertEquals("1", records.get(0).getHeader().getAttribute("r"));
    Assert.assertEquals("2", records.get(1).getHeader().getAttribute("r"));
  }

}
