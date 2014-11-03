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
package com.streamsets.pipeline.container;

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.record.RecordImpl;

import java.util.*;

public class PipeBatch implements BatchMaker, Batch {
  private Pipe pipe;
  private PipelineBatch pipelineBatch;
  private boolean observer;
  private List<Record> pipeInput;
  private Map<String, List<Record>> pipeOutput;

  public PipeBatch(Pipe pipe, PipelineBatch pipelineBatch) {
    this.pipe = pipe;
    observer = pipe instanceof ObserverPipe;
    this.pipelineBatch = pipelineBatch;
    if (!observer) {
      pipeOutput = createPipeOutput(pipe.getOutputLanes());
    }
  }

  private Map<String, List<Record>> createPipeOutput(Set<String> outputLanes) {
    Map<String, List<Record>> map = null;
    if (!observer) {
      map = new HashMap<String, List<Record>>();
      for (String lane : outputLanes) {
        map.put(lane, new ArrayList<Record>());
      }
    }
    return map;
  }

  public boolean isPreview() {
    return pipelineBatch.isPreview();
  }

  public String getPreviousBatchId() {
    return pipelineBatch.getPreviousBatchId();
  }

  public void setBatchId(String batchId) {
    pipelineBatch.setBatchId(batchId);
  }

  @Override
  public String getSourceOffset() {
    return pipelineBatch.getBatchId();
  }

  public void extractFromPipelineBatch() {
    if (!observer) {
      pipeInput = pipelineBatch.drain();
    } else {
      pipeInput = pipelineBatch.getRecords();
    }
  }

  public void flushBackToPipelineBatch() {
    //FIXME<Hari>: temp implementation. Check with Tucu
    if(pipeOutput != null && !pipeOutput.isEmpty()) {
      List<Record> records = pipeOutput.get("lane");
      if (records != null && !records.isEmpty()) {
        pipelineBatch.populate(records);
      }
    }
  }

  // BatchMaker

  @Override
  public void addRecord(Record record, String... lanes) {
    Preconditions.checkState(!observer, "Observer cannot add records");
    if (lanes.length == 0) {
      Preconditions.checkArgument(pipeOutput.size() == 1, String.format(
          "No lane has been specified and the module '%s' has multiple output lanes '%s'",
          pipe.getModuleInfo().getInstanceName(), pipe.getOutputLanes()));
      pipeOutput.get(pipeOutput.keySet().iterator().next()).add(record);
    } else {
      for (String lane : lanes) {
        Preconditions.checkArgument(pipeOutput.containsKey(lane));
        pipeOutput.get(lane).add(record);
      }
    }
  }

  // Batch

  @Override
  public Set<String> getLanes() {
    return pipe.getInputLanes();
  }

  private void snapshotRecords(List<Record> records) {
    String moduleName = pipe.getModuleInfo().getInstanceName();
    for (int i = 0; i < records.size(); i++) {
      records.set(i, new RecordImpl(moduleName, (RecordImpl) records.get(i)));
    }
  }

  @Override
  public Iterator<Record> getRecords() {
    List<Record> list = new ArrayList<Record>(pipeInput);
    snapshotRecords(list);
    list = Collections.unmodifiableList(list);
    return list.iterator();
  }

  public boolean isInputFullyConsumed() {
    return pipeInput.isEmpty();
  }

}
