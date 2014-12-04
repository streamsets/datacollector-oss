/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.container;

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.record.RecordImpl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PipeBatch implements BatchMaker, Batch {
  private Pipe pipe;
  private PipelineBatch pipelineBatch;
  private boolean observer;
  private Map<String, List<Record>> pipeInput;
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
  public String getBatchId() {
    return pipelineBatch.getBatchId();
  }

  public void extractFromPipelineBatch() {
    if (!observer) {
      pipeInput = pipelineBatch.drainLanes(pipe.getInputLanes());
    } else {
      pipeInput = pipelineBatch.getLanes(pipe.getInputLanes());
    }
  }

  public void flushBackToPipelineBatch() {
    if (!observer) {
      pipelineBatch.populateLanes(pipeOutput);
    }
  }

  // BatchMaker

  @Override
  public void addRecord(Record record, String... lanes) {
    Preconditions.checkState(!observer, "Observer cannot add records");
    for (String lane : lanes) {
      Preconditions.checkArgument(pipeOutput.containsKey(lane));
      pipeOutput.get(lane).add(record);
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
      records.set(i, new RecordImpl( (RecordImpl) records.get(i), moduleName));
    }
  }

  @Override
  public Iterator<Record> getRecords(String... lanes) {
    List<Record> list = new ArrayList<Record>(512);
    for (String lane : lanes) {
      list.addAll(pipeInput.remove(lane));
    }
    snapshotRecords(list);
    list = Collections.unmodifiableList(list);
    return list.iterator();
  }

  public boolean isInputFullyConsumed() {
    return pipeInput.isEmpty();
  }

}
