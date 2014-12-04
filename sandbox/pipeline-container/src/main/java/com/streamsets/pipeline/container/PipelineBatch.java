/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.container;

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.Record;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PipelineBatch {
  private String previousBatchId;
  private boolean preview;
  private Set<String> lanes;
  private Map<String, List<Record>> records;
  private String batchId;

  public PipelineBatch(String previousBatchId) {
    this(previousBatchId, false);
  }

  public PipelineBatch(String previousBatchId, boolean preview) {
    this.previousBatchId = previousBatchId;
    this.preview = preview;
    lanes = new HashSet<String>();
    records = new LinkedHashMap<String, List<Record>>();
  }

  public boolean isPreview() {
    return preview;
  }

  public String getPreviousBatchId() {
    return previousBatchId;
  }

  public void createLines(Set<String> lanes) {
    for (String line : lanes) {
      this.lanes.add(line);
      if (!records.containsKey(line)) {
        records.put(line, new ArrayList<Record>());
      }
    }
  }

  public void deleteLines(Set<String> lanes) {
    for (String line : lanes) {
      this.lanes.remove(line);
      records.remove(line);
    }
  }

  public Set<String> getLanes() {
    return lanes;
  }

  public boolean isEmpty() {
    return lanes.isEmpty();
  }

  public void setBatchId(String batchId) {
    this.batchId = batchId;
  }

  public String getBatchId() {
    return batchId;
  }

  public Map<String, List<Record>> getRecords() {
    return records;
  }

  public Map<String, List<Record>> drainLanes(Set<String> lanes) {
    Map<String, List<Record>> drain = new HashMap<String, List<Record>>();
    for (String lane : lanes) {
      Preconditions.checkState(records.containsKey(lane), String.format("Undefined lane '%s'", lane));
      drain.put(lane, records.remove(lane));
    }
    return drain;
  }

  public Map<String, List<Record>> getLanes(Set<String> lanes) {
    Map<String, List<Record>> map = new HashMap<String, List<Record>>();
    for (String lane : lanes) {
      Preconditions.checkState(records.containsKey(lane), String.format("Undefined lane '%s'", lane));
      map.put(lane, records.get(lane));
    }
    return map;
  }

  public void populateLanes(Map<String, List<Record>> map) {
    records.putAll(map);
  }

}
