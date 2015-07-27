/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.runner;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class BatchImpl implements Batch {
  private final String instanceName;
  private final List<Record> records;
  private final String sourceOffset;

  public BatchImpl(String instanceName, String sourceOffset, List<Record> records) {
    this.instanceName = instanceName;
    this.records = Collections.unmodifiableList(records);
    this.sourceOffset = sourceOffset;
  }

  @Override
  public String getSourceOffset() {
    return sourceOffset;
  }

  @Override
  public Iterator<Record> getRecords() {
    return records.iterator();
  }

  public int getSize() {
    return records.size();
  }

  @Override
  public String toString() {
    return Utils.format("BatchImpl[instance='{}' size='{}']", instanceName, records.size());
  }

}
