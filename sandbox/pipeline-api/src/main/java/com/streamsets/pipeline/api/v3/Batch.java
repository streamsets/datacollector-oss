/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.v3;

import com.streamsets.pipeline.api.v3.record.Record;

import java.util.Iterator;
import java.util.List;

public interface Batch {

  public boolean isLastBatch();

  public List<String> getTags();

  public Iterator<Record> getRecords(String ... tags);

}
