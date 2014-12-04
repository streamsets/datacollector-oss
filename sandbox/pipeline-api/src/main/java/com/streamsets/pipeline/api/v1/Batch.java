/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.v1;

import java.util.List;

public interface Batch {

  public boolean isLastBatch();

  public int size();

  public List<Record> getAllRecords();

  public List<String> getGroups();

  public List<Record> getRecords(String group);

}
