/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api;

import java.util.List;

public interface BatchMaker {

  public List<String> getLanes();

  public void addRecord(Record record, String... lanes);

}
