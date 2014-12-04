/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.v1;

public interface BatchMaker {

  public void addRecord(Record record);

  public void addRecord(String group, Record record);

}
