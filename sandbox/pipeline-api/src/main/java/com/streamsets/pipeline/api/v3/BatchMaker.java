/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.v3;

import com.streamsets.pipeline.api.v3.record.Record;

public interface BatchMaker {

  public void addRecord(Record record, String ... tags);

}
