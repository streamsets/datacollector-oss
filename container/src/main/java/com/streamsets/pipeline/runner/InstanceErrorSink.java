/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.record.RecordImpl;

public class InstanceErrorSink implements FilterRecordBatch.Sink {;
  private final String instanceName;
  private final ErrorSink errorSink;
  private int counter;

  public InstanceErrorSink(String instanceName, ErrorSink errorSink) {
    this.instanceName = instanceName;
    this.errorSink = errorSink;
  }

  @Override
  public void add(Record record, ErrorMessage reason) {
    RecordImpl recordImpl = (RecordImpl) record;
    recordImpl.getHeader().setError(instanceName, reason);
    errorSink.addRecord(instanceName, recordImpl);
    counter++;
  }

  public int size() {
    return counter;
  }


  @Override
  public String toString() {
    return Utils.format("InstanceErrorSink[instance='{}' counter='{}']", instanceName, counter);
  }

}
