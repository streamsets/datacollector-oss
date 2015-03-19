/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.toerror;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.RecordTarget;

public class ToErrorTarget extends RecordTarget {

  @Override
  protected void write(Record record) {
    getContext().toError(record, Errors.TOERROR_00);
  }
}
