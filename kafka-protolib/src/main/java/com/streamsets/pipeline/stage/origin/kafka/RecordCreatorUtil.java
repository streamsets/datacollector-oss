/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.kafka;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;

public class RecordCreatorUtil {

  private static final String DOT = ".";

  public static Record createRecord(Source.Context context, String topic, int partition, int currentRecordCount) {
    StringBuilder recordSourceId = new StringBuilder();
    recordSourceId.append(topic).append(DOT).append(partition).append(DOT);
    //do not add timestamp for preview as we want to generate the same record with same id for the same data when
    // previewing over and over again
    if(!context.isPreview()) {
      recordSourceId.append(System.currentTimeMillis()).append(DOT);
    }
    recordSourceId.append(currentRecordCount);
    return context.createRecord(recordSourceId.toString());
  }
}
