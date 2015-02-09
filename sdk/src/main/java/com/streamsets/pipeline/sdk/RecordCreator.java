/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.record.RecordImpl;

import java.util.Collections;
import java.util.List;

public class RecordCreator {
  private static long counter;

  public static Record create() {
    return create("sdk", "sdk:");
  }

  public static Record create(byte[] raw, String rawMimeType) {
    return create("sdk", "sdk:" + counter++, null, raw, rawMimeType);
  }

  public static Record create(String stageCreator, String recordSourceId) {
    return create(stageCreator, recordSourceId, Collections.EMPTY_LIST, null, null);
  }

  public static Record create(String stageCreator, String recordSourceId, List<String> stagesPath) {
    return create(stageCreator, recordSourceId, stagesPath, null, null);
  }

  public static Record create(String stageCreator, String recordSourceId, byte[] raw, String rawMimeType) {
    return create(stageCreator, recordSourceId, null, raw, rawMimeType);
  }

  public static Record create(String stageCreator, String recordSourceId, List<String> stagesPath, byte[] raw,
      String rawMimeType) {
    RecordImpl record = new RecordImpl(stageCreator, recordSourceId, raw, rawMimeType);
    if (stagesPath != null) {
      StringBuilder sb = new StringBuilder();
      String separator = "";
      for (String stage : stagesPath) {
        sb.append(separator).append(stage);
        separator = ":";
      }
      record.getHeader().setStagesPath(sb.toString());
    }
    record.getHeader().setTrackingId("tid");
    return record;
  }

}
