/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.sdk;

import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.pipeline.api.Record;

import java.util.Collections;
import java.util.List;

public class RecordCreator {
  private static long counter;

  private RecordCreator() {}

  public static Record create() {
    return create("sdk", "sdk:");
  }

  public static Record create(byte[] raw, String rawMimeType) {
    return create("sdk", "sdk:" + counter++, null, raw, rawMimeType);
  }

  public static Record create(String stageCreator, String recordSourceId) {
    return create(stageCreator, recordSourceId, Collections.<String>emptyList(), null, null);
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
