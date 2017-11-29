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

import com.streamsets.datacollector.record.HeaderImpl;

import java.util.Map;

public class RecordHeaderCreator {
  private RecordHeaderCreator() {}

  public static Map<String, Object> getRecordHeaderAvailableAttributes(
      String stageCreator,
      String sourceId,
      String stagesPath,
      String trackingId,
      String previousTrackingId,
      byte[] raw,
      String rawMimeType,
      String errorDataCollectorId,
      String errorPipelineName,
      String errorStage,
      String errorStageName,
      String errorCode,
      String errorMessage,
      long errorTimestamp,
      String errorStackTrace,
      Map<String, Object> map
  ) {
      HeaderImpl header = new HeaderImpl(
          stageCreator,
          sourceId,
          stagesPath,
          trackingId,
          previousTrackingId,
          raw,
          rawMimeType,
          errorDataCollectorId,
          errorPipelineName,
          errorStage,
          errorStageName,
          errorCode,
          errorMessage,
          errorTimestamp,
          errorStackTrace,
          map
      );

      return header.getAllAttributes();
  }
}
