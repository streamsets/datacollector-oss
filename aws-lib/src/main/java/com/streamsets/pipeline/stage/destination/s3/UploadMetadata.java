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
package com.streamsets.pipeline.stage.destination.s3;

import com.amazonaws.services.s3.transfer.Upload;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Record;

import java.util.List;

public class UploadMetadata {

  /**
   * Underlying Upload object with what is being sent to AWS.
   */
  private final Upload upload;

  /**
   * Calculated bucket for this upload.
   */
  private final String bucket;

  /**
   * List of records associated with this upload.
   *
   * They will be sent to error in case that the upload fails.
   */
  private final List<Record> records;

  /**
   * List of events that should be propagated if the upload is successful.
   */
  private final List<EventRecord> events;

  public UploadMetadata(
    Upload upload,
    String bucket,
    List<Record> records,
    List<EventRecord> events
  ) {
    this.upload = upload;
    this.bucket = bucket;
    this.records = records;
    this.events = events;
  }

  public Upload getUpload() {
    return upload;
  }

  public String getBucket() {
    return bucket;
  }

  public List<Record> getRecords() {
    return records;
  }

  public List<EventRecord> getEvents() {
    return events;
  }
}
