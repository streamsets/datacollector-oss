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
package com.streamsets.pipeline.stage.cloudstorage.lib;

import com.streamsets.pipeline.lib.event.EventCreator;
import com.streamsets.pipeline.lib.io.fileref.FileRefUtil;

public final class GCSEvents {
  public static final String BUCKET = "bucket";
  public static final String OBJECT_KEY = "objectKey";
  public static final String RECORD_COUNT = "recordCount";
  private GCSEvents(){
  }
  /**
   * Fired after a GCS Object is created
   */
  public static EventCreator GCS_OBJECT_WRITTEN = new EventCreator.Builder("GCS Object Written", 1)
      .withRequiredField(BUCKET)
      .withRequiredField(OBJECT_KEY)
      .withRequiredField(RECORD_COUNT)
      .build();

  /**
   * Fired when the file transfer is complete.
   */
  public static EventCreator FILE_TRANSFER_COMPLETE_EVENT = FileRefUtil.FILE_TRANSFER_COMPLETE_EVENT;
}
