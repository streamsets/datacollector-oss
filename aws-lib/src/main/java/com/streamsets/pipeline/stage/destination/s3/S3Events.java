/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.destination.s3;

import com.streamsets.pipeline.lib.event.EventCreator;
import com.streamsets.pipeline.lib.io.fileref.FileRefUtil;

public final class S3Events {

  /**
   * Fired after a S3Object is created
   */
  public static EventCreator S3_OBJECT_WRITTEN = new EventCreator.Builder("S3 Object Written", 1)
      .withRequiredField("bucket")
      .withRequiredField("objectKey")
      .build();

  /**
   * Fired when the file transfer is complete.
   */
  public static EventCreator FILE_TRANSFER_COMPLETE_EVENT = FileRefUtil.FILE_TRANSFER_COMPLETE_EVENT;

  private S3Events() {
    // Instantiation is prohibited
  }
}
