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
package com.streamsets.pipeline.stage.destination.hdfs.writer;

import com.streamsets.pipeline.lib.event.EventCreator;

public final class HdfsEvents {

  /**
   * Fired once a new table is being created.
   */
  public static EventCreator CLOSED_FILE = new EventCreator.Builder("file-closed", 1)
    .withRequiredField("filepath") // Absolute path to the closed file
    .withRequiredField("filename") // File name of the closed file
    .withRequiredField("length") // Size of the closed file in bytes
    .build();

  private HdfsEvents() {
    // Instantiation is prohibited
  }
}
