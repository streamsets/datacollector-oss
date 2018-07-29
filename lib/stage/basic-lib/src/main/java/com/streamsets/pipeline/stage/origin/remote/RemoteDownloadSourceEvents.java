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
package com.streamsets.pipeline.stage.origin.remote;

import com.streamsets.pipeline.lib.event.EventCreator;

public final class RemoteDownloadSourceEvents {
  private RemoteDownloadSourceEvents() {
  }

  /**
   * Fired when a new file is selected for ingest.
   */
  public static EventCreator NEW_FILE = new EventCreator.Builder("new-file", 1)
      .withRequiredField("filepath")
      .build();

  /**
   * Fired when a file has been completely read.
   */
  public static EventCreator FINISHED_FILE = new EventCreator.Builder("finished-file", 2)
      .withRequiredField("filepath")
      .withOptionalField("record-count")
      .withOptionalField("error-count")
      .build();

  /**
   * Fired when there is no more files to be read.
   */
  public static EventCreator NO_MORE_DATA = new EventCreator.Builder("no-more-data", 1)
      .withOptionalField("record-count")
      .withOptionalField("error-count")
      .withOptionalField("file-count")
      .build();
}
