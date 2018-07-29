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
package com.streamsets.pipeline.lib.dirspooler;

import com.streamsets.pipeline.lib.event.EventCreator;

public class SpoolDirEvents {

  /**
   * Fired when a new file is being spooled.
   */
  public static EventCreator NEW_FILE = new EventCreator.Builder("new-file", 1)
      .withRequiredField("filepath") // Absolute path to the opened file
      .build();

  /**
   * Fired when a file is finished tailing.
   */
  public static EventCreator FINISHED_FILE = new EventCreator.Builder("finished-file", 2)
      .withRequiredField("filepath") // Absolute path to the done file
      .withOptionalField("record-count")
      .withOptionalField("error-count")
      .build();

  public static EventCreator NO_MORE_DATA = new EventCreator.Builder("no-more-data", 1)
      .withOptionalField("record-count")
      .withOptionalField("error-count")
      .withOptionalField("file-count")
      .build();
}
