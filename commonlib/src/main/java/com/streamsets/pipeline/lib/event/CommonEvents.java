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
package com.streamsets.pipeline.lib.event;

public class CommonEvents {
  public static final String NO_MORE_DATA_TAG = "no-more-data";

  /**
   * Fired by the Origin when it is out of data.
   */
  public static EventCreator NO_MORE_DATA = new EventCreator.Builder(CommonEvents.NO_MORE_DATA_TAG, 2)
    .withOptionalField("record-count")
    .withOptionalField("error-count")
    .withOptionalField("file-count")
    .build();

  private CommonEvents() {
    // instantiation not permitted.
  }

}
