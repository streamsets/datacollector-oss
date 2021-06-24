/*
 * Copyright 2021 StreamSets Inc.
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
package com.streamsets.pipeline.stage.cloudstorage.executor;

import com.streamsets.pipeline.lib.event.EventCreator;

public final class Events {

  public static EventCreator FILE_COPIED = new EventCreator.Builder("file-moved", 1)
    .withRequiredField("object_key")
    .build();

  public static EventCreator FILE_CREATED = new EventCreator.Builder("file-created", 1)
    .withRequiredField("object_key")
    .build();

  public static EventCreator FILE_CHANGED = new EventCreator.Builder("file-changed", 1)
    .withRequiredField("object_key")
    .build();

  private Events() {
    // Instantiation is prohibited
  }
}
