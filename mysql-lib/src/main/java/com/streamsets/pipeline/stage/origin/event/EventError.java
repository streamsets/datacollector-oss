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
package com.streamsets.pipeline.stage.origin.event;

import com.github.shyiko.mysql.binlog.event.Event;
import com.streamsets.pipeline.stage.origin.mysql.offset.SourceOffset;

public class EventError {
  private final Event event;
  private final SourceOffset offset;
  private final Exception exception;

  public EventError(Event event, SourceOffset offset, Exception exception) {
    this.event = event;
    this.offset = offset;
    this.exception = exception;
  }

  public Event getEvent() {
    return event;
  }

  public Exception getException() {
    return exception;
  }

  public SourceOffset getOffset() {
    return offset;
  }
}
