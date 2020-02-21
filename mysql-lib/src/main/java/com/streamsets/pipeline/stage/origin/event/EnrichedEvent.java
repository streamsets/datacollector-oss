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
import com.streamsets.pipeline.stage.origin.mysql.schema.Table;

public class EnrichedEvent {
  private final Event event;
  private final Table table;
  private final SourceOffset offset;

  public EnrichedEvent(Event event, Table table, SourceOffset offset) {
    this.event = event;
    this.table = table;
    this.offset = offset;
  }

  public Event getEvent() {
    return event;
  }

  public Table getTable() {
    return table;
  }

  public SourceOffset getOffset() {
    return offset;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("EnrichedEvent{");
    sb.append("event=").append(event);
    sb.append(", table=").append(table);
    sb.append(", offset=").append(offset);
    sb.append('}');
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    EnrichedEvent that = (EnrichedEvent) o;

    if (event != null ? !event.equals(that.event) : that.event != null) {
      return false;
    }
    if (table != null ? !table.equals(that.table) : that.table != null) {
      return false;
    }
    return offset != null ? offset.equals(that.offset) : that.offset == null;

  }

  @Override
  public int hashCode() {
    int result = event != null ? event.hashCode() : 0;
    result = 31 * result + (table != null ? table.hashCode() : 0);
    result = 31 * result + (offset != null ? offset.hashCode() : 0);
    return result;
  }
}
