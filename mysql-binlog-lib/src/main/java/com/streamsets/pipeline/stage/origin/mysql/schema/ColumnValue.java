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
package com.streamsets.pipeline.stage.origin.mysql.schema;

import java.io.Serializable;

public class ColumnValue {
  private final Column header;
  private final Serializable value;

  public ColumnValue(Column header, Serializable value) {
    this.header = header;
    this.value = value;
  }

  public Column getHeader() {
    return header;
  }

  public Serializable getValue() {
    return value;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("ColumnValue{");
    sb.append("header=").append(header);
    sb.append(", value=").append(value);
    sb.append('}');
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ColumnValue)) {
      return false;
    }

    ColumnValue that = (ColumnValue) o;

    if (header != null ? !header.equals(that.header) : that.header != null) {
      return false;
    }
    return value != null ? value.equals(that.value) : that.value == null;

  }

  @Override
  public int hashCode() {
    int result = header != null ? header.hashCode() : 0;
    result = 31 * result + (value != null ? value.hashCode() : 0);
    return result;
  }
}
