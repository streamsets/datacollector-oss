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
package com.streamsets.pipeline.hbase.api.common.producer;

import java.util.Arrays;

public class HBaseColumn{
  private byte[] cf;
  private byte[] qualifier;
  private long timestamp;

  public HBaseColumn() {
    this.cf = null;
    this.qualifier = null;
    this.timestamp = -1;
  }

  public HBaseColumn(byte[] cf, byte[] qualifier) {
    this.cf = cf;
    this.qualifier = qualifier;
    this.timestamp = -1;
  }

  public byte[] getCf() {
    return cf;
  }

  public byte[] getQualifier() {
    return qualifier;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public boolean equals(Object other) {
    boolean result = false;
    if (other instanceof HBaseColumn) {
      HBaseColumn that = (HBaseColumn) other;
      result = Arrays.equals(this.cf, that.getCf()) && Arrays.equals(this.qualifier, that.getQualifier()) && this.timestamp == that.getTimestamp();
    }
    return result;
  }

  @Override
  public int hashCode() {
    int result = 17;
    result = 31 * result + Arrays.hashCode(this.cf);
    result = 31 * result + Arrays.hashCode(this.qualifier);
    result = 31 * result + Long.valueOf(timestamp).hashCode();
    return result;
  }
}
