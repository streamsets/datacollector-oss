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

import com.google.common.base.Optional;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;
import java.util.OptionalLong;

public class HBaseColumn{
  private Optional<byte[]> cf;
  private Optional<byte[]> qualifier;
  private OptionalLong timestamp;

  public static final long NO_TIMESTAMP = -1;

  public HBaseColumn() {
    cf = Optional.absent();
    qualifier = Optional.absent();
    timestamp = OptionalLong.empty();
  }

  public void setCf(byte[] cf) {
    this.cf = Optional.of(cf);
  }

  public Optional<byte[]> getCf() {
    return cf;
  }

  public void setQualifier(byte[] qualifier) {
    this.qualifier = Optional.of(qualifier);
  }

  public Optional<byte[]> getQualifier() {
    return qualifier;
  }

  public OptionalLong getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = OptionalLong.of(timestamp);
  }

  public String getQualifiedName() {
    return Bytes.toString(cf.or(new byte[0])) + ":" + Bytes.toString(qualifier.or(new byte[0]));
  }

  @Override
  public boolean equals(Object other) {
    boolean result = false;
    if (other instanceof HBaseColumn) {
      HBaseColumn that = (HBaseColumn) other;
      result = Arrays.equals(that.cf.orNull(), cf.orNull()) &&
          Arrays.equals(that.qualifier.orNull(), qualifier.orNull()) &&
          that.getTimestamp().orElse(NO_TIMESTAMP) == timestamp.orElse(NO_TIMESTAMP);
    }
    return result;
  }

  @Override
  public int hashCode() {
    int result = 17;
    result = 31 * result + Arrays.hashCode(cf.orNull());
    result = 31 * result + Arrays.hashCode(qualifier.orNull());
    result = 31 * result + Long.valueOf(timestamp.orElse(NO_TIMESTAMP)).hashCode();
    return result;
  }
}
