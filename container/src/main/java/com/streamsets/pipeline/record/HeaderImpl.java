/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.record;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.streamsets.pipeline.api.Record;

import java.util.Map;
import java.util.Set;

class HeaderImpl implements Record.Header {
  private final SimpleMap<String, Object> rawHeaders;
  private final ReservedPrefixSimpleMap<Object> headers;

  public HeaderImpl(SimpleMap<String, Object> headers) {
    this.rawHeaders = headers;
    this.headers = new ReservedPrefixSimpleMap<Object>(RecordImpl.RESERVED_PREFIX, headers);
  }

  @Override
  public String getTrackingId() {
    return (String) rawHeaders.get(RecordImpl.TRACKING_ID_ATTR);
  }

  @Override
  public String getPreviousStageTrackingId() {
    return (String) rawHeaders.get(RecordImpl.PREVIOUS_STAGE_TRACKING_ID_ATTR);
  }

  @Override
  public byte[] getRaw() {
    byte[] raw = (byte[]) rawHeaders.get(RecordImpl.RAW_DATA_ATTR);
    return (raw != null) ? raw.clone() : null;
  }

  @Override
  public String getRawMimeType() {
    return (String) rawHeaders.get(RecordImpl.RAW_MIME_TYPE_ATTR);
  }

  @Override
  @JsonIgnore
  public Set<String> getAttributeNames() {
    return headers.getKeys();
  }

  @Override
  public String getAttribute(String name) {
    return (String) headers.get(name);
  }

  @Override
  public void setAttribute(String name, String value) {
    headers.put(name, value);
  }

  @Override
  public void deleteAttribute(String name) {
    headers.remove(name);
  }

  public String getStageCreator() {
    return (String) rawHeaders.get(RecordImpl.STAGE_CREATOR_INSTANCE_ATTR);
  }

  public String getSourceId() {
    return (String) rawHeaders.get(RecordImpl.RECORD_SOURCE_ID_ATTR);
  }

  public String getStagesPath() {
    return (String) rawHeaders.get(RecordImpl.STAGES_PATH_ATTR);
  }

  public Map<String, Object> getValues() {
    return headers.getValues();
  }

}
