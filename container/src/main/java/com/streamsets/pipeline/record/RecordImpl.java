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
import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;

import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

public class RecordImpl implements Record {
  private SimpleMap<String, Object> headerData;
  private SimpleMap<String, Field> fieldData;
  private Header header;

  private static final String RESERVED_PREFIX = "_.";
  private static final String CREATOR_STAGE_INSTANCE_ATTR = RESERVED_PREFIX + "creatorStage";
  private static final String RECORD_SOURCE_ID_ATTR = RESERVED_PREFIX + "recordSourceId";
  private static final String STAGES_PATH_ATTR = RESERVED_PREFIX + "stagePath";
  private static final String RAW_DATA_ATTR = RESERVED_PREFIX + "rawData";
  private static final String RAW_MIME_ATTR = RESERVED_PREFIX + "rawMIME";

  static class HeaderImpl implements Header {
    private final SimpleMap<String, Object> rawHeaders;
    private final ReservedPrefixSimpleMap<Object> headers;

    public HeaderImpl(SimpleMap<String, Object> headers) {
      this.rawHeaders = headers;
      this.headers = new ReservedPrefixSimpleMap<Object>(RESERVED_PREFIX, headers);
    }

    @Override
    public byte[] getRaw() {
      byte[] raw = (byte[]) rawHeaders.get(RAW_DATA_ATTR);
      return (raw != null) ? raw.clone() : null;
    }

    @Override
    public String getRawMime() {
      return (String) rawHeaders.get(RAW_MIME_ATTR);
    }

    @Override
    @JsonIgnore
    public Iterator<String> getAttributeNames() {
      return headers.getKeys().iterator();
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
    public void removeAttribute(String name) {
      headers.remove(name);
    }

    public String getCreatorStage() {
      return (String) rawHeaders.get(CREATOR_STAGE_INSTANCE_ATTR);
    }

    public String getRecordSourceId() {
      return (String) rawHeaders.get(RECORD_SOURCE_ID_ATTR);
    }

    public String getStagesPath() {
      return (String) rawHeaders.get(STAGES_PATH_ATTR);
    }

    public Map<String, Object> getValues() {
      return headers.getValues();
    }
    
  }

  public RecordImpl(String stage, String source, byte[] raw, String rawMime) {
    Preconditions.checkNotNull(stage, "stage cannot be null");
    Preconditions.checkNotNull(source, "source cannot be null");
    Preconditions.checkArgument((raw != null && rawMime != null) || (raw == null && rawMime == null),
                                "raw and rawMime have both to be null or not null");
    headerData = new VersionedSimpleMap<String, Object>();
    if (raw != null) {
      headerData.put(RAW_DATA_ATTR, raw.clone());
      headerData.put(RAW_MIME_ATTR, rawMime);
    }
    headerData.put(CREATOR_STAGE_INSTANCE_ATTR, stage);
    headerData.put(RECORD_SOURCE_ID_ATTR, source);
    headerData.put(STAGES_PATH_ATTR, stage);

    fieldData = new VersionedSimpleMap<String, Field>();
    header = new HeaderImpl(headerData);
  }

  private RecordImpl(SimpleMap<String, Object> headers, SimpleMap<String, Field> data) {
    headerData = headers;
    fieldData = data;
    header = new HeaderImpl(headerData);
  }

  private RecordImpl(RecordImpl record) {
    Preconditions.checkNotNull(record, "record cannot be null");
    RecordImpl snapshot = record.createSnapshot();
    headerData = new VersionedSimpleMap<String, Object>(snapshot.headerData);
    fieldData = new VersionedSimpleMap<String, Field>(snapshot.fieldData);
    header = new HeaderImpl(headerData);
  }


  //TODO comment on implementation difference between snapshot and copy

  public RecordImpl createCopy() {
    return new RecordImpl(this);
  }

  public RecordImpl createSnapshot() {
    RecordImpl snapshot = new RecordImpl(headerData, fieldData);
    headerData = new VersionedSimpleMap<String, Object>(headerData);
    fieldData = new VersionedSimpleMap<String, Field>(fieldData);
    header = new HeaderImpl(headerData);
    return snapshot;
  }

  public void setStage(String stage) {
    Preconditions.checkNotNull(stage, "stage cannot be null");
    headerData.put(STAGES_PATH_ATTR, headerData.get(STAGES_PATH_ATTR) + ":" + stage);
  }

  @Override
  public Header getHeader() {
    return header;
  }

  @Override
  @JsonIgnore
  public Iterator<String> getFieldNames() {
    return fieldData.getKeys().iterator();
  }

  @Override
  public Field getField(String name) {
    return fieldData.get(name);
  }

  @Override
  public void setField(String name, Field field) {
    fieldData.put(name, field);
  }

  @Override
  public void deleteField(String name) {
    fieldData.remove(name);
  }

  @Override
  public String toString(Locale locale) {
    return null;
  }

  public Map<String, Field> getValues() {
    return fieldData.getValues();
  }

  public String toString() {
    return String.format("Record headers %s, fields %s", headerData.toString(), fieldData.toString());
  }

}
