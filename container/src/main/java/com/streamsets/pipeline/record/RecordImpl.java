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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.container.Utils;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

@JsonDeserialize(using = RecordImplDeserializer.class)
public class RecordImpl implements Record {
  private SimpleMap<String, Object> headerData;
  private SimpleMap<String, Field> fieldData;
  private Header header;

  private static final String RESERVED_PREFIX = "_.";
  static final String STAGE_CREATOR_INSTANCE_ATTR = RESERVED_PREFIX + "stageCreator";
  static final String RECORD_SOURCE_ID_ATTR = RESERVED_PREFIX + "recordSourceId";
  static final String STAGES_PATH_ATTR = RESERVED_PREFIX + "stagePath";
  static final String RAW_DATA_ATTR = RESERVED_PREFIX + "rawData";
  static final String RAW_MIME_TYPE_ATTR = RESERVED_PREFIX + "rawMimeType";
  static final String TRACKING_ID_ATTR = RESERVED_PREFIX + "trackingId";
  static final String PREVIOUS_STAGE_TRACKING_ID_ATTR = RESERVED_PREFIX + "previousStageTrackingId";

  static class HeaderImpl implements Header {
    private final SimpleMap<String, Object> rawHeaders;
    private final ReservedPrefixSimpleMap<Object> headers;

    public HeaderImpl(SimpleMap<String, Object> headers) {
      this.rawHeaders = headers;
      this.headers = new ReservedPrefixSimpleMap<Object>(RESERVED_PREFIX, headers);
    }

    @Override
    public String getTrackingId() {
      return (String) rawHeaders.get(TRACKING_ID_ATTR);
    }

    @Override
    public String getPreviousStageTrackingId() {
      return (String) rawHeaders.get(PREVIOUS_STAGE_TRACKING_ID_ATTR);
    }

    @Override
    public byte[] getRaw() {
      byte[] raw = (byte[]) rawHeaders.get(RAW_DATA_ATTR);
      return (raw != null) ? raw.clone() : null;
    }

    @Override
    public String getRawMimeType() {
      return (String) rawHeaders.get(RAW_MIME_TYPE_ATTR);
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
    public void deleteAttribute(String name) {
      headers.remove(name);
    }

    public String getStageCreator() {
      return (String) rawHeaders.get(STAGE_CREATOR_INSTANCE_ATTR);
    }

    public String getSourceId() {
      return (String) rawHeaders.get(RECORD_SOURCE_ID_ATTR);
    }

    public String getStagesPath() {
      return (String) rawHeaders.get(STAGES_PATH_ATTR);
    }

    public Map<String, Object> getValues() {
      return headers.getValues();
    }
    
  }

  public RecordImpl(String stageCreator, String recordSourceId, byte[] raw, String rawMime) {
    Preconditions.checkNotNull(stageCreator, "stage cannot be null");
    Preconditions.checkNotNull(recordSourceId, "source cannot be null");
    Preconditions.checkArgument((raw != null && rawMime != null) || (raw == null && rawMime == null),
                                "raw and rawMime have both to be null or not null");
    headerData = new VersionedSimpleMap<String, Object>();
    if (raw != null) {
      headerData.put(RAW_DATA_ATTR, raw.clone());
      headerData.put(RAW_MIME_TYPE_ATTR, rawMime);
    }
    headerData.put(STAGE_CREATOR_INSTANCE_ATTR, stageCreator);
    headerData.put(RECORD_SOURCE_ID_ATTR, recordSourceId);
    headerData.put(STAGES_PATH_ATTR, stageCreator);

    fieldData = new VersionedSimpleMap<String, Field>();
    header = new HeaderImpl(headerData);
  }

  public RecordImpl(String stageCreator, Record originatorRecord, byte[] raw, String rawMime) {
    this(stageCreator, originatorRecord.getHeader().getSourceId(), raw, rawMime);
    String trackingId = originatorRecord.getHeader().getTrackingId();
    if (trackingId != null) {
      headerData.put(TRACKING_ID_ATTR, trackingId);
    }
  }

  RecordImpl(SimpleMap<String, Object> headers, SimpleMap<String, Field> data) {
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
    String trackingId = record.getHeader().getTrackingId();
    if (trackingId != null) {
      headerData.put(TRACKING_ID_ATTR, trackingId);
    }
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

  public void setTrackingId() {
    String newTrackingId = UUID.randomUUID().toString();
    String currentTrackingId = (String) headerData.get(TRACKING_ID_ATTR);
    if (currentTrackingId != null) {
      headerData.put(PREVIOUS_STAGE_TRACKING_ID_ATTR, currentTrackingId);
    }
    headerData.put(TRACKING_ID_ATTR, newTrackingId);
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
  public boolean hasField(String name) {
    return fieldData.hasKey(name);
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

  public Map<String, Field> getValues() {
    return fieldData.getValues();
  }

  public String toString() {
    return Utils.format("Record[headers='{}' fields='{}']", headerData, fieldData);
  }

}
