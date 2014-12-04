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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.streamsets.pipeline.container.LocalizableString;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.container.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class HeaderImpl implements Record.Header, Predicate<String> {
  private static final String RESERVED_PREFIX = "_.";
  private static final String STAGE_CREATOR_INSTANCE_ATTR = RESERVED_PREFIX + "stageCreator";
  private  static final String RECORD_SOURCE_ID_ATTR = RESERVED_PREFIX + "recordSourceId";
  private  static final String STAGES_PATH_ATTR = RESERVED_PREFIX + "stagePath";
  private  static final String RAW_DATA_ATTR = RESERVED_PREFIX + "rawData";
  private  static final String RAW_MIME_TYPE_ATTR = RESERVED_PREFIX + "rawMimeType";
  private  static final String TRACKING_ID_ATTR = RESERVED_PREFIX + "trackingId";
  private  static final String PREVIOUS_TRACKING_ID_ATTR = RESERVED_PREFIX + "previousTrackingId";
  private  static final String ERROR_CODE_ATTR = RESERVED_PREFIX + "errorCode";
  private  static final String ERROR_MESSAGE_ATTR = RESERVED_PREFIX + "errorMessage";

  private final Map<String, Object> map;

  public HeaderImpl() {
    this.map = new HashMap<>();
  }

  // for clone() purposes
  private HeaderImpl(HeaderImpl header) {
    this.map = new HashMap<>(header.map);
  }

  // Predicate interface

  @Override
  public boolean apply(String input) {
    return !input.startsWith(RESERVED_PREFIX);
  }

  // Record.Header interface
  
  @Override
  public String getStageCreator() {
    return (String) map.get(STAGE_CREATOR_INSTANCE_ATTR);
  }

  @Override
  public String getSourceId() {
    return (String) map.get(RECORD_SOURCE_ID_ATTR);
  }

  @Override
  public String getStagesPath() {
    return (String) map.get(STAGES_PATH_ATTR);
  }

  @Override
  public String getTrackingId() {
    return (String) map.get(TRACKING_ID_ATTR);
  }

  @Override
  public String getPreviousTrackingId() {
    return (String) map.get(PREVIOUS_TRACKING_ID_ATTR);
  }

  @Override
  public byte[] getRaw() {
    byte[] raw = (byte[]) map.get(RAW_DATA_ATTR);
    return (raw != null) ? raw.clone() : null;
  }

  @Override
  public String getRawMimeType() {
    return (String) map.get(RAW_MIME_TYPE_ATTR);
  }

  @Override
  public String getErrorId() {
    return (String) map.get(ERROR_CODE_ATTR);
  }

  @Override
  public String getErrorMessage() {
    final Object error = map.get(ERROR_MESSAGE_ATTR);
    return (error == null)
           ? null
           : (error instanceof LocalizableString) ? ((LocalizableString) error).getLocalized() : (String) error;
  }

  @Override
  @JsonIgnore
  public Set<String> getAttributeNames() {
    return ImmutableSet.copyOf(Sets.filter(map.keySet(), this));
  }

  private static final String RESERVED_PREFIX_EXCEPTION_MSG = "Header attributes cannot start with '" +
                                                              RESERVED_PREFIX + "'";

  @Override
  public String getAttribute(String name) {
    Preconditions.checkNotNull(name, "name cannot be null");
    Preconditions.checkArgument(!name.startsWith(RESERVED_PREFIX), RESERVED_PREFIX_EXCEPTION_MSG);
    return (String) map.get(name);
  }

  @Override
  public void setAttribute(String name, String value) {
    Preconditions.checkNotNull(name, "name cannot be null");
    Preconditions.checkArgument(!name.startsWith(RESERVED_PREFIX), RESERVED_PREFIX_EXCEPTION_MSG);
    Preconditions.checkNotNull(value, "value cannot be null");
    map.put(name, value);
  }

  @Override
  public void deleteAttribute(String name) {
    Preconditions.checkNotNull(name, "name cannot be null");
    Preconditions.checkArgument(!name.startsWith(RESERVED_PREFIX), RESERVED_PREFIX_EXCEPTION_MSG);
    map.remove(name);
  }

  // For Json serialization
  
  @SuppressWarnings("unchecked")
  public Map<String, String> getValues() {
    return (Map) Maps.filterKeys(map, this);
  }

  // For Json deserialization

  @JsonCreator
  public HeaderImpl(  @JsonProperty("stageCreator") String stageCreator,
      @JsonProperty("sourceId") String sourceId,
      @JsonProperty("stagesPath") String stagesPath,
      @JsonProperty("trackingId") String trackingId,
      @JsonProperty("previousTrackingId") String previousTrackingId,
      @JsonProperty("raw") byte[] raw,
      @JsonProperty("rawMimeType") String rawMimeType,
      @JsonProperty("errorId") String errorId,
      @JsonProperty("errorMessage") String errorMsg,
      @JsonProperty("values") Map<String, Object> map) {
    this.map = map;
    setStageCreator(stageCreator);
    setSourceId(sourceId);
    setStagesPath(stagesPath);
    setTrackingId(trackingId);
    if (errorId != null) {
      setErrorId(errorId);
      setErrorMessage(errorMsg);
    }
    if (previousTrackingId != null) {
      setPreviousTrackingId(previousTrackingId);
    }
    if (raw != null) {
      setRaw(raw);
      setRawMimeType(rawMimeType);
    }
  }

  // HeaderImpl setter methods

  public void setStageCreator(String stateCreator) {
    Preconditions.checkNotNull(stateCreator, "stateCreator cannot be null");
    map.put(STAGE_CREATOR_INSTANCE_ATTR, stateCreator);
  }

  public void setSourceId(String sourceId) {
    Preconditions.checkNotNull(sourceId, "sourceId cannot be null");
    map.put(RECORD_SOURCE_ID_ATTR, sourceId);
  }

  public void setStagesPath(String stagePath) {
    Preconditions.checkNotNull(stagePath, "stagePath cannot be null");
    map.put(STAGES_PATH_ATTR, stagePath);
  }

  public void setTrackingId(String trackingId) {
    Preconditions.checkNotNull(trackingId, "trackingId cannot be null");
    map.put(TRACKING_ID_ATTR, trackingId);
  }

  public void setPreviousTrackingId(String previousTrackingId) {
    Preconditions.checkNotNull(previousTrackingId, "previousTrackingId cannot be null");
    map.put(PREVIOUS_TRACKING_ID_ATTR, previousTrackingId);
  }

  public void setRaw(byte[] raw) {
    Preconditions.checkNotNull(raw, "raw cannot be null");
    map.put(RAW_DATA_ATTR, raw.clone());
  }

  public void setRawMimeType(String rawMime) {
    Preconditions.checkNotNull(rawMime, "rawMime cannot be null");
    map.put(RAW_MIME_TYPE_ATTR, rawMime);
  }

  public void setErrorId(String errorId) {
    Preconditions.checkNotNull(errorId, "errorId cannot be null");
    map.put(ERROR_CODE_ATTR, errorId);
  }

  public void setErrorMessage(LocalizableString errorMsg) {
    Preconditions.checkNotNull(errorMsg, "errorMsg cannot be null");
    map.put(ERROR_MESSAGE_ATTR, errorMsg);
  }

  @JsonProperty("errorMessage")
  public void setErrorMessage(String errorMsg) {
    Preconditions.checkNotNull(errorMsg, "errorMsg cannot be null");
    map.put(ERROR_MESSAGE_ATTR, errorMsg);
  }

  // Object methods

  @Override
  public int hashCode() {
    return map.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    boolean eq = this == obj;
    if (!eq && obj != null && obj instanceof HeaderImpl) {
      eq = map.equals(((HeaderImpl) obj).map);
    }
    return eq;
  }

  @Override
  public HeaderImpl clone() {
    return new HeaderImpl(this);
  }

  @Override
  public String toString() {
    return Utils.format("HeaderImpl[{}]", map);
  }

}
