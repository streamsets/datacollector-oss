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
package com.streamsets.datacollector.record;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.LocalizableString;
import com.streamsets.pipeline.api.impl.Utils;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class HeaderImpl implements Record.Header, Predicate<String>, Cloneable, Serializable {
  private static final String RESERVED_PREFIX = "_.";
  private static final String STAGE_CREATOR_INSTANCE_ATTR = RESERVED_PREFIX + "stageCreator";
  private static final String RECORD_SOURCE_ID_ATTR = RESERVED_PREFIX + "recordSourceId";
  private static final String STAGES_PATH_ATTR = RESERVED_PREFIX + "stagePath";
  private static final String RAW_DATA_ATTR = RESERVED_PREFIX + "rawData";
  private static final String RAW_MIME_TYPE_ATTR = RESERVED_PREFIX + "rawMimeType";
  private static final String TRACKING_ID_ATTR = RESERVED_PREFIX + "trackingId";
  private static final String PREVIOUS_TRACKING_ID_ATTR = RESERVED_PREFIX + "previousTrackingId";
  private static final String ERROR_CODE_ATTR = RESERVED_PREFIX + "errorCode";
  private static final String ERROR_MESSAGE_ATTR = RESERVED_PREFIX + "errorMessage";
  private static final String ERROR_STAGE_ATTR = RESERVED_PREFIX + "errorStage";
  private static final String ERROR_STAGE_LABEL_ATTR = RESERVED_PREFIX + "errorStageLabel";
  private static final String ERROR_TIMESTAMP_ATTR = RESERVED_PREFIX + "errorTimestamp";
  private static final String SOURCE_RECORD_ATTR = RESERVED_PREFIX + "sourceRecord";
  private static final String ERROR_DATACOLLECTOR_ID_ATTR = RESERVED_PREFIX + "dataCollectorId";
  private static final String ERROR_PIPELINE_NAME_ATTR = RESERVED_PREFIX + "pipelineName";
  private static final String ERROR_STACKTRACE = RESERVED_PREFIX + "errorStackTrace";
  private static final String ERROR_JOB_ID = RESERVED_PREFIX + "errorJobId";
  private static final String ERROR_JOB_NAME = RESERVED_PREFIX + "errorJobName";
  public static final String TRANSFORMER_RECORD = "transformerRecord";
  private static final List<String> REQUIRED_ATTRIBUTES = ImmutableList.of(STAGE_CREATOR_INSTANCE_ATTR,
      RECORD_SOURCE_ID_ATTR);

  //Note: additional fields should also define in ScriptRecord

  private Map<String, Object> map;

  public HeaderImpl() {
    map = new HashMap<>();
    map.put(SOURCE_RECORD_ATTR, null);
  }

  @VisibleForTesting
  Map<String, Object> getInternalHeaderMap() {
    return map;
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
  public String getErrorDataCollectorId() {
    return (String) map.get(ERROR_DATACOLLECTOR_ID_ATTR);
  }

  @Override
  public String getErrorPipelineName() {
    return (String) map.get(ERROR_PIPELINE_NAME_ATTR);
  }

  @Override
  public String getErrorCode() {
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
  public String getErrorStage() {
    return (String) map.get(ERROR_STAGE_ATTR);
  }

  @Override
  public String getErrorStageLabel() {
    return (String) map.get(ERROR_STAGE_LABEL_ATTR);
  }

  @Override
  public long getErrorTimestamp() {
    final Object time = map.get(ERROR_TIMESTAMP_ATTR);
    return (time != null) ? (long) time : 0;
  }

  @Override
  public String getErrorStackTrace() {
    return (String) map.get(ERROR_STACKTRACE);
  }

  @Override
  public String getErrorJobId() {
    return (String) map.get(ERROR_JOB_ID);
  }

  @Override
  public String getErrorJobName() {
    return (String) map.get(ERROR_JOB_NAME);
  }

  @Override
  public Set<String> getAttributeNames() {
    return ImmutableSet.copyOf(Sets.filter(map.keySet(), this));
  }

  private static final String REQUIRED_ATTR_EXCEPTION_MSG = "Does not have required attributes";
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

  public HeaderImpl(
    String stageCreator,
    String sourceId,
    String stagesPath,
    String trackingId,
    String previousTrackingId,
    byte[] raw,
    String rawMimeType,
    String errorDataCollectorId,
    String errorPipelineName,
    String errorStage,
    String errorStageName,
    String errorCode,
    String errorMessage,
    long errorTimestamp,
    String errorStackTrace,
    Map<String, Object> map,
    String errorJobId,
    String errorJobName
  ) {
    this.map = map;
    setStageCreator(stageCreator);
    setSourceId(sourceId);
    if (stagesPath != null) {
      setStagesPath(stagesPath);
    }
    if (trackingId != null) {
      setTrackingId(trackingId);
    }
    if (errorDataCollectorId != null && errorPipelineName != null) {
      setErrorContext(errorDataCollectorId, errorPipelineName);
    }
    if (errorCode != null) {
      setError(errorStage, errorStageName, errorCode, errorMessage, errorTimestamp, errorStackTrace);
    }
    if (previousTrackingId != null) {
      setPreviousTrackingId(previousTrackingId);
    }
    if (raw != null) {
      setRaw(raw);
      setRawMimeType(rawMimeType);
    }
    map.put(SOURCE_RECORD_ATTR, null);
    if (errorJobId != null) {
      setErrorJobId(errorJobId);
    }
    if (errorJobName != null) {
      setErrorJobName(errorJobName);
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

  public void setErrorJobId(String errorJobId) {
    Preconditions.checkNotNull(errorJobId, "errorJobId cannot be null");
    map.put(ERROR_JOB_ID, errorJobId);
  }

  public void setErrorJobName(String errorJobName) {
    Preconditions.checkNotNull(errorJobName, "errorJobName cannot be null");
    map.put(ERROR_JOB_NAME, errorJobName);
  }

  public void setError(String errorStage, String errorStageName, ErrorMessage errorMessage) {
    Preconditions.checkNotNull(errorMessage, "errorCode cannot be null");
    setError(
      errorStage,
      errorStageName,
      errorMessage.getErrorCode(),
      errorMessage.getNonLocalized(),
      System.currentTimeMillis(),
      errorMessage.getErrorStackTrace()
    );
  }

  public void copyErrorFrom(Record record) {
    Record.Header header = record.getHeader();
    setError(
      header.getErrorStage(),
      header.getErrorStageLabel(),
      header.getErrorCode(),
      header.getErrorMessage(),
      header.getErrorTimestamp(),
      header.getErrorStackTrace()
    );
    if(header.getErrorJobId() != null) {
      setErrorJobId(header.getErrorJobId());
    }
    if(header.getErrorJobName() != null) {
      setErrorJobName(header.getErrorJobName());
    }
  }

  public void setErrorContext(String datacollector, String pipelineName) {
    map.put(ERROR_DATACOLLECTOR_ID_ATTR, datacollector);
    map.put(ERROR_PIPELINE_NAME_ATTR, pipelineName);
  }

  private void setError(
    String errorStage,
    String errorStageName,
    String errorCode,
    String errorMessage,
    long errorTimestamp,
    String errorStackTrace
  ) {
    map.put(ERROR_STAGE_ATTR, errorStage);
    map.put(ERROR_STAGE_LABEL_ATTR, errorStageName);
    map.put(ERROR_CODE_ATTR, errorCode);
    map.put(ERROR_MESSAGE_ATTR, errorMessage);
    map.put(ERROR_TIMESTAMP_ATTR, errorTimestamp);
    map.put(ERROR_STACKTRACE, errorStackTrace);
  }

  public void setSourceRecord(Record record) {
    map.put(SOURCE_RECORD_ATTR, record);
  }

  public Record getSourceRecord() {
    return (Record) map.get(SOURCE_RECORD_ATTR);
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
      Map<String, Object> otherMap = ((HeaderImpl) obj).map;
      eq = map.size() == otherMap.size();
      if (eq) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
          String key = entry.getKey();
          Object value = entry.getValue();
          Object otherValue = otherMap.get(key);
          switch (key) {
            case SOURCE_RECORD_ATTR:
              break;
            case RAW_DATA_ATTR:
              eq = value == otherValue;
              if (!eq && value != null && otherValue != null) {
                byte[] arr = (byte[]) value;
                byte[] otherArr = (byte[]) otherValue;
                eq = arr.length == otherArr.length;
                for (int i = 0; eq && i < arr.length; i++) {
                  eq = arr[i] == otherArr[i];
                }
              }
              break;
            default:
              eq = (value == otherValue) || (value != null && value.equals(otherValue));
              break;
          }
          if (!eq) {
            break;
          }
        }
      }
    }
    return eq;
  }

  @Override
  public HeaderImpl clone() {
    return new HeaderImpl(this);
  }

  @Override
  public String toString() {
    return Utils.format("HeaderImpl[{}]", getSourceId());
  }

  // ImmutableMap can't have null values and our map could have, so use unmodifiable map
  public Map<String, Object> getAllAttributes() {
    return Collections.unmodifiableMap(map);
  }

  private Map<String, Object> getSystemAttributes() {
    Map<String, Object> existingSystemAttr = new HashMap<>();

    //Need to do this way due to valid null values
    List<String> existingReservedKeys = map.keySet()
        .stream()
        .filter(key -> key.startsWith(RESERVED_PREFIX))
        .collect(Collectors.toList());
    existingReservedKeys.forEach( (key) -> existingSystemAttr.put(key, map.get(key)));

    return existingSystemAttr;
  }

  private boolean hasRequiredAttributes(Set<String> keys) {
    /* Any new header map MUST have the MINIMUM required reserved header attributes AND if any
    reserved attributes exist in existing map the MUST exist in new map's keys
    */

    // Check that new map's keys have the minimum required attributes of a header impl.
    if(!keys.containsAll(REQUIRED_ATTRIBUTES)) {
      return false;
    }

    // Check that new map's keys also contain any existing reserved attributes.
    Set<String> existingReservedKeys = getSystemAttributes().keySet();
    if (!keys.containsAll(existingReservedKeys)) {
      return false;
    }

    return true;
  }

  public Map<String, Object> overrideUserAndSystemAttributes(Map<String, Object> newAttrs) {
    /* Any new header map MUST have the MINIMUM required reserved header attributes AND if any
      reserved attributes exist in existing map the MUST exist in new map's keys
    */
    if (!hasRequiredAttributes(newAttrs.keySet())) {
      throw new IllegalArgumentException(REQUIRED_ATTR_EXCEPTION_MSG);
    }

    // ImmutableMap can't have null values and our map could have, so use unmodifiable map
    Map<String, Object> old = Collections.unmodifiableMap(map);
    map = new HashMap<>(newAttrs);
    return old;
  }

  /** To be removed */
  public Map<String, Object> getUserAttributes() {
    return map.entrySet()
        .stream()
        .filter(map -> !map.getKey().startsWith(RESERVED_PREFIX))
        .collect(Collectors.toMap(map -> map.getKey(), map -> map.getValue()));
  }

  /** To be removed */
  public Map<String, Object> setUserAttributes(Map<String, Object> newAttributes) {
    // ImmutableMap can't have null values and our map could have, so use unmodifiable map
    Map<String, Object> old = Collections.unmodifiableMap(getUserAttributes());

    //Set current map to just the Reserved System Attributes
    map = getSystemAttributes();
    // Add and validate each of the new user attributes
    newAttributes.forEach((k,v) -> setAttribute(k, v.toString()));
    return old;
  }

}
