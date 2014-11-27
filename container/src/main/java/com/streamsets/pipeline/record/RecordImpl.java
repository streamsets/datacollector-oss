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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.container.Utils;
import com.streamsets.pipeline.util.TextUtils;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.UUID;

@JsonDeserialize(using = RecordImplDeserializer.class)
public class RecordImpl implements Record {
  private SimpleMap<String, Object> headerData;
  private Header header;
  private Field data;

  static final String RESERVED_PREFIX = "_.";
  static final String STAGE_CREATOR_INSTANCE_ATTR = RESERVED_PREFIX + "stageCreator";
  static final String RECORD_SOURCE_ID_ATTR = RESERVED_PREFIX + "recordSourceId";
  static final String STAGES_PATH_ATTR = RESERVED_PREFIX + "stagePath";
  static final String RAW_DATA_ATTR = RESERVED_PREFIX + "rawData";
  static final String RAW_MIME_TYPE_ATTR = RESERVED_PREFIX + "rawMimeType";
  static final String TRACKING_ID_ATTR = RESERVED_PREFIX + "trackingId";
  static final String PREVIOUS_STAGE_TRACKING_ID_ATTR = RESERVED_PREFIX + "previousStageTrackingId";

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
    header = new HeaderImpl(headerData);
  }

  public RecordImpl(String stageCreator, Record originatorRecord, byte[] raw, String rawMime) {
    this(stageCreator, originatorRecord.getHeader().getSourceId(), raw, rawMime);
    String trackingId = originatorRecord.getHeader().getTrackingId();
    if (trackingId != null) {
      headerData.put(TRACKING_ID_ATTR, trackingId);
    }
  }

  RecordImpl(SimpleMap<String, Object> headerData, Field data) {
    this.headerData = headerData;
    this.data = data;
    header = new HeaderImpl(this.headerData);
  }

  private RecordImpl(RecordImpl record) {
    Preconditions.checkNotNull(record, "record cannot be null");
    RecordImpl snapshot = record.createSnapshot();
    headerData = new VersionedSimpleMap<>(snapshot.headerData);
    data = snapshot.data;
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
    RecordImpl snapshot = new RecordImpl(headerData, data);
    headerData = new VersionedSimpleMap<>(headerData);
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
  public Field get() {
    return data;
  }

  @Override
  public Field set(Field field) {
    Field oldData = data;
    data = field;
    return oldData;
  }

  private static class PathElement {

    enum Type {ROOT, MAP, LIST }

    private final Type type;
    private final String name;
    private final int idx;

    static final PathElement ROOT = new PathElement(Type.ROOT, null, 0);

    private PathElement(Type type, String name, int idx) {
      this.type = type;
      this.name = name;
      this.idx = idx;
    }

    public static PathElement createMapElement(String name) {
      return new PathElement(Type.MAP, name, 0);
    }

    public static PathElement createArrayElement(int idx) {
      return new PathElement(Type.LIST, null, idx);
    }

    public Type getType() {
      return type;
    }

    public String getName() {
      return name;
    }

    public int getIndex() {
      return idx;
    }

    @Override
    public String toString() {
      switch (type) {
        case ROOT:
          return "PathElement[type=ROOT]";
        case MAP:
          return Utils.format("PathElement[type=MAP, name='{}']", getName());
        case LIST:
          return Utils.format("PathElement[type=LIST, idx='{}']", getIndex());
        default:
          throw new IllegalStateException();
      }
    }
  }

  private static final String INVALID_FIELD_PATH = "Invalid fieldPath '{}' at char '{}'";

  @VisibleForTesting
  List<PathElement> parse(String fieldPath) {
    Preconditions.checkNotNull(fieldPath, "fieldPath cannot be null");
    List<PathElement> elements = new ArrayList<>();
    elements.add(PathElement.ROOT);
    if (!fieldPath.isEmpty()) {
      if (fieldPath.charAt(0) != '/' && fieldPath.charAt(0) != '[') {
        throw new IllegalArgumentException(Utils.format(INVALID_FIELD_PATH, fieldPath, 0));
      }
      StringTokenizer st = new StringTokenizer(fieldPath, "/[", true);
      int pathPos = 0;
      while (st.hasMoreTokens()) {
        String tokenType = st.nextToken();
        pathPos++;
        if (tokenType.equals("/") || tokenType.equals("[")) {
          if (!st.hasMoreElements()) {
            throw new IllegalArgumentException(Utils.format(INVALID_FIELD_PATH, fieldPath, pathPos));
          } else {
            String tokenValue = st.nextToken();
            pathPos += tokenValue.length();
            if (tokenType.equals("/")) { // MAP
              if (TextUtils.isValidName(tokenValue)) {
                elements.add(PathElement.createMapElement(tokenValue));
              } else {
                throw new IllegalArgumentException(Utils.format(INVALID_FIELD_PATH, fieldPath, pathPos));
              }
            } else { // LIST
              if (tokenValue.endsWith("]")) {
                tokenValue = tokenValue.substring(0, tokenValue.length() - 1);
                try {
                  int index = Integer.parseInt(tokenValue);
                  if (index >= 0) {
                    elements.add(PathElement.createArrayElement(index));
                  } else {
                    throw new IllegalArgumentException(Utils.format(INVALID_FIELD_PATH, fieldPath, pathPos));
                  }
                } catch (NumberFormatException ex) {
                  throw new IllegalArgumentException(Utils.format(INVALID_FIELD_PATH, fieldPath, pathPos));
                }
              } else {
                throw new IllegalArgumentException(Utils.format(INVALID_FIELD_PATH, fieldPath, pathPos));
              }
            }
          }
        } else {
          throw new IllegalArgumentException(Utils.format(INVALID_FIELD_PATH, fieldPath, pathPos));
        }
      }
    }
    return elements;
  }

  private List<Field> get(List<PathElement> elements) {
    List<Field> fields = new ArrayList<>(elements.size());
    if (data != null) {
      Field current = data;
      for (int i = 0; current != null &&  i < elements.size(); i++) {
        Field next = null;
        PathElement element = elements.get(i);
        switch (element.getType()) {
          case ROOT:
            fields.add(current);
            next = current;
            break;
          case MAP:
            if (current.getType() == Field.Type.MAP) {
              String name = element.getName();
              Map<String, Field> map = current.getValueAsMap();
              if (map != null) {
                Field field = map.get(name);
                if (field != null) {
                  fields.add(field);
                  next = field;
                }
              }
            }
            break;
          case LIST:
            if (current.getType() == Field.Type.LIST) {
              int index = element.getIndex();
              List<Field> list = current.getValueAsList();
              if (list != null) {
                if (list.size() > index) {
                  Field field = list.get(index);
                  fields.add(field);
                  next = field;
                }
              }
            }
            break;
        }
        current = next;
      }
    }
    return fields;
  }

  @Override
  public Field get(String fieldPath) {
    List<PathElement> elements = parse(fieldPath);
    List<Field> fields = get(elements);
    return (elements.size() == fields.size()) ? fields.get(fields.size() - 1) : null;
  }

  @Override
  public Field delete(String fieldPath) {
    List<PathElement> elements = parse(fieldPath);
    List<Field> fields = get(elements);
    Field deleted = null;
    int fieldPos = fields.size();
    if (elements.size() == fieldPos) {
      fieldPos--;
      if (fieldPos == 0) {
        deleted = data;
        data = null;
      } else {
        switch (elements.get(fieldPos).getType()) {
          case MAP:
            deleted = fields.get(fieldPos - 1).getValueAsMap().remove(elements.get(fieldPos).getName());
            break;
          case LIST:
            deleted = fields.get(fieldPos - 1).getValueAsList().remove(elements.get(fieldPos).getIndex());
            break;
        }
      }
    }
    return deleted;
  }

  @Override
  public boolean has(String fieldPath) {
    List<PathElement> elements = parse(fieldPath);
    List<Field> fields = get(elements);
    return (elements.size() == fields.size());
  }

  @Override
  public Set<String> getFieldPaths() {
    Set<String> paths = new LinkedHashSet<>();
    if (data != null) {
      paths.add("");
      switch (data.getType()) {
        case MAP:
          gatherPaths("", data.getValueAsMap(), paths);
          break;
        case LIST:
          gatherPaths("", data.getValueAsList(), paths);
          break;
      }
    }
    return paths;
  }

  private void gatherPaths(String base, Map<String, Field> map, Set<String> paths) {
    base += "/";
    if (map != null) {
      for (Map.Entry<String, Field> entry : map.entrySet()) {
        paths.add(base + entry.getKey());
        switch (entry.getValue().getType()) {
          case MAP:
            gatherPaths(base + entry.getKey(), entry.getValue().getValueAsMap(), paths);
            break;
          case LIST:
            gatherPaths(base + entry.getKey(), entry.getValue().getValueAsList(), paths);
            break;
        }
      }
    }
  }

  private void gatherPaths(String base, List<Field> list, Set<String> paths) {
    if (list != null) {
      for (int i = 0; i < list.size(); i++) {
        paths.add(base + "[" + i + "]");
        Field element = list.get(i);
        switch (element.getType()) {
          case MAP:
            gatherPaths(base + "[" + i + "]", element.getValueAsMap(), paths);
            break;
          case LIST:
            gatherPaths(base + "[" + i + "]", element.getValueAsList(), paths);
            break;
        }
      }
    }
  }

  @Override
  public String toString() {
    return Utils.format("Record[headers='{}' data='{}']", headerData, data);
  }

  @Override
  public int hashCode() {
    return getFieldPaths().hashCode();
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean equals(Object obj) {
    boolean eq = (this == obj);
    if (!eq && obj != null && obj instanceof RecordImpl) {
      RecordImpl other = (RecordImpl) obj;
      SimpleMap<String, Object> thisHeader = headerData;
      SimpleMap<String, Object> otherHeader = other.headerData;
      eq = thisHeader.equals(otherHeader) && data.equals(other.data);
    }
    return eq;
  }

}
