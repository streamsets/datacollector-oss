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

public class RecordImpl implements Record {
  private final HeaderImpl header;
  private Field value;

  // Json deserialization

  @JsonCreator
  public RecordImpl(@JsonProperty("header") HeaderImpl header, @JsonProperty("value") Field value) {
    this.header = header;
    this.value = value;
  }

  public RecordImpl(String stageCreator, String recordSourceId, byte[] raw, String rawMime) {
    Preconditions.checkNotNull(stageCreator, "stage cannot be null");
    Preconditions.checkNotNull(recordSourceId, "source cannot be null");
    Preconditions.checkArgument((raw != null && rawMime != null) || (raw == null && rawMime == null),
                                "raw and rawMime have both to be null or not null");
    header = new HeaderImpl();
    if (raw != null) {
      header.setRaw(raw);
      header.setRawMimeType(rawMime);
    }
    header.setStageCreator(stageCreator);
    header.setSourceId(recordSourceId);
    header.setStagesPath(stageCreator);
  }

  public RecordImpl(String stageCreator, Record originatorRecord, byte[] raw, String rawMime) {
    this(stageCreator, originatorRecord.getHeader().getSourceId(), raw, rawMime);
    String trackingId = originatorRecord.getHeader().getTrackingId();
    if (trackingId != null) {
      header.setTrackingId(trackingId);
    }
  }

  // for clone() purposes

  private RecordImpl(RecordImpl record) {
    Preconditions.checkNotNull(record, "record cannot be null");
    header = record.header.clone();
    value = (record.value != null) ? record.value.clone() : null;
  }

  public void addStageToStagePath(String stage) {
    Preconditions.checkNotNull(stage, "stage cannot be null");
    header.setStagesPath(header.getStagesPath() + ":" + stage);
  }

  public void createTrackingId() {
    String currentTrackingId = header.getTrackingId();
    String newTrackingId = UUID.randomUUID().toString();
    if (currentTrackingId != null) {
      header.setPreviousStageTrackingId(currentTrackingId);
    }
    header.setTrackingId(newTrackingId);
  }

  @Override
  public Header getHeader() {
    return header;
  }

  @Override
  @JsonProperty("value")
  public Field get() {
    return value;
  }

  @Override
  public Field set(Field field) {
    Field oldData = value;
    value = field;
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
    if (value != null) {
      Field current = value;
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
        deleted = value;
        value = null;
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
  @JsonIgnore
  public Set<String> getFieldPaths() {
    Set<String> paths = new LinkedHashSet<>();
    if (value != null) {
      paths.add("");
      switch (value.getType()) {
        case MAP:
          gatherPaths("", value.getValueAsMap(), paths);
          break;
        case LIST:
          gatherPaths("", value.getValueAsList(), paths);
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
    return Utils.format("Record[headers='{}' data='{}']", header, value);
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
      eq = header.equals(other.header);
      eq = eq && ((value != null && other.value != null) || (value == null && other.value == null));
      if (eq && value != null) {
        eq = value.equals(other.value);
      }
    }
    return eq;
  }

  @Override
  public RecordImpl clone() {
    return new RecordImpl(this);
  }

}
