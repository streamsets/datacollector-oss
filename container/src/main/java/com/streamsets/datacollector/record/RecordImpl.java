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

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.streamsets.datacollector.util.EscapeUtil;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RecordImpl implements Record, Cloneable {
  private final HeaderImpl header;
  private Field value;
  //Default true: so as to denote the record is just created
  //and initialized in a stage and did not pass through any other stage.
  private boolean isInitialRecord = true;

  // need default constructor for deserialization purposes (Kryo)
  private RecordImpl() {
    header = new HeaderImpl();
  }

  public RecordImpl(HeaderImpl header, Field value) {
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
  }

  public RecordImpl(String stageCreator, Record originatorRecord, byte[] raw, String rawMime) {
    this(stageCreator, originatorRecord.getHeader().getSourceId(), raw, rawMime);
    String trackingId = originatorRecord.getHeader().getTrackingId();
    if (trackingId != null) {
      header.setTrackingId(trackingId);
    }
  }

  // for clone() purposes

  protected RecordImpl(RecordImpl record) {
    Preconditions.checkNotNull(record, "record cannot be null");
    header = record.header.clone();
    value = (record.value != null) ? record.value.clone() : null;
    isInitialRecord = record.isInitialRecord();
  }

  public void addStageToStagePath(String stage) {
    Preconditions.checkNotNull(stage, "stage cannot be null");
    String currentPath = (header.getStagesPath() == null) ? "" : header.getStagesPath() + ":";
    header.setStagesPath(currentPath + stage);
  }

  public void createTrackingId() {
    String currentTrackingId = header.getTrackingId();
    String newTrackingId = getHeader().getSourceId() + "::" + getHeader().getStagesPath();
    if (currentTrackingId != null) {
      header.setPreviousTrackingId(currentTrackingId);
    }
    header.setTrackingId(newTrackingId);
  }

  public boolean isInitialRecord() {
    return isInitialRecord;
  }

  public void setInitialRecord(boolean isInitialRecord) {
    this.isInitialRecord = isInitialRecord;
  }

  @Override
  public HeaderImpl getHeader() {
    return header;
  }

  @Override
  public Field get() {
    return value;
  }

  @Override
  public Field set(Field field) {
    Field oldData = value;
    value = field;
    return oldData;
  }

  private static class FieldWithPath {
    private final String sqPath; //Single Quote escaped path
    private final String dqPath; //Double Quote escaped path
    private final Field.Type type;
    private final Object value;
    private final Map<String, String> attributes;

    public FieldWithPath(String singleQuoteEscapedPath, String doubleQuoteEscapedPath, Field.Type type, Object value) {
      this(singleQuoteEscapedPath, doubleQuoteEscapedPath, type, value, null);
    }

    public FieldWithPath(String singleQuoteEscapedPath, String doubleQuoteEscapedPath, Field.Type type, Object value,
        Map<String, String> attributes) {
      this.sqPath = singleQuoteEscapedPath;
      this.dqPath = doubleQuoteEscapedPath;
      this.type = type;
      this.value = value;
      if (attributes == null) {
        this.attributes = null;
      } else {
        this.attributes = new LinkedHashMap<>(attributes);
      }
    }

    /**
     * Returns single quote escaped path
     * @return String
     */
    public String getSQPath() {
      return sqPath;
    }

    /**
     * Returns double quote escaped path
     * @return String
     */
    public String getDQPath() {
      return dqPath;
    }

    public String getType() {
      return type.toString();
    }

    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    public Map<String, String> getAttributes() {
      if (attributes == null) {
        return null;
      } else {
        return Collections.unmodifiableMap(attributes);
      }
    }

    @SuppressWarnings("unchecked")
    public Object getValue() {
      Object v = value;
      if (value != null) {
        switch (type) {
          case INTEGER:
          case LONG:
          case FLOAT:
          case DOUBLE:
            v = value.toString();
            break;
          case LIST_MAP:
            // During serialization we convert listMap to list to preserve the order of fields in JSON object
            // When we convert listMap to list we loose the key,
            // UI & deserializer need to use path attribute to recover the key for listMap
            return new ArrayList<>(((Map) value).values());
          default:
            break;
        }
      }
      return v;
    }

    @Override
    public String toString() {
      return Utils.format("FieldWithPath[path='{}', type='{}', value='{}']", getSQPath(), getType(), getValue());
    }

  }

  @SuppressWarnings("unchecked")
  private FieldWithPath createFieldWithPath(String singleQuoteEscapedPath, String doubleQuoteEscapedPath, Field field) {
    FieldWithPath fieldWithPath = null;
    if (field != null) {
      if (field.getValue() == null) {
        fieldWithPath = new FieldWithPath(
            singleQuoteEscapedPath,
            doubleQuoteEscapedPath,
            field.getType(),
            null,
            field.getAttributes()
        );
      } else {
        switch (field.getType()) {
          case LIST:
            List<FieldWithPath> list = new ArrayList<>();
            List<Field> fList = (List<Field>) field.getValue();
            for (int i = 0; i < fList.size(); i++) {
              String ePath1 = singleQuoteEscapedPath + "[" + i + "]";
              String ePath2 = doubleQuoteEscapedPath + "[" + i + "]";
              list.add(createFieldWithPath(ePath1, ePath2, fList.get(i)));
            }
            fieldWithPath = new FieldWithPath(
                singleQuoteEscapedPath,
                doubleQuoteEscapedPath,
                Field.Type.LIST,
                list,
                field.getAttributes()
            );
            break;
          case MAP:
          case LIST_MAP:
            Map<String, FieldWithPath> map = new LinkedHashMap<>();
            for (Map.Entry<String, Field> entry : ((Map<String, Field>) field.getValue()).entrySet()) {
              String ePath1 = singleQuoteEscapedPath + "/" + EscapeUtil.singleQuoteEscape(entry.getKey());
              String ePath2 = doubleQuoteEscapedPath + "/" + EscapeUtil.doubleQuoteEscape(entry.getKey());
              Field eField = entry.getValue();
              map.put(entry.getKey(), createFieldWithPath(ePath1, ePath2, eField));
            }
            fieldWithPath = new FieldWithPath(
                singleQuoteEscapedPath,
                doubleQuoteEscapedPath,
                field.getType(),
                map,
                field.getAttributes()
            );
            break;
          default:
            fieldWithPath = new FieldWithPath(
                singleQuoteEscapedPath,
                doubleQuoteEscapedPath,
                field.getType(),
                field.getValue(),
                field.getAttributes()
            );
            break;
        }
      }
    }
    return fieldWithPath;
  }

  public FieldWithPath getValue() {
    return createFieldWithPath("", "", get());
  }

  List<PathElement> parse(String fieldPath) {
    return PathElement.parse(fieldPath, true);
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
            if (current.getType().isOneOf(Field.Type.MAP, Field.Type.LIST_MAP)) {
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
            if (current.getType().isOneOf(Field.Type.LIST, Field.Type.LIST_MAP)) {
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
          case FIELD_EXPRESSION:
          default:
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
        // the field to delete must be a primitive. delete it directly.
        deleted = value;
        value = null;
      } else {
        // the field to delete is a map or list element, so to delete, you must remove it from the parent collection.
        PathElement element = elements.get(fieldPos);
        switch (element.getType()) {
          case MAP:
            deleted = fields.get(fieldPos - 1).getValueAsMap().remove(element.getName());
            break;
          case LIST:
            deleted = fields.get(fieldPos - 1).getValueAsList().remove(element.getIndex());
            break;
          case FIELD_EXPRESSION:
          default:
            throw new IllegalStateException("Unexpected field type " + element.getType());
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
  @Deprecated
  public Set<String> getFieldPaths() {
    return gatherPaths(false);
  }

  @Override
  public Set<String> getEscapedFieldPaths() {
    return gatherPaths(true);
  }

  private Set<String> gatherPaths(boolean includeSingleQuotes) {
    Set<String> paths = new LinkedHashSet<>();
    if (value != null) {
      paths.add("");
      switch (value.getType()) {
        case MAP:
          gatherPaths("", value.getValueAsMap(), paths, includeSingleQuotes);
          break;
        case LIST:
          gatherPaths("", value.getValueAsList(), paths, includeSingleQuotes);
          break;
        case LIST_MAP:
          gatherPaths("", value.getValueAsListMap(), paths, includeSingleQuotes);
          break;
        default:
          break;
      }
    }
    return paths;
  }

  private void gatherPaths(String base, Map<String, Field> map, Set<String> paths, boolean includeSingleQuotes) {
    base += "/";
    if (map != null) {
      for (Map.Entry<String, Field> entry : map.entrySet()) {
        paths.add(base + escapeName(entry.getKey(), includeSingleQuotes));
        switch (entry.getValue().getType()) {
          case MAP:
            gatherPaths(
                base + escapeName(entry.getKey(), includeSingleQuotes),
                entry.getValue().getValueAsMap(),
                paths,
                includeSingleQuotes
            );
            break;
          case LIST:
            gatherPaths(
                base + escapeName(entry.getKey(), includeSingleQuotes),
                entry.getValue().getValueAsList(),
                paths,
                includeSingleQuotes
            );
            break;
          case LIST_MAP:
            gatherPaths(
                base + escapeName(entry.getKey(), includeSingleQuotes),
                entry.getValue().getValueAsListMap(),
                paths,
                includeSingleQuotes
            );
            break;
          default:
            // not a collection type, so don't need to do anything
            break;
        }
      }
    }
  }

  private void gatherPaths(String base, List<Field> list, Set<String> paths, boolean includeSingleQuotes) {
    if (list != null) {
      for (int i = 0; i < list.size(); i++) {
        paths.add(base + "[" + i + "]");
        Field element = list.get(i);
        switch (element.getType()) {
          case MAP:
            gatherPaths(base + "[" + i + "]", element.getValueAsMap(), paths, includeSingleQuotes);
            break;
          case LIST:
            gatherPaths(base + "[" + i + "]", element.getValueAsList(), paths, includeSingleQuotes);
            break;
          case LIST_MAP:
            gatherPaths(base + "[" + i + "]", element.getValueAsListMap(), paths, includeSingleQuotes);
            break;
          default:
            // not a collection type, no further effort needed
            break;
        }
      }
    }
  }

  private String escapeName(String name, boolean includeSingleQuotes) {
    if(includeSingleQuotes) {
      return EscapeUtil.singleQuoteEscape(name);
    } else {
      StringBuilder sb = new StringBuilder(name.length() * 2);
      char[] chars = name.toCharArray();
      for (char c : chars) {
        if (c == '/') {
          sb.append("//");
        } else if (c == '[') {
          sb.append("[[");
        } else if (c == ']') {
          sb.append("]]");
        } else {
          sb.append(c);
        }
      }
      return sb.toString();
    }
  }

  @Override
  public String toString() {
    return Utils.format("Record[headers='{}' data='{}']", header, value);
  }

  @Override
  public int hashCode() {
    return getEscapedFieldPaths().hashCode();
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

  @Override
  public Field set(String fieldPath, Field newField) {
    //get all the elements present in the fieldPath, including the newest element
    //For example, if the existing record has /a/b/c and the argument fieldPath is /a/b/d the parser returns three
    // elements - a, b and d
    List<PathElement> elements = parse(fieldPath);
    //return all *existing* fields form the list of elements
    //In the above case it is going to return only field a and field b. Field d does not exist.
    List<Field> fields = get(elements);
    Field fieldToReplace;
    int fieldPos = fields.size();
    if (elements.size() == fieldPos) {
      //The number of elements in the path is same as the number of fields => set use case
      fieldPos--;
      fieldToReplace = doSet(fieldPos, newField, elements, fields);
    } else if (elements.size() -1 == fieldPos) {
      //The number of elements in the path is on more than the number of fields => add use case
      fieldToReplace = doSet(fieldPos, newField, elements, fields);
    } else {
      throw new IllegalArgumentException(Utils.format("Field-path '{}' not reachable", fieldPath));
    }
    return fieldToReplace;
  }

  private Field doSet(int fieldPos, Field newField, List<PathElement> elements, List<Field> fields) {
    Field fieldToReplace = null;
    if (fieldPos == 0) {
      //root element
      fieldToReplace = value;
      value = newField;
    } else {
      //get the type of the element based on the output of the parser.
      //Note that this is not the real type of the field, this is how the parser interpreted the fieldPath argument
      //to the set API above. For example if fieldPath is /a/b parser interprets a as type map, if fieldPath is a[0]/b
      //parser interprets a as of type list
      switch (elements.get(fieldPos).getType()) {
        case MAP:
          //get the name of the field which must be added
          String elementName = elements.get(fieldPos).getName();
          //attempt to get the parent as a map type.
          fieldToReplace = fields.get(fieldPos - 1).getValueAsMap().put(elementName, newField);
          break;
        case LIST:
          int elementIndex = elements.get(fieldPos).getIndex();
          Field parentField = fields.get(fieldPos - 1);
          if(elementIndex == parentField.getValueAsList().size()){
            //add at end
            parentField.getValueAsList().add(newField);
          } else {
            //replace existing value
            fieldToReplace = parentField.getValueAsList().set(elementIndex, newField);
          }
          break;
        case FIELD_EXPRESSION:
        case ROOT:
          break;
      }
    }
    return fieldToReplace;
  }
}
