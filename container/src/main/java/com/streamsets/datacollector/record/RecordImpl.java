/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.record;

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RecordImpl implements Record {
  private final HeaderImpl header;
  private Field value;

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

  private RecordImpl(RecordImpl record) {
    Preconditions.checkNotNull(record, "record cannot be null");
    header = record.header.clone();
    value = (record.value != null) ? record.value.clone() : null;
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
    private final String path;
    private final Field.Type type;
    private final Object value;

    public FieldWithPath(String path, Field.Type type, Object value) {
      this.path = path;
      this.type = type;
      this.value = value;
    }

    public String getPath() {
      return path;
    }

    public String getType() {
      return type.toString();
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
        }
      }
      return v;
    }

    @Override
    public String toString() {
      return Utils.format("FieldWithPath[path='{}', type='{}', value='{}']", getPath(), getType(), getValue());
    }

  }

  @SuppressWarnings("unchecked")
  private FieldWithPath createFieldWithPath(String path, Field field) {
    FieldWithPath fieldWithPath = null;
    if (field != null) {
      if (field.getValue() == null) {
        fieldWithPath = new FieldWithPath(path, field.getType(), null);
      } else {
        switch (field.getType()) {
          case LIST:
            List<FieldWithPath> list = new ArrayList<>();
            List<Field> fList = (List<Field>) field.getValue();
            for (int i = 0; i < fList.size(); i++) {
              String ePath = path + "[" + i + "]";
              list.add(createFieldWithPath(ePath, fList.get(i)));
            }
            fieldWithPath = new FieldWithPath(path, Field.Type.LIST, list);
            break;
          case MAP:
          case LIST_MAP:
            Map<String, FieldWithPath> map = new LinkedHashMap<>();
            for (Map.Entry<String, Field> entry : ((Map<String, Field>) field.getValue()).entrySet()) {
              String ePath = path + "/" + entry.getKey();
              Field eField = entry.getValue();
              map.put(entry.getKey(), createFieldWithPath(ePath, eField));
            }
            fieldWithPath = new FieldWithPath(path, field.getType(), map);
            break;
          default:
            fieldWithPath = new FieldWithPath(path, field.getType(), field.getValue());
            break;
        }
      }
    }
    return fieldWithPath;
  }

  public FieldWithPath getValue() {
    return createFieldWithPath("", get());
  }

  List<PathElement> parse(String fieldPath) {
    return PathElement.parse(fieldPath, false);
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
            if (current.getType() == Field.Type.MAP || current.getType() == Field.Type.LIST_MAP) {
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
            if (current.getType() == Field.Type.LIST || current.getType() == Field.Type.LIST_MAP) {
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
        case LIST_MAP:
          gatherPaths("", value.getValueAsListMap(), paths);
          break;
      }
    }
    return paths;
  }

  private void gatherPaths(String base, Map<String, Field> map, Set<String> paths) {
    base += "/";
    if (map != null) {
      for (Map.Entry<String, Field> entry : map.entrySet()) {
        paths.add(base + escapeName(entry.getKey()));
        switch (entry.getValue().getType()) {
          case MAP:
            gatherPaths(base + escapeName(entry.getKey()), entry.getValue().getValueAsMap(), paths);
            break;
          case LIST:
            gatherPaths(base + escapeName(entry.getKey()), entry.getValue().getValueAsList(), paths);
            break;
          case LIST_MAP:
            gatherPaths(base + escapeName(entry.getKey()), entry.getValue().getValueAsListMap(), paths);
            break;

        }
      }
    }
  }

  private String escapeName(String name) {
    return name.replace("/", "//").replace("[", "[[").replace("]", "]]");
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
          case LIST_MAP:
            gatherPaths(base + "[" + i + "]", element.getValueAsListMap(), paths);
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

  @Override
  public Field set(String fieldPath, Field newField) {
    //get all the elements present in the fieldPath, including the newest element
    //For example, if the existing record has /a/b/c and the argument fieldPath is /a/b/d the parser returns three
    // elements - a, b and d
    List<PathElement> elements = parse(fieldPath);
    //return all *existing* fields form the list of elements
    //In the above case it is going to return only field a and field b. Field d does not exist.
    List<Field> fields = get(elements);
    Field fieldToReplace = null;
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
      }
    }
    return fieldToReplace;
  }
}
