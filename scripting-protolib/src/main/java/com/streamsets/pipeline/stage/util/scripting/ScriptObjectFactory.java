/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.pipeline.stage.util.scripting;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.stage.util.scripting.config.ScriptRecordType;

import javax.script.ScriptEngine;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ScriptObjectFactory {

  protected final ScriptEngine engine;
  protected final Stage.Context context;
  private final ScriptRecordType scriptRecordType;

  public ScriptObjectFactory(
    ScriptEngine engine,
    Stage.Context context,
    ScriptRecordType scriptRecordType
  ) {
    this.engine = engine;
    this.context = context;
    this.scriptRecordType = scriptRecordType;
  }

  public ScriptRecord createScriptRecord(Record record) {
    if(scriptRecordType == ScriptRecordType.SDC_RECORDS) {
      return new SdcScriptRecord(record);
    }

    Object scriptValue = null;
    if (record.get() != null) {
      scriptValue = fieldToScript(record.get());
    }
    return new NativeScriptRecord(record, scriptValue);
  }

  @SuppressWarnings("unchecked")
  public Record getRecord(ScriptRecord scriptRecord) {
    if(scriptRecord instanceof SdcScriptRecord) {
      return ((SdcScriptRecord) scriptRecord).sdcRecord;
    }

    NativeScriptRecord nativeScriptRecord = (NativeScriptRecord) scriptRecord;
    Record record = nativeScriptRecord.sdcRecord;
    Field field = scriptToField(nativeScriptRecord.value, record, "");
    record.set(field);
    // Update Record Header Attributes
    updateRecordHeader(nativeScriptRecord.attributes, record);
    return record;
  }

  public interface MapInfo {

    public boolean isListMap();

  }

  public interface ScriptFileRef {
    public InputStream getInputStream() throws IOException;
  }

  private class ScriptFileRefImpl implements ScriptFileRef {
    private final FileRef fileRef;
    private final Stage.Context context;

    ScriptFileRefImpl(FileRef fileRef, Stage.Context context) {
      this.fileRef = fileRef;
      this.context = context;
    }

    @Override
    public InputStream getInputStream() throws IOException {
      return fileRef.createInputStream(context, InputStream.class);
    }
  }


  @SuppressWarnings("unchecked")
  public void putInMap(Object obj, Object key, Object value) {
    ((Map) obj).put(key, value);
  }

  private static class LinkedHashMapWithMapInfo extends LinkedHashMap implements MapInfo {
    private final boolean isListMap;

    public LinkedHashMapWithMapInfo(boolean isListMap) {
      this.isListMap = isListMap;
    }

    @Override
    public boolean isListMap() {
      return isListMap;
    }
  }

  public Object createMap(boolean isListMap) {
    return new LinkedHashMapWithMapInfo(isListMap);
  }

  public Object createArray(List elements) {
    return elements;
  }

  @SuppressWarnings("unchecked")
  protected Object fieldToScript(Field field) {
    Object scriptObject = null;
    if (field != null) {
      scriptObject = field.getValue();
      if (scriptObject != null) {
        switch (field.getType()) {
          case MAP:
          case LIST_MAP:
            Map<String, Field> fieldMap = (Map<String, Field>) scriptObject;
            Object scriptMap = createMap(field.getType() == Field.Type.LIST_MAP);
            for (Map.Entry<String, Field> entry : fieldMap.entrySet()) {
              putInMap(scriptMap, entry.getKey(), fieldToScript(entry.getValue()));
            }
            scriptObject = scriptMap;
            break;
          case LIST:
            List<Field> fieldArray = (List<Field>) scriptObject;
            List scripArrayElements = new ArrayList();
            for (Field aFieldArray : fieldArray) {
              scripArrayElements.add(fieldToScript(aFieldArray));
            }
            scriptObject = createArray(scripArrayElements);
            break;
          case FILE_REF:
            scriptObject = new ScriptFileRefImpl(field.getValueAsFileRef(), context);
            break;
          default:
            // no action
            break;
        }
      }
    }
    return scriptObject;
  }

  public static final Pattern PATTERN = Pattern.compile("\\W", Pattern.CASE_INSENSITIVE);

  protected static String singleQuoteEscape(String path) {
    if (path != null) {
      Matcher matcher = PATTERN.matcher(path);
      if (matcher.find()) {
        path = path.replace("\\", "\\\\")
            .replace("\"", "\\\"")
            .replace("'", "\\\\\'");
        return "'" + path + "'";
      }
    }
    return path;
  }

  protected String composeMapPath(String parent, String mapEntry) {
    return parent + "/" + singleQuoteEscape(mapEntry);
  }

  protected String composeArrayPath(String parent, int arrayIndex) {
    return parent + "[" + arrayIndex + "]";
  }

  protected void updateRecordHeader(Map<String, String> header, Record record) {
    // Transfer new or updated headers
    for (Map.Entry<String, String> entry: header.entrySet()) {
      record.getHeader().setAttribute(entry.getKey(), entry.getValue());
    }

    // Remove removed headers
    Set<String> removedHeaders = new HashSet<>(record.getHeader().getAttributeNames());
    removedHeaders.removeAll(header.keySet());
    for(String removedHeader : removedHeaders) {
      record.getHeader().deleteAttribute(removedHeader);
    }
  }

  @SuppressWarnings("unchecked")
  protected Field scriptToField(Object scriptObject, Record record, String path) {
    Field field;
    if (scriptObject != null) {
      if (scriptObject instanceof Map) {
        Map<String, Object> scriptMap = (Map<String, Object>) scriptObject;
        LinkedHashMap<String, Field> fieldMap = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : scriptMap.entrySet()) {
          fieldMap.put(entry.getKey(), scriptToField(entry.getValue(), record, composeMapPath(path, entry.getKey())));
        }
        boolean isListMap = (scriptObject instanceof MapInfo) && ((MapInfo) scriptObject).isListMap();
        field = (isListMap) ? Field.createListMap(fieldMap) : Field.create(fieldMap);
      } else if (scriptObject instanceof List) {
        List scriptArray = (List) scriptObject;
        List<Field> fieldArray = new ArrayList<>(scriptArray.size());
        for (int i = 0; i < scriptArray.size(); i++) {
          Object element = scriptArray.get(i);
          fieldArray.add(scriptToField(element, record, composeArrayPath(path, i)));
        }
        field = Field.create(fieldArray);
      } else {
        field = convertPrimitiveObject(scriptObject);
      }
    } else {
      Field originalField = record.get(path);
      if (originalField != null) {
        field = Field.create(originalField.getType(), null);
      } else {
        field = Field.create((String) null);
      }
    }
    return field;
  }

  protected Field convertPrimitiveObject(Object scriptObject) {
    Field field;
    if (scriptObject instanceof Boolean) {
      field = Field.create((Boolean) scriptObject);
    } else if (scriptObject instanceof Character) {
      field = Field.create((Character) scriptObject);
    } else if (scriptObject instanceof Byte) {
      field = Field.create((Byte) scriptObject);
    } else if (scriptObject instanceof Short) {
      field = Field.create((Short) scriptObject);
    } else if (scriptObject instanceof Integer) {
      field = Field.create((Integer) scriptObject);
    } else if (scriptObject instanceof Long) {
      field = Field.create((Long) scriptObject);
    } else if (scriptObject instanceof Float) {
      field = Field.create((Float) scriptObject);
    } else if (scriptObject instanceof Double) {
      field = Field.create((Double) scriptObject);
    } else if (scriptObject instanceof Date) {
      field = Field.createDatetime((Date) scriptObject);
    } else if (scriptObject instanceof BigDecimal) {
      field = Field.create((BigDecimal) scriptObject);
    } else if (scriptObject instanceof String) {
      field = Field.create((String) scriptObject);
    } else if (scriptObject instanceof byte[]) {
      field = Field.create((byte[]) scriptObject);
    } else if (scriptObject instanceof ScriptFileRef) {
      field = Field.create(getFileRefFromScriptFileRef((ScriptFileRef)scriptObject));
    } else {
      field = ScriptTypedNullObject.getTypedNullFieldFromScript(scriptObject);
      if (field == null) {
        // unable to find field type from scriptObject. Return null String.
        if (scriptObject == null) {
          field = Field.create(Field.Type.STRING, null);
        } else {
          field = Field.create(scriptObject.toString());
        }
      }
    }
    return field;
  }

  protected FileRef getFileRefFromScriptFileRef(ScriptFileRef scriptObject) {
    return ((ScriptFileRefImpl)scriptObject).fileRef;
  }
}
