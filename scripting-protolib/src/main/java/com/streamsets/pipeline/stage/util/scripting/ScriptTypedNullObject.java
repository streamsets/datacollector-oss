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
import com.streamsets.pipeline.api.Record;

import javax.script.SimpleBindings;

public class ScriptTypedNullObject {

  public static final Object NULL_BOOLEAN = new Object();
  public static final Object NULL_CHAR = new Object(); //Groovy support char
  public static final Object NULL_BYTE = new Object(); //Groovy support byte
  public static final Object NULL_SHORT = new Object(); //Groovy support short
  public static final Object NULL_INTEGER = new Object();
  public static final Object NULL_LONG = new Object();
  public static final Object NULL_FLOAT = new Object();
  public static final Object NULL_DOUBLE = new Object();
  public static final Object NULL_DATE = new Object();
  public static final Object NULL_DATETIME = new Object();
  public static final Object NULL_TIME = new Object();
  public static final Object NULL_DECIMAL = new Object();
  public static final Object NULL_BYTE_ARRAY = new Object();
  public static final Object NULL_STRING = new Object();
  public static final Object NULL_LIST = new Object();
  public static final Object NULL_MAP = new Object();

  /**
   * Receive record and fieldPath from scripting processor.
   * It resolves type of the field, and if value is null, it returns
   * one of the NULL_XXX objects defined in this class.
   * If field value is not null, it returns the value stored in the field.
   * @param record Record
   * @param fieldPath field Path
   * @return One of the NULL_XXX objects if field has type but value is null.
   */
  public static Object getFieldNull(Record record, String fieldPath) {
    Field f = record.get(fieldPath);
    if (f != null ) {
      return f.getValue() == null? getTypedNullFromField(f) : f.getValue();
    }
    return null;
  }

  /**
   * Receive a scriptOject and find out if the scriptObect is one of the NULL_**
   * object defined in this class. If so, create a new field with the type
   * and null value, then return the field.
   * If the scriptObject is not one of the typed null object, it returns a
   * new field with string converted from the value.
   * @param scriptObject: ScriptObject, this might be one of the Typed Null object.
   * @return
   */
  public static Field getTypedNullFieldFromScript(Object scriptObject) {
    Field field;
    if(scriptObject == NULL_BOOLEAN)
      field = Field.create(Field.Type.BOOLEAN, null);
    else if(scriptObject == NULL_CHAR)
      field = Field.create(Field.Type.CHAR, null);
    else if(scriptObject == NULL_BYTE)
      field = Field.create(Field.Type.BYTE, null);
    else if(scriptObject == NULL_SHORT)
      field = Field.create(Field.Type.SHORT, null);
    else if (scriptObject == NULL_INTEGER)
      field = Field.create(Field.Type.INTEGER, null);
    else if(scriptObject == NULL_LONG)
      field = Field.create(Field.Type.LONG, null);
    else if (scriptObject == NULL_FLOAT)
      field = Field.create(Field.Type.FLOAT, null);
    else if(scriptObject == NULL_DOUBLE)
      field = Field.create(Field.Type.DOUBLE, null);
    else if(scriptObject == NULL_DATE)
      field = Field.createDate(null);
    else if(scriptObject == NULL_DATETIME)
      field = Field.createDatetime(null);
    else if(scriptObject == NULL_TIME)
      field = Field.createTime(null);
    else if(scriptObject == NULL_DECIMAL)
      field = Field.create(Field.Type.DECIMAL, null);
    else if(scriptObject == NULL_BYTE_ARRAY)
      field = Field.create(Field.Type.BYTE_ARRAY, null);
    else if(scriptObject == NULL_STRING)
      field = Field.create(Field.Type.STRING, null);
    else if(scriptObject == NULL_LIST)
      field = Field.create(Field.Type.LIST, null);
    else if(scriptObject == NULL_MAP)
      field = Field.create(Field.Type.MAP, null);
    else  //this scriptObject is not Null typed field. Return null.
      field = null;
    return field;
  }

  public static Object getTypedNullFromField(Field field) {
    switch (field.getType()) {
      case BOOLEAN:
        return NULL_BOOLEAN;
      case CHAR:
        return NULL_CHAR;
      case SHORT:
        return NULL_SHORT;
      case INTEGER:
        return NULL_INTEGER;
      case LONG:
        return NULL_LONG;
      case FLOAT:
        return NULL_FLOAT;
      case DOUBLE:
        return NULL_DOUBLE;
      case DATE:
        return NULL_DATE;
      case DATETIME:
        return NULL_DATETIME;
      case DECIMAL:
        return NULL_DECIMAL;
      case BYTE_ARRAY:
        return NULL_BYTE_ARRAY;
      case STRING:
        return NULL_STRING;
      case LIST:
        return NULL_LIST;
      case MAP:
        return NULL_MAP;
      default:
        return null;
    }
  }
}
