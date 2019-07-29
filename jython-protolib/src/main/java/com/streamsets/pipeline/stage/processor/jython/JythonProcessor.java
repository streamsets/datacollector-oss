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
package com.streamsets.pipeline.stage.processor.jython;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.stage.processor.scripting.AbstractScriptingProcessor;
import com.streamsets.pipeline.stage.processor.scripting.ProcessingMode;
import com.streamsets.pipeline.stage.util.scripting.ScriptObjectFactory;
import com.streamsets.pipeline.stage.util.scripting.ScriptTypedNullObject;
import com.streamsets.pipeline.stage.util.scripting.config.ScriptRecordType;
import org.python.core.PyDictionary;
import org.python.core.PyList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptEngine;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

public class JythonProcessor extends AbstractScriptingProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(JythonProcessor.class);

  public static final String JYTHON_ENGINE = "jython";
  private final ScriptRecordType scriptRecordType;


  public JythonProcessor(ProcessingMode processingMode, String script, String initScript, String destroyScript,
                         ScriptRecordType scriptRecordType, Map<String, String> userParams) {
    super(LOG, JYTHON_ENGINE, Groups.JYTHON.name(), processingMode, script, initScript, destroyScript, userParams);
    this.scriptRecordType = scriptRecordType;
  }

  // For tests
  public JythonProcessor(ProcessingMode processingMode, String script) {
    this(processingMode, script, "", "", ScriptRecordType.NATIVE_OBJECTS, new HashMap<>());
  }

  @Override
  protected ScriptObjectFactory createScriptObjectFactory(Stage.Context context) {
    return new JythonScriptObjectFactory(engine, context, scriptRecordType);
  }

  private static class JythonScriptObjectFactory extends ScriptObjectFactory {

    public JythonScriptObjectFactory(ScriptEngine scriptEngine, Stage.Context context, ScriptRecordType scriptRecordType) {
      super(scriptEngine, context, scriptRecordType);
    }

    @Override
    public void putInMap(Object obj, Object key, Object value) {
      ((PyDictionary) obj).put(key, value);
    }

    //crude ConcurrentMap implementation (fully synchronized) baked by a LinkedHashMap to preserve Map entries ordering.
    private static class ConcurrentLinkedHashMap<K, V> extends LinkedHashMap<K, V> implements ConcurrentMap<K, V> {

      @Override
      public synchronized boolean containsValue(Object value) {
        return super.containsValue(value);
      }

      @Override
      public synchronized V get(Object key) {
        return super.get(key);
      }

      @Override
      public synchronized void clear() {
        super.clear();
      }

      @Override
      protected synchronized boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return super.removeEldestEntry(eldest);
      }

      @Override
      public synchronized int size() {
        return super.size();
      }

      @Override
      public synchronized boolean isEmpty() {
        return super.isEmpty();
      }

      @Override
      public synchronized boolean containsKey(Object key) {
        return super.containsKey(key);
      }

      @Override
      public synchronized V put(K key, V value) {
        return super.put(key, value);
      }

      @Override
      public synchronized void putAll(Map<? extends K, ? extends V> m) {
        super.putAll(m);
      }

      @Override
      public synchronized V remove(Object key) {
        return super.remove(key);
      }

      @Override
      public synchronized Object clone() {
        return super.clone();
      }

      @Override
      public synchronized Set<K> keySet() {
        return super.keySet();
      }

      @Override
      public synchronized Collection<V> values() {
        return super.values();
      }

      @Override
      public synchronized Set<Map.Entry<K, V>> entrySet() {
        return super.entrySet();
      }

      @Override
      public synchronized boolean equals(Object o) {
        return super.equals(o);
      }

      @Override
      public synchronized int hashCode() {
        return super.hashCode();
      }

      @Override
      public synchronized String toString() {
        return super.toString();
      }

      @Override
      public synchronized V putIfAbsent(K key, V value) {
        if (!containsKey(key)) {
          return put(key, value);
        } else {
          return get(key);
        }
      }

      @Override
      public synchronized boolean remove(Object key, Object value) {
        if (containsKey(key)) {
          if ((get(key) != null && get(key).equals(value)) || (get(key) == null && value == null)) {
            remove(key);
            return true;
          } else {
            return false;
          }
        } else {
          return false;
        }
      }

      @Override
      public synchronized boolean replace(K key, V oldValue, V newValue) {
        if (containsKey(key)) {
          if ((get(key) != null && get(key).equals(oldValue)) || (get(key) == null && oldValue == null)) {
            put(key, newValue);
            return true;
          } else {
            return false;
          }
        } else {
          return false;
        }
      }

      @Override
      public synchronized V replace(K key, V value) {
        if (containsKey(key)) {
          return put(key, value);
        } else {
          return null;
        }
      }

    }

    private static class PyDictionaryMapInfo extends PyDictionary implements MapInfo {
      private final boolean isListMap;

      public PyDictionaryMapInfo(boolean isListMap) {
        super(new ConcurrentLinkedHashMap<>(), true);
        this.isListMap = isListMap;
      }

      @Override
      public boolean isListMap() {
        return isListMap;
      }
    }

    @Override
    public Object createMap(boolean isListMap) {
      return new PyDictionaryMapInfo(isListMap);
    }

    @Override
    public Object createArray(List elements) {
      PyList list = new PyList();
      for (Object element : elements) {
        list.add(element);
      }
      return list;
    }

    @Override
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
      } else if (scriptObject instanceof BigInteger) { // special handling for Jython LONG type
        field = Field.create(((BigInteger) scriptObject).longValue());
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
          field = Field.create(scriptObject.toString());
        }
      }
      return field;
    }
  }

}
