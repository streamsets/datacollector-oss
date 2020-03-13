/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.datacollector.restapi.rbean.json;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;
import com.fasterxml.jackson.databind.ser.impl.ObjectIdWriter;
import com.fasterxml.jackson.databind.ser.std.BeanSerializerBase;
import com.streamsets.datacollector.restapi.rbean.lang.RBean;
import com.streamsets.datacollector.restapi.rbean.lang.RType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

public class FlatRBeanSerializerModifier extends BeanSerializerModifier {
  private static final ThreadLocal<Stack<Map<String, Object>>> CONTEXT_DATA_TL = ThreadLocal.withInitial(Stack::new);

  public static void addContextData(String name, Object value) {
    if (!CONTEXT_DATA_TL.get().empty()) {
      CONTEXT_DATA_TL.get().peek().put(name, value);
    }
  }
  static class RBeanContextData extends BeanSerializerBase {

    RBeanContextData(BeanSerializerBase source) {
      super(source);
    }

    RBeanContextData(RBeanContextData source, ObjectIdWriter objectIdWriter) {
      super(source, objectIdWriter);
    }

    public BeanSerializerBase withObjectIdWriter(ObjectIdWriter objectIdWriter) {
      return new RBeanContextData(this, objectIdWriter);
    }


    public void serialize(Object bean, JsonGenerator jgen, SerializerProvider provider)
        throws IOException, JsonGenerationException {
      if (bean instanceof RBean) {
        try {
          Map<String, Object> contextData = new HashMap<>();
          CONTEXT_DATA_TL.get().push(contextData);

          jgen.writeStartObject();
          serializeFields(bean, jgen, provider);
          for (Map.Entry<String, ?> entry : contextData.entrySet()) {
            jgen.writeObjectField(entry.getKey(), entry.getValue());
          }
          jgen.writeEndObject();
        } finally {
          CONTEXT_DATA_TL.get().pop();
        }
      } else {
        jgen.writeStartObject();
        serializeFields(bean, jgen, provider);
        jgen.writeEndObject();
      }
    }

    @Override
    protected BeanSerializerBase withIgnorals(Set<String> toIgnore) {
      return null;
    }

    @Override
    protected BeanSerializerBase asArraySerializer() {
      return null;
    }

    @Override
    public BeanSerializerBase withFilterId(Object filterId) {
      return null;
    }

  }

  public JsonSerializer<?> modifySerializer(
      SerializationConfig config,
      BeanDescription beanDesc,
      JsonSerializer<?> serializer
  ) {
    // Only use the serializer for any subclasses of RType
    if (RType.class.isAssignableFrom(beanDesc.getBeanClass()) && serializer instanceof BeanSerializerBase) {
      return new RBeanContextData((BeanSerializerBase) serializer);
    }
    return serializer;
  }

}
