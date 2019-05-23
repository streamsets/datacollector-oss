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
package com.streamsets.datacollector.definition;

import com.streamsets.datacollector.restapi.bean.EventDefinitionJson;
import com.streamsets.datacollector.restapi.bean.EventFieldDefinitionJson;
import com.streamsets.pipeline.api.EventDef;
import com.streamsets.pipeline.api.EventFieldDef;

import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;

/**
 * Extracts Event definition.
 */
public abstract class EventDefinitionExtractor {

  private static final EventDefinitionExtractor EXTRACTOR = new EventDefinitionExtractor() {};

  public static EventDefinitionExtractor get() {
    return EXTRACTOR;
  }

  public EventDefinitionJson extractEventDefinition(Class<?> klass) {
    EventDef eventDef = klass.getAnnotation(EventDef.class);
    if (eventDef != null) {
      List<EventFieldDefinitionJson> fieldDefinitions = new LinkedList<>();
      for (Field field : klass.getFields()) {
        EventFieldDef eventFieldDef = field.getAnnotation(EventFieldDef.class);
        if (eventFieldDef != null) {
          fieldDefinitions.add(new EventFieldDefinitionJson(
              eventFieldDef.name(),
              eventFieldDef.description(),
              eventFieldDef.optional()
          ));
        }
      }
      return new EventDefinitionJson(eventDef.type(), eventDef.description(), eventDef.version(), fieldDefinitions);
    }
    return null;
  }

}
