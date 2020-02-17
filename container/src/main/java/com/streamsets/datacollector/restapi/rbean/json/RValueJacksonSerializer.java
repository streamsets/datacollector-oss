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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonStreamContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.restapi.rbean.lang.RString;
import com.streamsets.datacollector.restapi.rbean.lang.RValue;

import java.io.IOException;
import java.util.List;

public class RValueJacksonSerializer extends StdSerializer<RValue> {

  public static final String SCRUBBED_ELEMENT = "********";
  public static final List<String> SCRUBBED = ImmutableList.of(SCRUBBED_ELEMENT);

  public RValueJacksonSerializer() {
    super(RValue.class);
  }

  String getPropertyName(JsonStreamContext context) {
    String name;
    if (context.inObject()) {
      name = context.getCurrentName();
    } else if (context.inArray()) {
      name = context.getParent().getCurrentName() + String.format("[%d]", context.getEntryCount());
    } else {
      name = "<root>";
    }
    return name;
  }


  @Override
  public void serialize(
      RValue value, JsonGenerator gen, SerializerProvider provider
  ) throws IOException {
    if (!value.getMessages().isEmpty()) {
      FlatRBeanSerializerModifier.addContextData(
          "#messages#" + getPropertyName(gen.getOutputContext()),
          value.getMessages()
      );
    }
    if (value instanceof RString) {
      RString rString = (RString) value;
      if (rString.getAlt() != null) {
        FlatRBeanSerializerModifier.addContextData(
            "#alt#" + getPropertyName(gen.getOutputContext()),
            rString.getAlt()
        );
      }
    }
    if (value.isScrubbed()) {
      gen.writeObject(SCRUBBED);
    } else {
      gen.writeObject(value.getValue());
    }
  }

}
