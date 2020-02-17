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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.streamsets.datacollector.restapi.rbean.lang.BaseMsg;
import com.streamsets.datacollector.restapi.rbean.lang.RValue;

import java.io.IOException;

public class BaseMsgJacksonDeserializer<M extends BaseMsg<M>> extends StdDeserializer<M> {

  public BaseMsgJacksonDeserializer() {
    super(RValue.class);
  }

  @Override
  @SuppressWarnings("unchecked")
  public M deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
    jsonParser.getCodec().readTree(jsonParser);
    return null;
  }

}
