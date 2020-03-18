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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.streamsets.datacollector.restapi.rbean.json.swagger.RJsonModelConverter;
import com.streamsets.datacollector.restapi.rbean.lang.BaseMsg;
import com.streamsets.datacollector.restapi.rbean.lang.RBoolean;
import com.streamsets.datacollector.restapi.rbean.lang.RChar;
import com.streamsets.datacollector.restapi.rbean.lang.RDate;
import com.streamsets.datacollector.restapi.rbean.lang.RDatetime;
import com.streamsets.datacollector.restapi.rbean.lang.RDecimal;
import com.streamsets.datacollector.restapi.rbean.lang.RDouble;
import com.streamsets.datacollector.restapi.rbean.lang.REnum;
import com.streamsets.datacollector.restapi.rbean.lang.RLong;
import com.streamsets.datacollector.restapi.rbean.lang.RString;
import com.streamsets.datacollector.restapi.rbean.lang.RText;
import com.streamsets.datacollector.restapi.rbean.lang.RTime;
import com.streamsets.datacollector.restapi.rbean.lang.RValue;
import io.swagger.converter.ModelConverters;

/**
 * To remove support for NESTED JSON format get rid of the OBJECT_MAPPER as well as the REST_OBJECT_MAPPER_TL
 * and the doFlatJson() logic. Also, remove the NESTED JSON format support from the RValueJacksonSerializer and
 * the AbstractRValueJacksonDeserializer.
 */
@SuppressWarnings("unchecked")
public class RJson {


  private static class RJsonModule extends SimpleModule {
    @Override
    public void setupModule(SetupContext context) {
      context.addBeanSerializerModifier(new FlatRBeanSerializerModifier());
      super.setupModule(context);
    }
  }

  private static Module createModule() {
    SimpleModule module = new RJsonModule();
    module.addSerializer(RValue.class, new RValueJacksonSerializer());

    module.addDeserializer(RBoolean.class, new RBooleanJacksonDeserializer());
    module.addDeserializer(RChar.class, new RCharJacksonDeserializer());
    module.addDeserializer(RDate.class, new RDateJacksonDeserializer());
    module.addDeserializer(RDatetime.class, new RDatetimeJacksonDeserializer());
    module.addDeserializer(RDecimal.class, new RDecimalJacksonDeserializer());
    module.addDeserializer(RDouble.class, new RDoubleJacksonDeserializer());
    module.addDeserializer(REnum.class, new REnumJacksonDeserializer());
    module.addDeserializer(RLong.class, new RLongJacksonDeserializer());
    module.addDeserializer(RString.class, new RStringJacksonDeserializer());
    module.addDeserializer(RText.class, new RTextJacksonDeserializer());
    module.addDeserializer(RTime.class, new RTimeJacksonDeserializer());

    module.addDeserializer(BaseMsg.class, new BaseMsgJacksonDeserializer());
    return module;
  }

  public static ObjectMapper configureRJson(ObjectMapper objectMapper) {
    objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    objectMapper.registerModule(createModule());
    return objectMapper;
  }

  public static void configureRJsonForSwagger(ObjectMapper objectMapper) {
    configureRJson(objectMapper);
    ModelConverters.getInstance().addConverter(new RJsonModelConverter(objectMapper));
  }

}
