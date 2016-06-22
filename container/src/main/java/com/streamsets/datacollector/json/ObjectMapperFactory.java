/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.json;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.json.MetricsModule;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.streamsets.datacollector.metrics.ExtendedMeter;
import com.streamsets.datacollector.record.FieldDeserializer;
import com.streamsets.datacollector.restapi.bean.FieldJson;
import com.streamsets.pipeline.api.impl.ErrorMessage;

import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

public class ObjectMapperFactory {

  private static final ObjectMapper OBJECT_MAPPER = create(true);
  private static final ObjectMapper OBJECT_MAPPER_ONE_LINE = create(false);

  private ObjectMapperFactory() {}

  private static ObjectMapper create(boolean indent) {
    ObjectMapper objectMapper = new ObjectMapper();
    // This will cause the objectmapper to not close the underlying output stream
    objectMapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
    objectMapper.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
    objectMapper.registerModule(new MetricsModule(TimeUnit.SECONDS, TimeUnit.SECONDS, false, MetricFilter.ALL));
    SimpleModule module = new SimpleModule();
    module.addDeserializer(FieldJson.class, new FieldDeserializer());
    module.addSerializer(ExtendedMeter.class, new ExtendedMeterSerializer(TimeUnit.SECONDS));
    module.addDeserializer(ErrorMessage.class, new ErrorMessageDeserializer());
    module.addSerializer(BigDecimal.class, new ToStringSerializer());
    objectMapper.registerModule(module);
    if (indent) {
      objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
    }
    return objectMapper;
  }

  public static ObjectMapper get() {
    return OBJECT_MAPPER;
  }

  public static ObjectMapper getOneLine() {
    return OBJECT_MAPPER_ONE_LINE;
  }

}
