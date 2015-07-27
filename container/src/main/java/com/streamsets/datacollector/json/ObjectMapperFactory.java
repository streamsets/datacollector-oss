/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.json;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.json.MetricsModule;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.core.JsonGenerator;
import com.streamsets.datacollector.metrics.ExtendedMeter;
import com.streamsets.datacollector.record.FieldDeserializer;
import com.streamsets.datacollector.restapi.bean.FieldJson;
import com.streamsets.pipeline.api.impl.ErrorMessage;

import java.util.concurrent.TimeUnit;

public class ObjectMapperFactory {

  private static final ObjectMapper OBJECT_MAPPER = create(true);
  private static final ObjectMapper OBJECT_MAPPER_ONE_LINE = create(false);

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
