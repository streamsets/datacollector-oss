/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.json;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.json.MetricsModule;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.metrics.ExtendedMeter;
import com.streamsets.pipeline.record.FieldDeserializer;

import java.util.concurrent.TimeUnit;

public class ObjectMapperFactory {

  private static final ObjectMapper OBJECT_MAPPER = create();

  private static ObjectMapper create() {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.registerModule(new MetricsModule(TimeUnit.SECONDS, TimeUnit.SECONDS, false, MetricFilter.ALL));
    SimpleModule module = new SimpleModule();
    module.addDeserializer(Field.class, new FieldDeserializer());
    module.addSerializer(ExtendedMeter.class, new ExtendedMeterSerializer(TimeUnit.SECONDS));
    module.addDeserializer(ErrorMessage.class, new ErrorMessageDeserializer());
    objectMapper.registerModule(module);
    objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
    return objectMapper;
  }

  public static ObjectMapper get() {
    return OBJECT_MAPPER;
  }

}
