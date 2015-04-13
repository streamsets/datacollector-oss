/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.generator.json;

import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.lib.generator.CharDataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class JsonCharDataGeneratorFactory extends CharDataGeneratorFactory {

  public static final Map<String, Object> CONFIGS = new HashMap<>();

  @SuppressWarnings("unchecked")
  public static final Set<Class<? extends Enum>> MODES = (Set) ImmutableSet.of(JsonMode.class);

  private final JsonMode jsonMode;

  public JsonCharDataGeneratorFactory(Settings settings) {
    super(settings);
    this.jsonMode = settings.getMode(JsonMode.class);
  }

  @Override
  public DataGenerator getGenerator(OutputStream os) throws IOException, DataGeneratorException {
    return new JsonDataGenerator(createWriter(os), jsonMode);
  }

}
