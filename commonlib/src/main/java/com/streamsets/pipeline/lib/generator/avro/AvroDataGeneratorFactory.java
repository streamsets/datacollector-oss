/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.generator.avro;

import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class AvroDataGeneratorFactory extends DataGeneratorFactory {

  static final String KEY_PREFIX = "avro.";
  public static final String SCHEMA_KEY = KEY_PREFIX + "schema";
  static final String SCHEMA_DEFAULT = "";
  public static final String INCLUDE_SCHEMA_KEY = KEY_PREFIX + "includeSchema";
  static final boolean INCLUDE_SCHEMA_DEFAULT = true;

  public static final Map<String, Object> CONFIGS;

  static {
    Map<String, Object> configs = new HashMap<>();
    configs.put(SCHEMA_KEY, SCHEMA_DEFAULT);
    configs.put(INCLUDE_SCHEMA_KEY, INCLUDE_SCHEMA_DEFAULT);
    CONFIGS = Collections.unmodifiableMap(configs);
  }

  @SuppressWarnings("unchecked")
  public static final Set<Class<? extends Enum>> MODES = (Set) ImmutableSet.of();

  private final String schema;
  private final boolean includeSchema;

  public AvroDataGeneratorFactory(Settings settings) {
    super(settings);
    schema = settings.getConfig(SCHEMA_KEY);
    includeSchema = settings.getConfig(INCLUDE_SCHEMA_KEY);

    Utils.checkNotNull(schema, "Avro Schema");
  }

  @Override
  public DataGenerator getGenerator(OutputStream os) throws IOException, DataGeneratorException {
    DataGenerator dataGenerator;
    if(includeSchema) {
      dataGenerator = new AvroDataOutputStreamGenerator(os, schema);
    } else {
      dataGenerator = new AvroMessageGenerator(os, schema);
    }
    return dataGenerator;
  }

}
