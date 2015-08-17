/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.generator.binary;

import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.avro.AvroDataOutputStreamGenerator;
import com.streamsets.pipeline.lib.generator.avro.AvroMessageGenerator;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class BinaryDataGeneratorFactory extends DataGeneratorFactory {

  static final String KEY_PREFIX = "binary.";
  public static final String FIELD_PATH_KEY = KEY_PREFIX + "fieldPath";
  static final String FIELD_PATH_DEFAULT = "/";

  public static final Map<String, Object> CONFIGS;

  static {
    Map<String, Object> configs = new HashMap<>();
    configs.put(FIELD_PATH_KEY, FIELD_PATH_DEFAULT);
    CONFIGS = Collections.unmodifiableMap(configs);
  }

  @SuppressWarnings("unchecked")
  public static final Set<Class<? extends Enum>> MODES = (Set) ImmutableSet.of();

  private final String fieldPath;

  public BinaryDataGeneratorFactory(Settings settings) {
    super(settings);
    fieldPath = settings.getConfig(FIELD_PATH_KEY);
    Utils.checkNotNull(fieldPath, "Binary Field Path");
  }

  @Override
  public DataGenerator getGenerator(OutputStream os) throws IOException {
    return new BinaryDataGenerator(os, fieldPath);
  }

  public Writer createWriter(OutputStream os) {
    throw new UnsupportedOperationException();
  }

}
