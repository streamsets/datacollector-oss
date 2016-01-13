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
package com.streamsets.pipeline.lib.generator.avro;

import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import org.apache.avro.Schema;

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
  public static final String DEFAULT_VALUES_KEY = KEY_PREFIX + "defaultValues";
  static final Map<String, Object> DEFAULT_VALUES_DEFAULT = new HashMap<>();
  public static final String COMPRESSION_CODEC_KEY = KEY_PREFIX + "compressionCodec";
  public static final String COMPRESSION_CODEC_DEFAULT = "null";


  public static final Map<String, Object> CONFIGS;

  static {
    Map<String, Object> configs = new HashMap<>();
    configs.put(SCHEMA_KEY, SCHEMA_DEFAULT);
    configs.put(INCLUDE_SCHEMA_KEY, INCLUDE_SCHEMA_DEFAULT);
    configs.put(DEFAULT_VALUES_KEY, DEFAULT_VALUES_DEFAULT);
    configs.put(COMPRESSION_CODEC_KEY, COMPRESSION_CODEC_DEFAULT);
    CONFIGS = Collections.unmodifiableMap(configs);
  }

  @SuppressWarnings("unchecked")
  public static final Set<Class<? extends Enum>> MODES = (Set) ImmutableSet.of();

  private final Schema schema;
  private final boolean includeSchema;
  private final Map<String, Object> defaultValuesFromSchema;
  private final String compressionCodec;

  public AvroDataGeneratorFactory(Settings settings) throws IOException {
    super(settings);
    includeSchema = settings.getConfig(INCLUDE_SCHEMA_KEY);
    schema = new Schema.Parser()
        .setValidate(true)
        .setValidateDefaults(true)
        .parse((String)settings.getConfig(SCHEMA_KEY));
    defaultValuesFromSchema = settings.getConfig(DEFAULT_VALUES_KEY);
    compressionCodec = settings.getConfig(COMPRESSION_CODEC_KEY);
    Utils.checkNotNull(schema, "Avro Schema");
  }

  @Override
  public DataGenerator getGenerator(OutputStream os) throws IOException {
    DataGenerator dataGenerator;
    if(includeSchema) {
      dataGenerator = new AvroDataOutputStreamGenerator(os, compressionCodec, schema, defaultValuesFromSchema);
    } else {
      dataGenerator = new AvroMessageGenerator(os, schema, defaultValuesFromSchema);
    }
    return dataGenerator;
  }

}
