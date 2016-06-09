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
import com.streamsets.pipeline.config.DestinationAvroSchemaSource;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.util.AvroSchemaHelper;
import com.streamsets.pipeline.lib.util.SchemaRegistryException;
import org.apache.avro.Schema;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.COMPRESSION_CODEC_DEFAULT;
import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.COMPRESSION_CODEC_KEY;
import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.DEFAULT_VALUES_KEY;
import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.INCLUDE_SCHEMA_DEFAULT;
import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.INCLUDE_SCHEMA_KEY;
import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.SCHEMA_DEFAULT;
import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.SCHEMA_ID_DEFAULT;
import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.SCHEMA_ID_KEY;
import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.SCHEMA_KEY;
import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.SCHEMA_REPO_URLS_KEY;
import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.SCHEMA_SOURCE_KEY;
import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.SUBJECT_DEFAULT;
import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.SUBJECT_KEY;

public class AvroDataGeneratorFactory extends DataGeneratorFactory {
  public static final Map<String, Object> CONFIGS;
  private static final DestinationAvroSchemaSource SCHEMA_SOURCE_DEFAULT = DestinationAvroSchemaSource.INLINE;

  static {
    Map<String, Object> configs = new HashMap<>();
    configs.put(SCHEMA_KEY, SCHEMA_DEFAULT);
    configs.put(SCHEMA_SOURCE_KEY, SCHEMA_SOURCE_DEFAULT);
    configs.put(SCHEMA_ID_KEY, SCHEMA_ID_DEFAULT);
    configs.put(SUBJECT_KEY, SUBJECT_DEFAULT);
    configs.put(SCHEMA_REPO_URLS_KEY, new ArrayList<>());
    configs.put(INCLUDE_SCHEMA_KEY, INCLUDE_SCHEMA_DEFAULT);
    configs.put(DEFAULT_VALUES_KEY, new HashMap<>());
    configs.put(COMPRESSION_CODEC_KEY, COMPRESSION_CODEC_DEFAULT);
    CONFIGS = Collections.unmodifiableMap(configs);
  }

  @SuppressWarnings("unchecked")
  public static final Set<Class<? extends Enum>> MODES = (Set) ImmutableSet.of(); // NOSONAR

  private final AvroSchemaHelper schemaHelper;
  private final DestinationAvroSchemaSource schemaSource;
  private final boolean includeSchema;
  private final Map<String, Object> defaultValuesFromSchema;
  private final String compressionCodec;

  private Schema schema;
  private int schemaId = 0;

  public AvroDataGeneratorFactory(Settings settings) throws SchemaRegistryException {
    super(settings);

    schemaHelper = new AvroSchemaHelper(settings);

    includeSchema = settings.getConfig(INCLUDE_SCHEMA_KEY);
    schemaSource = settings.getConfig(SCHEMA_SOURCE_KEY);

    schemaId = settings.getConfig(SCHEMA_ID_KEY);
    final String subject = settings.getConfig(SUBJECT_KEY);

    switch (schemaSource) {
      case HEADER:
        schema = null;
        break;
      case REGISTRY:
        schema = schemaHelper.loadFromRegistry(subject, schemaId);
        break;
      case INLINE:
        schema = schemaHelper.loadFromString((String) settings.getConfig(SCHEMA_KEY));
        if (schemaHelper.hasRegistryClient()) {
          schemaId = schemaHelper.registerSchema(schema, subject);
        }
        Utils.checkNotNull(schema, "Avro Schema");
        break;
      default:
        throw new UnsupportedOperationException("Unsupported Avro Schema source: " + schemaSource.getLabel());
    }
    defaultValuesFromSchema = settings.getConfig(DEFAULT_VALUES_KEY);
    compressionCodec = settings.getConfig(COMPRESSION_CODEC_KEY);
  }

  @Override
  public DataGenerator getGenerator(OutputStream os) throws IOException {
    DataGenerator dataGenerator;
    boolean schemaInHeader = schemaSource == DestinationAvroSchemaSource.HEADER;

    if (includeSchema) {
      dataGenerator = new AvroDataOutputStreamGenerator(
          schemaInHeader,
          os,
          compressionCodec,
          schema,
          defaultValuesFromSchema
      );
    } else {
      // If using Confluent Kafka Serializer we must write the magic byte
      if (schemaHelper.hasRegistryClient() && schemaId > 0) {
        schemaHelper.writeSchemaId(os, schemaId);
      }
      dataGenerator = new AvroMessageGenerator(schemaInHeader, os, schema, defaultValuesFromSchema);
    }
    return dataGenerator;
  }

}
