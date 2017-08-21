/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.pipeline.lib.parser.avro;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.OriginAvroSchemaSource;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.Errors;
import com.streamsets.pipeline.lib.util.AvroSchemaHelper;
import com.streamsets.pipeline.lib.util.SchemaRegistryException;
import org.apache.avro.Schema;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.ID_SIZE;
import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.MAGIC_BYTE_SIZE;
import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.REGISTER_SCHEMA_DEFAULT;
import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.REGISTER_SCHEMA_KEY;
import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.SCHEMA_DEFAULT;
import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.SCHEMA_ID_DEFAULT;
import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.SCHEMA_ID_KEY;
import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.SCHEMA_KEY;
import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.SCHEMA_REPO_URLS_KEY;
import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.SCHEMA_SOURCE_KEY;
import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.SUBJECT_DEFAULT;
import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.SUBJECT_KEY;
import static org.apache.commons.lang.StringUtils.isEmpty;


public class AvroDataParserFactory extends DataParserFactory {
  public static final Map<String, Object> CONFIGS;
  private static final OriginAvroSchemaSource SCHEMA_SOURCE_DEFAULT = OriginAvroSchemaSource.INLINE;

  static {
    Map<String, Object> configs = new HashMap<>();
    configs.put(SCHEMA_KEY, SCHEMA_DEFAULT);
    configs.put(SCHEMA_SOURCE_KEY, SCHEMA_SOURCE_DEFAULT);
    configs.put(SCHEMA_ID_KEY, SCHEMA_ID_DEFAULT);
    configs.put(SUBJECT_KEY, SUBJECT_DEFAULT);
    configs.put(SCHEMA_REPO_URLS_KEY, new ArrayList<>());
    configs.put(REGISTER_SCHEMA_KEY, REGISTER_SCHEMA_DEFAULT);
    CONFIGS = Collections.unmodifiableMap(configs);
  }

  @SuppressWarnings("unchecked")
  public static final Set<Class<? extends Enum>> MODES = (Set) ImmutableSet.of(); // NOSONAR

  private final OriginAvroSchemaSource schemaSource;
  private final AvroSchemaHelper schemaHelper;
  private Schema schema;
  private int schemaId = -1;

  public AvroDataParserFactory(Settings settings) throws SchemaRegistryException {
    super(settings);

    schemaHelper = new AvroSchemaHelper(settings);
    schemaSource = settings.getConfig(SCHEMA_SOURCE_KEY);

    int schemaId = settings.getConfig(SCHEMA_ID_KEY);
    final String subject = settings.getConfig(SUBJECT_KEY);

    switch (schemaSource) {
      // Load from the registry now if it was specified automatically,
      // otherwise we'll try to load schema from the records themselves
      // using an embedded schemaId
      case REGISTRY:
        if (schemaId > 0 || !isEmpty(subject)) {
          schema = schemaHelper.loadFromRegistry(subject, schemaId);
        }
        break;
      case INLINE:
        schema = schemaHelper.loadFromString((String) settings.getConfig(SCHEMA_KEY));
        if (schemaHelper.hasRegistryClient()) {
          schemaHelper.registerSchema(schema, subject);
        }
        Utils.checkNotNull(schema, "Avro Schema");
        break;
      case SOURCE:
        // no-op
        break;
      default:
        throw new UnsupportedOperationException("Unsupported Avro Schema source: " + schemaSource.getLabel());
    }

  }

  @Override
  public DataParser getParser(String id, InputStream is, String offset) throws DataParserException {
    try {
      return new AvroDataStreamParser(
          getSettings().getContext(), schema, id, is, Long.parseLong(offset),
          getSettings().getOverRunLimit()
      );
    } catch (IOException e) {
      throw new DataParserException(Errors.DATA_PARSER_01, e.toString(), e);
    }
  }

  @Override
  public DataParser getParser(String id, Reader reader, long offset) throws DataParserException {
    throw new UnsupportedOperationException();
  }

  @Override
  public DataParser getParser(String id, byte[] data) throws DataParserException {
    if (schemaSource == OriginAvroSchemaSource.REGISTRY) {
      Optional<Integer> detectedSchemaId = schemaHelper.detectSchemaId(data);
      byte[] remaining;
      try {
        if (detectedSchemaId.isPresent()) {
          // Load the schema if it doesn't match the schema ID we have
          if (detectedSchemaId.get() != schemaId) { // NOSONAR
            schemaId = detectedSchemaId.get();
            schema = schemaHelper.loadFromRegistry(schemaId);
          }
          // Strip the embedded ID
          remaining = Arrays.copyOfRange(data, MAGIC_BYTE_SIZE + ID_SIZE, data.length);
        } else {
          remaining = data;
        }
        return new AvroMessageParser(getSettings().getContext(), schema, remaining, id, schemaSource);
      } catch (SchemaRegistryException | IOException e) {
        throw new DataParserException(Errors.DATA_PARSER_03, e.toString(), e);
      }
    }
    try {
      return new AvroMessageParser(getSettings().getContext(), schema, data, id, schemaSource);
    } catch (IOException e) {
      throw new DataParserException(Errors.DATA_PARSER_01, e.toString(), e);
    }
  }

  @Override
  public DataParser getParser(File file, String fileOffset)
    throws DataParserException {
    try {
      return new AvroDataFileParser(getSettings().getContext(), schema, file, fileOffset,
        getSettings().getOverRunLimit());
    } catch (IOException e) {
      throw new DataParserException(Errors.DATA_PARSER_01, e.toString(), e);
    }
  }
}
