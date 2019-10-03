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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
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
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.BASIC_AUTH_USER_INFO;
import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.BASIC_AUTH_USER_INFO_DEFAULT;
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
import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.SCHEMA_DEFAULT_SKIP_AVRO_INDEXES;
import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.SCHEMA_SKIP_AVRO_INDEXES;
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
    configs.put(SCHEMA_SKIP_AVRO_INDEXES, SCHEMA_DEFAULT_SKIP_AVRO_INDEXES);
    configs.put(BASIC_AUTH_USER_INFO, BASIC_AUTH_USER_INFO_DEFAULT);
    CONFIGS = Collections.unmodifiableMap(configs);
  }

  @SuppressWarnings("unchecked")
  public static final Set<Class<? extends Enum>> MODES = (Set) ImmutableSet.of(); // NOSONAR

  private final OriginAvroSchemaSource schemaSource;
  private final AvroSchemaHelper schemaHelper;
  private Schema schema;
  LoadingCache<Integer, Schema> schemas;
  private final boolean skipAvroUnionIndexes;

  public AvroDataParserFactory(Settings settings) throws SchemaRegistryException {
    super(settings);

    schemaHelper = new AvroSchemaHelper(settings);
    schemaSource = settings.getConfig(SCHEMA_SOURCE_KEY);

    int schemaId = settings.getConfig(SCHEMA_ID_KEY);
    final String subject = settings.getConfig(SUBJECT_KEY);

    skipAvroUnionIndexes = settings.getConfig(SCHEMA_SKIP_AVRO_INDEXES);
    schemas = CacheBuilder.newBuilder()
      .maximumSize(100)
      .build(new CacheLoader<Integer, Schema>() {
        @Override
        public Schema load(Integer schemaId) throws Exception {
          return schemaHelper.loadFromRegistry(schemaId);
        }
      });

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
    if (is instanceof ByteArrayInputStream) {
      // HACK ALERT
      // The data parser framework currently has no way to invoke the byte[] forms of getParser, so for now
      // we will have to check if we are dealing with a ByteArrayInputStream and invoke the byte[] form from
      // here.  This condition can be removed if API-328 is completed.
      try {
        return getParser(id, IOUtils.toByteArray(is));
      } catch (IOException e) {
        throw new DataParserException(Errors.DATA_PARSER_05, e.getClass().getSimpleName(), e.getMessage(), e);
      }
    }
    try {
      return new AvroDataStreamParser(
          getSettings().getContext(), schema, id, is, Long.parseLong(offset),
          getSettings().getOverRunLimit(),
          skipAvroUnionIndexes
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
      Schema recordSchema = schema;
      try {
        if (detectedSchemaId.isPresent()) {
          // Load the schema for this id from cache
          recordSchema = schemas.get(detectedSchemaId.get());

          // Strip the embedded ID
          remaining = Arrays.copyOfRange(data, MAGIC_BYTE_SIZE + ID_SIZE, data.length);
        } else {
          remaining = data;
        }
        return new AvroMessageParser(getSettings().getContext(), recordSchema, remaining, id, schemaSource, skipAvroUnionIndexes);
      } catch (IOException | ExecutionException e) {
        throw new DataParserException(Errors.DATA_PARSER_03, e.toString(), e);
      }
    }
    try {
      return new AvroMessageParser(getSettings().getContext(), schema, data, id, schemaSource, skipAvroUnionIndexes);
    } catch (IOException e) {
      throw new DataParserException(Errors.DATA_PARSER_01, e.toString(), e);
    }
  }

  @Override
  public DataParser getParser(File file, String fileOffset)
    throws DataParserException {
    try {
      return new AvroDataFileParser(getSettings().getContext(), schema, file, fileOffset,
        getSettings().getOverRunLimit(), skipAvroUnionIndexes);
    } catch (IOException e) {
      throw new DataParserException(Errors.DATA_PARSER_01, e.toString(), e);
    }
  }
}
