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
package com.streamsets.pipeline.lib.util;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.streamsets.pipeline.config.DestinationAvroSchemaSource;
import com.streamsets.pipeline.config.OriginAvroSchemaSource;
import com.streamsets.pipeline.lib.data.DataFactory;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.apache.commons.lang.StringUtils.isEmpty;

/**
 *
 */
public class AvroSchemaHelper {

  public static final byte MAGIC_BYTE = 0x0;
  public static final byte MAGIC_BYTE_SIZE = 1;
  public static final int ID_SIZE = 4;

  private static final String KEY_PREFIX = "avro.";

  public static final String SCHEMA_SOURCE_KEY = KEY_PREFIX + "avroSchemaSource";

  public static final String SCHEMA_REPO_URLS_KEY = KEY_PREFIX + "schemaRegistryUrls";

  public static final String SUBJECT_KEY = KEY_PREFIX + "subject";
  public static final String SUBJECT_DEFAULT = "";

  public static final String SCHEMA_ID_KEY = KEY_PREFIX + "schemaId";
  public static final int SCHEMA_ID_DEFAULT = 0;

  public static final String SCHEMA_KEY = KEY_PREFIX + "schema";
  public static final String SCHEMA_DEFAULT = "";

  public static final String SCHEMA_SKIP_AVRO_INDEXES = KEY_PREFIX + "skipAvroIndexes";
  public static final boolean SCHEMA_DEFAULT_SKIP_AVRO_INDEXES = false;

  public static final String INCLUDE_SCHEMA_KEY = KEY_PREFIX + "includeSchema";
  public static final boolean INCLUDE_SCHEMA_DEFAULT = true;

  public static final String REGISTER_SCHEMA_KEY = KEY_PREFIX + "registerSchema";
  public static final boolean REGISTER_SCHEMA_DEFAULT = false;

  public static final String DEFAULT_VALUES_KEY = KEY_PREFIX + "defaultValues";

  public static final String COMPRESSION_CODEC_KEY = KEY_PREFIX + "compressionCodec";
  public static final String COMPRESSION_CODEC_DEFAULT = "null";

  public static final String BASIC_AUTH_USER_INFO = KEY_PREFIX + "basicAuthUserInfo";
  public static final String BASIC_AUTH_USER_INFO_DEFAULT = "";

  private final SchemaRegistryClient registryClient;

  private final Cache<String, Integer> schemaIdCache;

  /**
   * AvroSchemaHelper constructor. DataFactory settings should be passed in for parsing.
   * @param settings DataFactory settings.
   */
  public AvroSchemaHelper(DataFactory.Settings settings) {
    final List<String> schemaRepoUrls = settings.getConfig(SCHEMA_REPO_URLS_KEY);
    final Object schemaSource = settings.getConfig(SCHEMA_SOURCE_KEY);
    final boolean registerSchema = settings.getConfig(REGISTER_SCHEMA_KEY);
    final boolean schemaFromRegistry =
        schemaSource == DestinationAvroSchemaSource.REGISTRY ||
        schemaSource == OriginAvroSchemaSource.REGISTRY;

    // KafkaTargetConfig passes schema repo URLs in SCHEMA_REPO_URLS_KEY regardless of whether they are
    // for schema source or schema registration, since the two are mutually exclusive
    if ((schemaFromRegistry || registerSchema) && !schemaRepoUrls.isEmpty()) {
      Map<String, String> configs = new HashMap<>();

      String userInfo = settings.getConfig(BASIC_AUTH_USER_INFO);
      if (userInfo != null &&  !userInfo.isEmpty()) {
        configs.put("basic.auth.credentials.source", "USER_INFO");
        configs.put("basic.auth.user.info", userInfo);
        registryClient = new CachedSchemaRegistryClient(schemaRepoUrls, 1000, configs);
      } else {
        registryClient = new CachedSchemaRegistryClient(schemaRepoUrls, 1000);
      }
    } else {
      registryClient = null;
    }

    // Small cache to avoid going to Schema repository all the time
    schemaIdCache = CacheBuilder.newBuilder()
      .maximumSize(100)
      .build();
  }

  /**
   * Method to allow the caller to find out if this helper was configured with a schema registry or not.
   * @return true if a valid schema registry client is available.
   */
  public boolean hasRegistryClient() {
    return registryClient != null;
  }

  /**
   * Parses and returns an Avro schema loaded from the schema registry using the provided schema ID
   * if available, or the latest version of a schema for the specified subject.
   * @param subject optional schema subject (if schema ID is provided)
   * @param schemaId optional schema ID (if subject is provided)
   * @return parsed avro schema
   * @throws SchemaRegistryException if there is an error with the registry client
   */
  public Schema loadFromRegistry(String subject, int schemaId) throws SchemaRegistryException {
    try {
      if (isEmpty(subject)) {
        return loadFromRegistry(schemaId);
      } else {
        return loadFromRegistry(subject);
      }
    } catch (SchemaRegistryException e) {
      throw new SchemaRegistryException(e);
    }
  }

  /**
   * Parses an avro schema from a string instead of the schema registry.
   * @param schema JSON string representing an Avro schema
   * @return parsed avro schema
   */
  public Schema loadFromString(String schema) {
    return AvroTypeUtil.parseSchema(schema);
  }

  /**
   * Registers a parsed schema with the schema registry under the specified subject.
   * @param schema parsed avro schema
   * @param subject subject to register the schema under
   * @return schemaId if registration was successful
   * @throws SchemaRegistryException if there is an error with the registry client
   */
  public int registerSchema(Schema schema, String subject) throws SchemaRegistryException {
    try {
      return schemaIdCache.get(subject + schema.hashCode(), () -> registryClient.register(subject, schema));
    } catch (ExecutionException  e) {
      throw new SchemaRegistryException(e);
    }
  }

  /**
   * Loads and parses a schema for the specified subject from the schema registry
   * @param subject subject for which to fetch the latest version of a schema.
   * @return parsed avro schema
   * @throws SchemaRegistryException if there is an error with the registry client
   */
  public Schema loadFromRegistry(String subject) throws SchemaRegistryException {
    try {
      SchemaMetadata metadata = registryClient.getLatestSchemaMetadata(subject);
      return registryClient.getByID(metadata.getId());
    } catch (IOException | RestClientException e) {
      throw new SchemaRegistryException(e);
    }
  }

  /**
   * Looks up schema id for the specified subject from the schema registry
   * @param subject subject for which schema Id must be looked up.
   * @return the schema id
   * @throws SchemaRegistryException if there is an error with the registry client
   */
  public int getSchemaIdFromSubject(String subject) throws SchemaRegistryException {
    try {
      SchemaMetadata metadata = registryClient.getLatestSchemaMetadata(subject);
      return metadata.getId();
    } catch (IOException | RestClientException e) {
      throw new SchemaRegistryException(e);
    }
  }

  /**
   * Loads and parses a schema for the specified schema ID from the schema registry
   * @param id schema ID to fetch from the registry
   * @return parsed avro schema
   * @throws SchemaRegistryException if there is an error with the registry client
   */
  public Schema loadFromRegistry(int id) throws SchemaRegistryException {
    try {
      return registryClient.getByID(id);
    } catch (IOException | RestClientException e) {
      throw new SchemaRegistryException(e);
    }
  }

  /**
   * Writes the magic byte and schema ID to an output stream, replicating the functionality
   * of the Confluent Kafka Avro Serializer
   * @param os OutputStream to write to
   * @return schema ID that was written
   * @throws IOException if there is an error
   */
  public int writeSchemaId(OutputStream os, int schemaId) throws IOException {
    if (schemaId > 0) {
      os.write(MAGIC_BYTE);
      os.write(ByteBuffer.allocate(ID_SIZE).putInt(schemaId).array());
    }
    return schemaId;
  }

  /**
   * Checks for a magic byte in the data and if present extracts the schemaId
   * @param data byte array representing a kafka message
   * @return parsed schema ID
   */
  public Optional<Integer> detectSchemaId(byte[] data) {
    if (data.length < 5) {
      return Optional.empty();
    }

    ByteBuffer wrapped = ByteBuffer.wrap(data);
    // 5 == MAGIC_BYTE + ID_SIZE
    if (wrapped.get() != MAGIC_BYTE) {
      return Optional.empty();
    }

    return Optional.of(wrapped.getInt());
  }

  /**
   * Helper method to extract default values from a Schema. This is normally done
   * in DataGeneratorFormat validation, however we have to do it at runtime for
   * Schema Registry.
   * @param schema schema to extract default values from
   * @return map of default value
   * @throws SchemaRegistryException
   */
  public static Map<String, Object> getDefaultValues(Schema schema) throws SchemaRegistryException {
    Map<String, Object> defaultValues = new HashMap<>();
    try {
      defaultValues.putAll(AvroTypeUtil.getDefaultValuesFromSchema(schema, new HashSet<String>()));
    } catch (IOException e) {
      throw new SchemaRegistryException(e);
    }
    return defaultValues;
  }
}
