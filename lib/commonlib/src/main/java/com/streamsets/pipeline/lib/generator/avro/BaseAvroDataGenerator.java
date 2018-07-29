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
package com.streamsets.pipeline.lib.generator.avro;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.lib.util.AvroSchemaHelper;
import com.streamsets.pipeline.lib.util.AvroTypeUtil;
import com.streamsets.pipeline.lib.util.SchemaRegistryException;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;

/**
 * Base Avro data generator that provides shared logic for both message data output stream implementations.
 */
abstract public class BaseAvroDataGenerator implements DataGenerator {

  private static Logger LOG = LoggerFactory.getLogger(BaseAvroDataGenerator.class);

  /**
   * Header name containing JSON encoded AVRO schema
   */
  public static final String AVRO_SCHEMA_HEADER = "avroSchema";

  /**
   * Initialize output writer
   */
  abstract protected void initializeWriter() throws IOException;

  /**
   * Hook that will be called once initialize() is successfully done
   */
  protected void postInitialize() throws IOException {
  }

  /**
   * Write the given record out.
   */
  abstract protected void writeRecord(Record record) throws IOException, DataGeneratorException;

  /**
   * Object that we should call flush() on.
   */
  abstract protected Flushable getFlushable();

  /**
   * Object that we should call close() on.
   * @return
   */
  abstract protected Closeable getCloseable();

  /**
   * If true, then each record must have header avroSchema describing the schema.
   */
  protected boolean schemaInHeader;

  /**
   * Hashcode of the schema that was used to initialize the writer if getting schema from header
   */
  private int schemaHashCode;

  /**
   * Avro schema, can be null on creation, will be filled with value before calling initializeWriter()
   */
  protected Schema schema;

  /**
   * Confluent Avro Schema Repository id (if used).
   */
  protected int schemaId;

  /**
   * Map for default values from the avro schema.
   */
  protected Map<String, Object> defaultValueMap;

  /**
   * Subject of the schema.
   */
  protected final String schemaSubject;

  /**
   * Avro Schema helper object to work with schema repository.
   */
  protected final AvroSchemaHelper schemaHelper;

  /**
   * State of the generator
   */
  enum State {
    CREATED,  // Freshly created, schema/writers are not available
    OPENED,   // Schema is available, writers are opened
    CLOSED,   // Done writing
  }
  private State state;

  public BaseAvroDataGenerator(
      boolean schemaInHeader,
      Schema schema,
      Map<String, Object> defaultValueMap,
      AvroSchemaHelper schemaHelper,
      String schemaSubject,
      int schemaId
  ) throws IOException {
    this.state = State.CREATED;
    this.schemaInHeader = schemaInHeader;
    this.schema = schema;
    this.defaultValueMap = defaultValueMap;
    this.schemaSubject = schemaSubject;
    this.schemaHelper = schemaHelper;
    this.schemaId = schemaId;
  }

  protected void initialize() throws IOException {
    initializeWriter();

    // Schema registration is delayed with using it in header until this point
    if(schemaInHeader && schemaHelper != null && schemaHelper.hasRegistryClient()) {
      try {
        schemaId = schemaHelper.registerSchema(schema, schemaSubject);
      } catch (SchemaRegistryException e) {
        throw new IOException("Can't initialize writer: " + e.toString(), e);
      }
    }

    // Switch state to opened
    state = State.OPENED;

    // And run post initialize hook
    postInitialize();
  }

  private void initializeSchemaFromRecord(Record record) throws IOException, DataGeneratorException {
    String jsonSchema = AvroTypeUtil.getAvroSchemaFromHeader(record, AVRO_SCHEMA_HEADER);
    schemaHashCode = jsonSchema.hashCode();
    schema = AvroTypeUtil.parseSchema(jsonSchema);
    defaultValueMap = AvroTypeUtil.getDefaultValuesFromSchema(schema, new HashSet<String>());
    initialize();
  }

  @Override
  public void write(Record record) throws IOException, DataGeneratorException {
    if (schemaInHeader) {
      if (state == State.CREATED) {
        initializeSchemaFromRecord(record);
      } else {
        String newAvroSchema = AvroTypeUtil.getAvroSchemaFromHeader(record, AVRO_SCHEMA_HEADER);
        if (schemaHashCode != newAvroSchema.hashCode()) {
          LOG.error(
              "Record {} has a different schema. Expected: {}  Actual(Initialized): {}",
              record.getHeader().getSourceId(),
              schema.toString(),
              newAvroSchema
          );
          throw new DataGeneratorException(Errors.AVRO_GENERATOR_04,
            record.getHeader().getSourceId(),
            schema.toString(),
            newAvroSchema
          );
        }
      }
    }

    if (state == State.CLOSED) {
      throw new IOException("generator has been closed");
    }

    writeRecord(record);
  }

  @Override
  public void flush() throws IOException {
    if (state == State.CLOSED) {
      throw new IOException("generator has been closed");
    }

    if(state == State.OPENED) {
      getFlushable().flush();
    }
  }

  @Override
  public void close() throws IOException {
    if(state == State.CLOSED) {
      return;
    }

    if(state == State.OPENED) {
      try {
        getFlushable().flush();
      } finally {
        getCloseable().close();
      }
    }

    state = State.CLOSED;
  }
}
