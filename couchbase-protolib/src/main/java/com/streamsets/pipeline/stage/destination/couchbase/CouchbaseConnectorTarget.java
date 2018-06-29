/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.destination.couchbase;

import com.streamsets.pipeline.api.base.RecordTarget;
import com.streamsets.pipeline.stage.destination.couchbase.lib.CouchbaseConnector;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.streamsets.pipeline.stage.destination.couchbase.lib.Errors;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.ext.json.Mode;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactoryBuilder;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This target is a used to connect to a Couchbase NoSQL Database.
 */
public class CouchbaseConnectorTarget extends RecordTarget {

  private static final Logger LOG = LoggerFactory.getLogger(CouchbaseConnectorTarget.class);

  private final CouchbaseTargetConfiguration config;

  private CouchbaseConnector connector;
  private DataGeneratorFactory generatorFactory;

  public CouchbaseConnectorTarget(CouchbaseTargetConfiguration config) {
    this.config = config;
  }

  @Override
  protected List<ConfigIssue> init() {
    // Validate configuration values and open any required resources.
    List<ConfigIssue> issues = super.init();

    // Connect to Couchbase DB
    LOG.info("Connecting to Couchbase " + config.version +  " with details: " + config.URL + " " + config.bucket);

    // Check Couchbase Version
    try {
      if (config.version == CouchbaseVersionTypes.VERSION4) {
        connector = CouchbaseConnector.getInstance(
          config.URL,
          config.bucket,
          config.bucketPassword.get()
        );
      } else {
        connector = CouchbaseConnector.getInstance(
          config.URL,
          config.bucket,
          config.userName.get(),
          config.userPassword.get()
        );
      }
    } catch (StageException e) {
      issues.add(getContext().createConfigIssue(
        "CONNECTION",
        "config.URL",
        Errors.ERROR_03,
        e.toString(),
        e
      ));
    }

    //Data Generator for JSON Objects to Couchbase
    DataGeneratorFactoryBuilder builder = new DataGeneratorFactoryBuilder(
        getContext(),
        DataFormat.JSON.getGeneratorFormat()
    );
    builder.setCharset(StandardCharsets.UTF_8);
    builder.setMode(Mode.MULTIPLE_OBJECTS);
    generatorFactory = builder.build();

    // If issues is not empty, the UI will inform the user of each configuration issue in the list.
    return issues;
  }

  @Override
  public void destroy() {
    // Clean up any open resources.
    super.destroy();
    if(connector != null) {
      connector.close();
    }
  }

  /**
   * Writes a single record to the destination.
   *
   * @param record the record to write to the destination.
   * @throws OnRecordErrorException when a record cannot be written.
   */
  public void write(Record record) throws OnRecordErrorException {
    try {
      // Generate data from the record object and create JsonObject from byte ARRAY String
      ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
      try (DataGenerator generator = generatorFactory.getGenerator(baos)) {
        generator.write(record);
      }
      JsonObject jsonObject = JsonObject.fromJson(new String(baos.toByteArray()));

      // Either get key JSON or generate unique one
      Object keyObject = null;

      if (config.generateDocumentKey) {
        UUID uuid = UUID.randomUUID();
        keyObject = uuid.toString();
      } else {
        keyObject = jsonObject.get(config.documentKey);
        if (keyObject == null) {
          throw new OnRecordErrorException(record, Errors.ERROR_00);
        }
      }

      String keyString = keyObject.toString();

      //Write to Couchbase DB
      connector.writeToBucket(keyString, jsonObject);

    } catch (StageException se) {
      throw new OnRecordErrorException(record, Errors.ERROR_00);
    } catch (IOException ioe) {
      throw new OnRecordErrorException(record, Errors.ERROR_01);
    }
  }
}
