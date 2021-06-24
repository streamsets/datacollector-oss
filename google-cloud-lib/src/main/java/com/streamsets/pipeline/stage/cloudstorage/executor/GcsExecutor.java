/*
 * Copyright 2021 StreamSets Inc.
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
package com.streamsets.pipeline.stage.cloudstorage.executor;

import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.storage.*;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseExecutor;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.googlecloud.GoogleCloudCredentialsConfig;
import com.streamsets.pipeline.stage.cloudstorage.executor.config.GcsExecutorConfig;
import com.streamsets.pipeline.stage.cloudstorage.lib.GcsOps;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.streamsets.pipeline.stage.cloudstorage.lib.Errors.GCS_01;

public class GcsExecutor extends BaseExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(GcsExecutor.class);

  private GcsOps gcsOps;
  private ErrorRecordHandler errorRecordHandler;
  private CredentialsProvider credentialsProvider;
  private Map<String, ELEval> evals;
  private GcsExecutorConfig config;

  public GcsExecutor(GcsExecutorConfig config) {
    this.config = config;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    config.credentials.getCredentialsProvider(getContext(), issues).ifPresent(p -> credentialsProvider = p);
    try {
      Storage storage = StorageOptions.newBuilder().setCredentials(credentialsProvider.getCredentials()).build().getService();
      gcsOps = new GcsOps(storage);
    } catch (IOException e) {
      issues.add(getContext().createConfigIssue(
          com.streamsets.pipeline.stage.pubsub.lib.Groups.CREDENTIALS.name(),
          GoogleCloudCredentialsConfig.CONF_CREDENTIALS_CREDENTIALS_PROVIDER,
          GCS_01,
          e
      ));
    }
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    evals = new HashMap<>();

    // Initialize ELs
    validateEL("bucketTemplate", config.bucketTemplate, issues);
    validateEL("objectPath", config.taskConfig.objectPath, issues);
    switch (config.taskConfig.taskType) {
      case CREATE_NEW_OBJECT:
        validateEL("content", config.taskConfig.content, issues);
        break;
      case CHANGE_EXISTING_OBJECT:
        validateEL("tags", null, issues);
        break;
      case COPY_OBJECT:
        validateEL("copyTargetLocation", config.taskConfig.copyTargetLocation, issues);
        break;
    }

    return issues;
  }

  private void validateEL(String configName, String el, List<ConfigIssue> issues) {
    try {
      evals.put(configName, getContext().createELEval(configName));
      if (el != null) {
        getContext().parseEL(el);
      }
    } catch (ELEvalException e) {
      issues.add(getContext().createConfigIssue(Groups.TASKS.name(), configName, Errors.GCP_EXECUTOR_0001, e.getMessage()));
    }
  }

  @Override
  public void write(Batch batch) throws StageException {
    Iterator<Record> it = batch.getRecords();
    while (it.hasNext()) {
      Record record = it.next();
      ELVars variables = getContext().createELVars();
      RecordEL.setRecordInContext(variables, record);

      try {
        // Calculate working file (the same for all task types)
        String bucket = evaluate(record, "bucketTemplate", variables, config.bucketTemplate);
        String objectPath = evaluate(record, "objectPath", variables, config.taskConfig.objectPath);
        if (bucket.isEmpty()) {
          throw new OnRecordErrorException(record, Errors.GCP_EXECUTOR_0003);
        }
        if (objectPath.isEmpty()) {
          throw new OnRecordErrorException(record, Errors.GCP_EXECUTOR_0004);
        }
        LOG.debug("Working on {}:{}", bucket, objectPath);

        // And execute given task
        switch (config.taskConfig.taskType) {
          case CREATE_NEW_OBJECT:
            createNewObject(record, variables, bucket, objectPath);
            break;
          case COPY_OBJECT:
            copyObject(record, variables, bucket, objectPath);
            break;
          case CHANGE_EXISTING_OBJECT:
            changeExistingObject(record, variables, bucket, objectPath);
            break;
          default:
            throw new StageException(Errors.GCP_EXECUTOR_0000, "Unknown task type: " + config.taskConfig.taskType);
        }
      } catch (OnRecordErrorException e) {
        errorRecordHandler.onError(e);
      } catch (Exception e) {
        LOG.error("Can't execute S3 operation", e);
        errorRecordHandler.onError(new OnRecordErrorException(record, Errors.GCP_EXECUTOR_0000, e.toString()));
      }
    }
  }

  private void copyObject(
      Record record,
      ELVars variables,
      String bucket,
      String objectPath
  ) throws StageException {
    String newLocation = evaluate(record, "copyTargetLocation", variables, config.taskConfig.copyTargetLocation);
    BlobId sourceBlobId = BlobId.of(bucket, objectPath);
    gcsOps.copy(sourceBlobId, bucket, newLocation, config.taskConfig.dropAfterCopy);
    Events.FILE_COPIED.create(getContext())
        .with("object_key", newLocation)
        .createAndSend();
  }

  private void createNewObject(
      Record record,
      ELVars variables,
      String bucket,
      String objectPath
  ) throws OnRecordErrorException {
    // Evaluate content
    String content = evaluate(record, "content", variables, config.taskConfig.content);
    Blob blob = gcsOps.createObject(bucket, objectPath, content.getBytes());
    Events.FILE_CREATED.create(getContext())
        .with("object_key", objectPath)
        .createAndSend();
  }

  private void changeExistingObject(
      Record record,
      ELVars variables,
      String bucket,
      String objectPath
  ) throws OnRecordErrorException {
    // Tag application
    if (!config.taskConfig.tags.isEmpty()) {

      Blob blob = gcsOps.getBlob(bucket, objectPath);
      Map<String, String> metadata = blob.getMetadata();
      if (metadata == null) {
        metadata = new HashMap<String, String>();
      }
      for (Map.Entry<String, String> entry : config.taskConfig.tags.entrySet()) {
        metadata.put(
            evaluate(record, "tags", variables, entry.getKey()),
            evaluate(record, "tags", variables, entry.getValue())
        );
      }
      //metadata.putAll(config.taskConfig.tags);

      BlobInfo blobInfo = BlobInfo.newBuilder(blob.getBlobId())
          .setMetadata(metadata)
          .build();
      Blob updatedBlob = gcsOps.update(blobInfo);
      Events.FILE_CHANGED.create(getContext())
          .with("object_key", objectPath)
          .createAndSend();
    }
  }

  private String evaluate(Record record, String name, ELVars vars, String expression) throws OnRecordErrorException {
    try {
      return evals.get(name).eval(vars, expression, String.class);
    } catch (ELEvalException e) {
      throw new OnRecordErrorException(record, Errors.GCP_EXECUTOR_0002, e.toString(), e);
    }
  }

  @Override
  public void destroy() {
    super.destroy();
  }
}
