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

package com.streamsets.pipeline.stage.pubsub.destination;

import com.google.api.core.ApiFuture;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowController;
import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.base.Throwables;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.pubsub.lib.Errors;
import com.streamsets.pipeline.stage.pubsub.lib.Groups;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class PubSubTarget extends BaseTarget {
  private static final Logger LOG = LoggerFactory.getLogger(PubSubTarget.class);

  private final PubSubTargetConfig conf;

  private Publisher publisher;
  private DataGeneratorFactory generatorFactory;
  private List<PendingMessage> pendingMessages = new ArrayList<>();
  private ErrorRecordHandler errorRecordHandler;
  private CredentialsProvider credentialsProvider;

  private class PendingMessage {
    private final Record record;
    private final ApiFuture<String> future;

    private PendingMessage(Record record, ApiFuture<String> future) {
      this.record = record;
      this.future = future;
    }

    Record getRecord() {
      return record;
    }

    ApiFuture<String> getFuture() {
      return future;
    }
  }

  public PubSubTarget(PubSubTargetConfig conf) {
    this.conf = conf;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    pendingMessages.clear();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    if (conf.dataFormatConfig.init(
        getContext(),
        conf.dataFormat,
        Groups.DATA_FORMAT.name(),
        "conf.dataFormat.",
        issues
    )) {
      generatorFactory = conf.dataFormatConfig.getDataGeneratorFactory();
    }

    ProjectTopicName topic = ProjectTopicName.of(conf.credentials.getProjectId(), conf.topicId);

    conf.credentials.getCredentialsProvider(getContext(), issues).ifPresent(p -> credentialsProvider = p);

    try {
      FlowControlSettings.Builder flowControlSettingsBuilder = FlowControlSettings.newBuilder()
                                                                                  .setLimitExceededBehavior(
                                                                                      getLimitExceededBehaviour(conf.limitExceededBehavior))
                                                                                  .setMaxOutstandingElementCount(conf.maxOutstandingElementCount)
                                                                                  .setMaxOutstandingRequestBytes(conf.maxOutstandingRequestBytes);

      FlowControlSettings flowControlSettings = flowControlSettingsBuilder.build();

      BatchingSettings batchingSettings = BatchingSettings.newBuilder()
                                                          .setElementCountThreshold(conf.elementsCountThreshold)
                                                          .setRequestByteThreshold(conf.requestBytesThreshold)
                                                          .setDelayThreshold(
                                                              Duration.ofMillis(conf.defaultDelayThreshold))
                                                          .setIsEnabled(conf.batchingEnabled)
                                                          .setFlowControlSettings(flowControlSettings)
                                                          .build();

      publisher = Publisher.newBuilder(topic)
                           .setCredentialsProvider(credentialsProvider)
                           .setBatchingSettings(batchingSettings)
                           .build();
    } catch (IOException e) {
      LOG.error(Errors.PUBSUB_07.getMessage(), conf.topicId, e.toString(), e);
      issues.add(getContext().createConfigIssue(
          Groups.PUBSUB.name(),
          "conf.topicId",
          Errors.PUBSUB_07,
          conf.topicId,
          e.toString()
      ));
    } catch (StageException e) {
      issues.add(getContext().createConfigIssue(
          Groups.ADVANCED.name(),
          "conf.limitExceededBehavior",
          Errors.PUBSUB_09,
          conf.limitExceededBehavior.getLabel(),
          e.toString()
      ));
    }

    return issues;
  }

  @Override
  public void destroy() {
    if (publisher != null) {
      try {
        publisher.shutdown();
      } catch (Exception e) {
        LOG.warn("Error shutting down PubSub publisher: '{}'", e.toString(), e);
      }
    }
  }

  @Override
  public void write(Batch batch) {
    pendingMessages.clear();
    Iterator<Record> records = batch.getRecords();
    while (records.hasNext()) {
      publish(records.next());
    }

    // Wait for entire batch to finish
    int idx = 0;
    for (PendingMessage pendingMessage: pendingMessages) {
      try {
        pendingMessage.getFuture().get();
        ++idx;
      } catch (ExecutionException e) {
        Throwable t = Throwables.getRootCause(e);
        LOG.error(Errors.PUBSUB_08.getMessage(), t.toString(), t);
        errorRecordHandler.onError(new OnRecordErrorException(
            pendingMessage.getRecord(),
            Errors.PUBSUB_08,
            t.toString(),
            t
        ));
        ++idx;
      } catch (InterruptedException e) {
        // This is a force stop, fail the remaining pending messages.
        List<Record> errorRecords = new ArrayList<>();
        for (int i = idx; i < pendingMessages.size(); i++) {
          errorRecords.add(pendingMessages.get(i).getRecord());
        }

        LOG.error(Errors.PUBSUB_02.getMessage(), e.toString(), e);
        errorRecordHandler.onError(errorRecords, new StageException(Errors.PUBSUB_02, e.toString(), e));

        Thread.currentThread().interrupt();
      }
    }
  }

  private void publish(Record record) {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    try (DataGenerator generator = generatorFactory.getGenerator(os)) {
      generator.write(record);
    } catch (IOException | DataGeneratorException e) {
      errorRecordHandler.onError(new OnRecordErrorException(record, Errors.PUBSUB_06, e.toString(), e));
      return;
    }

    ByteString data = ByteString.copyFrom(os.toByteArray());

    Map<String, String> attributes = new HashMap<>();
    Record.Header header = record.getHeader();
    header.getAttributeNames().forEach(k -> attributes.put(k, header.getAttribute(k)));

    PubsubMessage message = PubsubMessage.newBuilder().setData(data).putAllAttributes(attributes).build();

    ApiFuture<String> messageIdFuture = publisher.publish(message);
    pendingMessages.add(new PendingMessage(record, messageIdFuture));
  }

  private FlowController.LimitExceededBehavior getLimitExceededBehaviour(LimitExceededBehaviour limitExceededBehavior) {
    switch (limitExceededBehavior) {
      case BLOCK:
        return FlowController.LimitExceededBehavior.Block;
      case IGNORE:
        return FlowController.LimitExceededBehavior.Ignore;
      case THROW_EXCEPTION:
        return FlowController.LimitExceededBehavior.ThrowException;
      default:
        throw new StageException(Errors.PUBSUB_09, limitExceededBehavior.getLabel());
    }
  }
}
