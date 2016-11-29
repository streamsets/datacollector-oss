/**
 * Copyright 2016 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.tokafka;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.http.FragmentWriter;
import com.streamsets.pipeline.lib.http.HttpConfigs;
import com.streamsets.pipeline.lib.http.HttpReceiver;
import com.streamsets.pipeline.lib.http.HttpReceiverServer;
import com.streamsets.pipeline.lib.http.HttpRequestFragmenter;
import com.streamsets.pipeline.stage.destination.kafka.KafkaTargetConfig;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public abstract class ToKafkaSource extends BaseSource implements OffsetCommitter {
  private final HttpConfigs httpConfigs;
  private final KafkaTargetConfig kafkaConfigs;
  private final int kafkaMaxMessageSizeKB;

  private HttpReceiver receiver;
  private HttpRequestFragmenter fragmenter;
  private FragmentWriter writer;
  private HttpReceiverServer server;

  private long counter;
  private BlockingQueue<Exception> errorQueue;
  private List<Exception> errorList;

  public ToKafkaSource(HttpConfigs httpConfigs, KafkaTargetConfig kafkaConfigs, int kafkaMaxMessageSizeKB) {
    this.httpConfigs = httpConfigs;
    this.kafkaConfigs = kafkaConfigs;
    this.kafkaMaxMessageSizeKB = kafkaMaxMessageSizeKB;

    // Although the following is not used it helps validate Kafka connection
    // set the data format to SDC_JSON
    kafkaConfigs.dataFormat = DataFormat.SDC_JSON;
    kafkaConfigs.dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
  }

  public HttpConfigs getHttpConfigs() {
    return httpConfigs;
  }

  public int getKafkaMaxMessageSizeKB() {
    return kafkaMaxMessageSizeKB;
  }

  protected abstract HttpRequestFragmenter createFragmenter();

  protected abstract String getUriPath();

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    // init and validate http configs
    issues.addAll(httpConfigs.init(getContext()));

    // init and validate kafka
    kafkaConfigs.init(getContext(), issues);

    if (issues.isEmpty()) {
      errorQueue = new ArrayBlockingQueue<>(100);
      errorList = new ArrayList<>(100);

      fragmenter = createFragmenter();
      issues.addAll(fragmenter.init(getContext()));
      writer = new KafkaFragmentWriter(kafkaConfigs, kafkaMaxMessageSizeKB, httpConfigs.getMaxConcurrentRequests());
      issues.addAll(writer.init(getContext()));
      receiver = new HttpReceiver(getUriPath(), httpConfigs, fragmenter, writer);
      issues.addAll(receiver.init(getContext()));
      if (issues.isEmpty()) {
        server = new HttpReceiverServer(httpConfigs, receiver, getErrorQueue());
        issues.addAll(server.init(getContext()));
      }
    }
    return issues;
  }

  @Override
  public void destroy() {
    if (server != null) {
      server.destroy();
    }
    if (receiver != null) {
      receiver.destroy();
    }
    if (writer != null) {
      writer.destroy();
    }
    if (fragmenter != null) {
      fragmenter.destroy();
    }
    kafkaConfigs.destroy();
    httpConfigs.destroy();
    super.destroy();
  }

  @VisibleForTesting
  BlockingQueue<Exception> getErrorQueue() {
    return errorQueue;
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    try {
      Thread.sleep(1000);
    } catch (InterruptedException ex) {
    }
    // report any Kafka producer errors captured by the IpcToKafkaServer's servlet
    errorList.clear();
    getErrorQueue().drainTo(errorList);
    for (Exception exception : errorList) {
      getContext().reportError(exception);
    }
    return "::tokafkasource::" + (counter++) + System.currentTimeMillis();
  }

  @Override
  public void commit(String offset) throws StageException {
  }

}
