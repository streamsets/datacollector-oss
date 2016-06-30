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
package com.streamsets.pipeline.stage.origin.udptokafka;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.udp.UDPConsumingServer;
import com.streamsets.pipeline.stage.destination.kafka.KafkaTargetConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class UDPToKafkaSource extends BaseSource implements OffsetCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(UDPToKafkaSource.class);

  public final UDPConfigBean udpConfigs;

  public final KafkaTargetConfig kafkaConfigBean;

  private final List<InetSocketAddress> addresses;
  private boolean privilegedPortUsage = false;
  KafkaUDPConsumer udpConsumer;
  private UDPConsumingServer udpServer;
  private long counter;
  private BlockingQueue<Exception> errorQueue;
  private List<Exception> errorList;

  public UDPToKafkaSource(UDPConfigBean udpConfigs, KafkaTargetConfig kafkaConfigBean) {
    this.udpConfigs = udpConfigs;
    this.kafkaConfigBean = kafkaConfigBean;
    this.addresses = new ArrayList<>();
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    issues.addAll(udpConfigs.init(getContext()));
    kafkaConfigBean.init(getContext(), DataFormat.BINARY, issues);

    for (String candidatePort : udpConfigs.ports) {
      try {
        int port = Integer.parseInt(candidatePort.trim());
        if (port > 0 && port < 65536) {
          if (port < 1024) {
            privilegedPortUsage = true; // only for error handling purposes
          }
          addresses.add(new InetSocketAddress(port));
        } else {
          issues.add(getContext().createConfigIssue(Groups.UDP.name(), "ports", Errors.UDP_KAFKA_ORIG_00, port));
        }
      } catch (NumberFormatException ex) {
        issues.add(getContext().createConfigIssue(Groups.UDP.name(),
            "ports",
            Errors.UDP_KAFKA_ORIG_00,
            candidatePort
        ));
      }
    }
    if (addresses.isEmpty()) {
      issues.add(getContext().createConfigIssue(Groups.UDP.name(), "ports", Errors.UDP_KAFKA_ORIG_03));
    }
    if (udpConfigs.concurrency < 1 || udpConfigs.concurrency > 2048) {
      issues.add(getContext().createConfigIssue(Groups.UDP.name(), "concurrency", Errors.UDP_KAFKA_ORIG_04));
    }
    if (issues.isEmpty()) {
      errorQueue = new ArrayBlockingQueue<>(100);
      errorList = new ArrayList<>(100);

      if (!addresses.isEmpty()) {
        udpConsumer = new KafkaUDPConsumer(getContext(), udpConfigs, kafkaConfigBean, errorQueue);
        udpConsumer.init();
        udpServer = new UDPConsumingServer(addresses, udpConsumer);
        try {
          udpServer.listen();
          udpServer.start();
        } catch (Exception ex) {
          udpServer.destroy();
          udpServer = null;

          if (ex instanceof SocketException && privilegedPortUsage) {
            issues.add(getContext().createConfigIssue(Groups.UDP.name(),
                "ports",
                Errors.UDP_KAFKA_ORIG_01,
                udpConfigs.ports,
                ex
            ));
          } else {
            LOG.debug("Caught exception while starting up UDP server: {}", ex);
            issues.add(getContext().createConfigIssue(null,
                null,
                Errors.UDP_KAFKA_ORIG_02,
                addresses.toString(),
                ex.toString(),
                ex
            ));
          }
        }
      }
    }
    return issues;
  }

  @Override
  public void destroy() {
    if (udpServer != null) {
      udpServer.destroy();
      udpServer = null;
    }
    if (udpConsumer != null) {
      udpConsumer.destroy();
      udpConsumer = null;
    }
    udpConfigs.destroy();
    kafkaConfigBean.destroy();
    super.destroy();
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    try {
      Thread.sleep(1000);
    } catch (InterruptedException ex) {
    }
    // report any Kafka producer errors captured by the KafkaUDPConsumer
    errorList.clear();
    errorQueue.drainTo(errorList);
    for (Exception exception : errorList) {
      getContext().reportError(exception);
    }
    return "::asyncudp::" + (counter++) + System.currentTimeMillis();
  }

  @Override
  public void commit(String offset) throws StageException {
  }

}
