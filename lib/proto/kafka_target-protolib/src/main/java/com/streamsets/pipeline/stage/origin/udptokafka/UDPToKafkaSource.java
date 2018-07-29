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

import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class UDPToKafkaSource extends BaseSource implements OffsetCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(UDPToKafkaSource.class);

  private final UDPConfigBean udpConfigs;

  public final KafkaTargetConfig kafkaConfigBean;

  KafkaUDPConsumer udpConsumer;
  private UDPConsumingServer udpServer;
  private long counter;
  private BlockingQueue<Exception> errorQueue;
  private List<Exception> errorList;

  public UDPToKafkaSource(UDPConfigBean udpConfigs, KafkaTargetConfig kafkaConfigBean) {
    this.udpConfigs = udpConfigs;
    this.kafkaConfigBean = kafkaConfigBean;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    issues.addAll(udpConfigs.init(getContext()));
    kafkaConfigBean.init(getContext(), DataFormat.BINARY, false, issues);

    if (issues.isEmpty()) {
      errorQueue = new ArrayBlockingQueue<>(100);
      errorList = new ArrayList<>(100);

      if (!udpConfigs.getAddresses().isEmpty()) {
        udpConsumer = new KafkaUDPConsumer(getContext(), udpConfigs, kafkaConfigBean, errorQueue);
        udpConsumer.init();
        udpServer = new UDPConsumingServer(
            udpConfigs.enableEpoll,
            udpConfigs.acceptThreads,
            udpConfigs.getAddresses(),
            udpConsumer
        );
        try {
          udpServer.listen();
          udpServer.start();
        } catch (Exception ex) {
          udpServer.destroy();
          udpServer = null;

          if (ex instanceof SocketException && udpConfigs.isPrivilegedPortUsage()) {
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
                udpConfigs.getAddresses().toString(),
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
