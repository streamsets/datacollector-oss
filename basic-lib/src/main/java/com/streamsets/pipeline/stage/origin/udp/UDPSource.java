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
package com.streamsets.pipeline.stage.origin.udp;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.udp.UDPConsumingServer;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class UDPSource extends BaseSource {
  private static final Logger LOG = LoggerFactory.getLogger(UDPSource.class);
  private static final boolean IS_TRACE_ENABLED = LOG.isTraceEnabled();
  private static final boolean IS_DEBUG_ENABLED = LOG.isDebugEnabled();
  private final Queue<Record> overrunQueue;
  private final UDPSourceConfigBean conf;
  private long recordCount;
  private UDPConsumingServer udpServer;
  private ErrorRecordHandler errorRecordHandler;
  private BlockingQueue<ParseResult> incomingQueue;
  private boolean checkBatchSize = true;

  public UDPSource(UDPSourceConfigBean conf) {
    this.conf = conf;
    this.overrunQueue = new LinkedList<>();
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = new ArrayList<>();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    this.recordCount = 0;
    this.incomingQueue = new ArrayBlockingQueue<>(conf.batchSize * 10);

    final boolean valid = conf.init(getContext(), issues);

    if (valid && issues.isEmpty()) {
      final List<InetSocketAddress> addresses = conf.getAddresses();
      if (!addresses.isEmpty()) {
        QueuingUDPConsumer udpConsumer = new QueuingUDPConsumer(conf.getParser(), incomingQueue);
        udpServer = new UDPConsumingServer(conf.enableEpoll, conf.numThreads, addresses, udpConsumer);
        try {
          udpServer.listen();
          udpServer.start();
        } catch (Exception ex) {
          udpServer.destroy();
          udpServer = null;

          if (ex instanceof SocketException && conf.isPrivilegedPortUsage()) {
            issues.add(getContext().createConfigIssue(Groups.UDP.name(), "ports", Errors.UDP_07, conf.ports, ex));
          } else {
            LOG.debug("Caught exception while starting up UDP server: {}", ex);
            issues.add(getContext().createConfigIssue(Groups.UDP.name(), null, Errors.UDP_00, addresses.toString(), ex.toString(), ex));
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
    super.destroy();
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    Utils.checkNotNull(udpServer, "UDP server is null");
    Utils.checkNotNull(incomingQueue, "Incoming queue is null");

    maxBatchSize = Math.min(conf.batchSize, maxBatchSize);
    if (!getContext().isPreview() && checkBatchSize && conf.batchSize > maxBatchSize) {
      getContext().reportError(Errors.UDP_09, maxBatchSize);
      checkBatchSize = false;
    }

    final long startingRecordCount = recordCount;
    long remainingTime = conf.maxWaitTime;
    for (int i = 0; i < maxBatchSize; i++) {
      if (overrunQueue.isEmpty()) {
        try {
          long start = System.currentTimeMillis();
          ParseResult result = incomingQueue.poll(remainingTime, TimeUnit.MILLISECONDS);
          long elapsedTime = System.currentTimeMillis() - start;
          if (elapsedTime > 0) {
            remainingTime -= elapsedTime;
          }
          if (result != null) {
            try {
              List<Record> records = result.getRecords();
              if (IS_TRACE_ENABLED) {
                LOG.trace("Found {} records", records.size());
              }
              overrunQueue.addAll(records);
            } catch (OnRecordErrorException ex) {
              errorRecordHandler.onError(ex.getErrorCode(), ex.getParams());
            }
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      Record record = overrunQueue.poll();
      if (record != null) {
        recordCount++;
        batchMaker.addRecord(record);
      }
      if (remainingTime <= 0) {
        break;
      }
    }
    if (IS_DEBUG_ENABLED) {
      LOG.debug("Processed {} records", (recordCount - startingRecordCount));
    }
    return getOffset();
  }

  private String getOffset() {
    return Long.toString(recordCount);
  }
}
