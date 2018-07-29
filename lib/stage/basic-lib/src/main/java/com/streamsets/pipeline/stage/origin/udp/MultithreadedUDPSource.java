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

import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BasePushSource;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.lib.parser.udp.AbstractParser;
import com.streamsets.pipeline.lib.udp.PacketQueueUDPHandler;
import com.streamsets.pipeline.lib.udp.UDPConsumingServer;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import io.netty.channel.socket.DatagramPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;


public class MultithreadedUDPSource extends BasePushSource {
  private static final Logger LOG = LoggerFactory.getLogger(MultithreadedUDPSource.class);
  private static final boolean IS_TRACE_ENABLED = LOG.isTraceEnabled();
  private static final boolean IS_DEBUG_ENABLED = LOG.isDebugEnabled();
  public static final String PACKET_QUEUE_GAUGE_NAME = "Packet Queue";

  private final SafeScheduledExecutorService executorService;
  private long recordCount;
  private UDPConsumingServer udpServer;
  private AbstractParser parser;

  private PacketQueueUDPHandler handler;

  private final UDPSourceConfigBean configs;
  private final int packetQueueSize;
  private final int numWorkerThreads;

  public MultithreadedUDPSource(
      UDPSourceConfigBean configs,
      int packetQueueSize,
      int numWorkerThreads
  ) {
    this.configs = configs;
    this.packetQueueSize = packetQueueSize;
    this.numWorkerThreads = numWorkerThreads;

    executorService = new SafeScheduledExecutorService(this.numWorkerThreads, "UDP_Source_Worker_");

  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = new ArrayList<>();

    boolean valid = configs.init(getContext(), issues);
    this.recordCount = 0;
    if (valid && issues.isEmpty()) {
      parser = configs.getParser();
      final List<InetSocketAddress> addresses = configs.getAddresses();
      if (!addresses.isEmpty()) {
        final Map<String, Object> gaugeMap = getContext().createGauge(PACKET_QUEUE_GAUGE_NAME).getValue();
        handler = new PacketQueueUDPHandler(gaugeMap, packetQueueSize);
        udpServer = new UDPConsumingServer(configs.enableEpoll, configs.numThreads, addresses, handler);
        try {
          udpServer.listen();
          udpServer.start();
        } catch (Exception ex) {
          udpServer.destroy();
          udpServer = null;

          if (ex instanceof SocketException && configs.isPrivilegedPortUsage()) {
            issues.add(getContext().createConfigIssue(Groups.UDP.name(), "ports", Errors.UDP_07, configs.ports, ex));
          } else {
            LOG.debug("Caught exception while starting up UDP server: {}", ex);
            issues.add(getContext().createConfigIssue(
                Groups.UDP.name(),
                null,
                Errors.UDP_00,
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
    super.destroy();
  }

  private String getOffset() {
    return Long.toString(recordCount);
  }

  @Override
  public int getNumberOfThreads() {
    return numWorkerThreads;
  }

  @Override
  public void produce(Map<String, String> offsets, int maxBatchSize) throws StageException {
    Utils.checkNotNull(udpServer, "UDP server is null");

    final int finalMaxBatchSize = Math.min(configs.batchSize, maxBatchSize);

    try {
      ExecutorCompletionService<Future> completionService = new ExecutorCompletionService<>(executorService);

      List<Future> allFutures = new LinkedList<>();
      IntStream.range(0, numWorkerThreads).forEach(threadNumber -> {
        Runnable runnable = new Runnable() {
          @Override
          public void run() {
            BatchContext batchContext = null;
            long remainingTime = configs.maxWaitTime;
            while (!getContext().isStopped()) {
              if (batchContext == null) {
                batchContext = getContext().startBatch();
              }

              final long startingRecordCount = recordCount;
              try {
                long start = System.currentTimeMillis();
                //ParseResult result = incomingQueue.poll(remainingTime, TimeUnit.MILLISECONDS);

                final DatagramPacket packet = handler.getPacketQueue().poll(remainingTime, TimeUnit.MILLISECONDS);
                List<Record> records = null;
                if (packet != null) {
                  if (LOG.isTraceEnabled()) {
                    LOG.trace("Took packet; new size: {}", handler.getPacketQueue().size());
                  }

                  try {
                    records = parser.parse(packet.content(), packet.recipient(), packet.sender());
                  } catch (OnRecordErrorException ex) {
                    getContext().reportError(ex.getErrorCode(), ex.getParams());
                  } catch (Exception e) {
                    getContext().reportError(e);
                    continue;
                  } finally {
                    packet.release();
                  }
                }
                long elapsedTime = System.currentTimeMillis() - start;
                if (elapsedTime > 0) {
                  remainingTime -= elapsedTime;
                }

                if (records != null) {
                  if (IS_TRACE_ENABLED) {
                    LOG.trace("Found {} records", records.size());
                  }
                  for (Record record : records) {
                    if (IS_TRACE_ENABLED) {
                      LOG.trace("Processed {} records", (recordCount - startingRecordCount));
                    }

                    batchContext.getBatchMaker().addRecord(record);

                    if (++recordCount % finalMaxBatchSize == 0) {
                      getContext().processBatch(batchContext);
                      batchContext = getContext().startBatch();
                    }
                  }
                }

                if (remainingTime <= 0) {
                  remainingTime = configs.maxWaitTime;
                  getContext().processBatch(batchContext);
                  batchContext = getContext().startBatch();
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            }
          }
        };
        allFutures.add(completionService.submit(runnable, null));
      });

      while (!getContext().isStopped()) {
        ThreadUtil.sleep(101);
      }

      for (Future future : allFutures) {
        try {
          future.get();
        } catch (ExecutionException e) {
          LOG.error(
              "ExecutionException when attempting to wait for all UDP runnables to complete, after context was" +
                  " stopped: {}",
              e.getMessage(),
              e
          );
        } catch (InterruptedException e) {
          LOG.error(
              "InterruptedException when attempting to wait for all UDP runnables to complete, after context " +
                  "was stopped: {}",
              e.getMessage(),
              e
          );
          Thread.currentThread().interrupt();
        }
      }
    } finally {
      if (!executorService.isShutdown()) {
        executorService.shutdown();
      }
    }

  }
}
