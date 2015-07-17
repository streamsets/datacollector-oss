/*
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.udp;

import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.parser.AbstractParser;
import com.streamsets.pipeline.lib.parser.ParserConfig;
import com.streamsets.pipeline.lib.parser.collectd.CollectdParser;
import com.streamsets.pipeline.lib.parser.netflow.NetflowParser;
import com.streamsets.pipeline.lib.parser.syslog.SyslogParser;
import io.netty.channel.socket.DatagramPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.streamsets.pipeline.lib.parser.ParserConfigKey.AUTH_FILE_PATH;
import static com.streamsets.pipeline.lib.parser.ParserConfigKey.CHARSET;
import static com.streamsets.pipeline.lib.parser.ParserConfigKey.CONVERT_TIME;
import static com.streamsets.pipeline.lib.parser.ParserConfigKey.EXCLUDE_INTERVAL;
import static com.streamsets.pipeline.lib.parser.ParserConfigKey.TYPES_DB_PATH;


public class UDPSource extends BaseSource {
  private static final Logger LOG = LoggerFactory.getLogger(UDPSource.class);
  private static final boolean IS_TRACE_ENABLED = LOG.isTraceEnabled();
  private static final boolean IS_DEBUG_ENABLED = LOG.isDebugEnabled();
  private final Set<String> ports;
  private final int maxBatchSize;
  private final Queue<Record> overrunQueue;
  private final long maxWaitTime;
  private final List<InetSocketAddress> addresses;
  private final ParserConfig parserConfig;
  private final UDPDataFormat dataFormat;
  private long recordCount;
  private UDPConsumingServer udpServer;
  private AbstractParser parser;
  private BlockingQueue<DatagramPacket> incomingQueue;

  public UDPSource(
      List<String> ports,
      ParserConfig parserConfig,
      UDPDataFormat dataFormat,
      int maxBatchSize,
      long maxWaitTime
  ) {
    this.ports = ImmutableSet.copyOf(ports);
    this.parserConfig = parserConfig;
    this.dataFormat = dataFormat;
    this.maxBatchSize = maxBatchSize;
    this.maxWaitTime = maxWaitTime;
    this.overrunQueue = new LinkedList<>();
    this.addresses = new ArrayList<>();
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = new ArrayList<>();
    this.recordCount = 0;
    this.incomingQueue = new ArrayBlockingQueue<>(this.maxBatchSize * 10);
    if (ports.isEmpty()) {
      issues.add(getContext().createConfigIssue(Groups.UDP.name(), "ports",
        Errors.UDP_02));
    } else {
      for (String candidatePort : ports) {
        try {
          int port = Integer.parseInt(candidatePort.trim());
          if (port > 1023 && port < 65536) {
            addresses.add(new InetSocketAddress(port));
          } else {
            issues.add(getContext().createConfigIssue(Groups.UDP.name(), "ports",
              Errors.UDP_03, port));
          }
        } catch (NumberFormatException ex) {
          issues.add(getContext().createConfigIssue(Groups.UDP.name(), "ports",
            Errors.UDP_03, candidatePort));
        }
      }
    }
    Charset charset;
    try {
      charset = Charset.forName(parserConfig.getString(CHARSET));
    } catch (UnsupportedCharsetException ex) {
      charset = StandardCharsets.UTF_8;
      issues.add(getContext().createConfigIssue(Groups.SYSLOG.name(), "charset", Errors.UDP_04, charset));
    }
    switch (dataFormat) {
      case NETFLOW:
        parser = new NetflowParser(getContext());
        break;
      case SYSLOG:
        parser = new SyslogParser(getContext(), charset);
        break;
      case COLLECTD:
        parser = new CollectdParser(
            getContext(),
            parserConfig.getBoolean(CONVERT_TIME),
            parserConfig.getString(TYPES_DB_PATH),
            parserConfig.getBoolean(EXCLUDE_INTERVAL),
            parserConfig.getString(AUTH_FILE_PATH),
            charset
        );
        break;
      default:
        issues.add(getContext().createConfigIssue(Groups.UDP.name(), "dataFormat",
          Errors.UDP_01, dataFormat));
        break;
    }
    if (issues.isEmpty()) {
      if (!addresses.isEmpty()) {
        QueuingUDPConsumer udpConsumer = new QueuingUDPConsumer(incomingQueue);
        udpServer = new UDPConsumingServer(addresses, udpConsumer);
        try {
          udpServer.listen();
          udpServer.start();
        } catch (Exception ex) {
          udpServer.destroy();
          udpServer = null;
          issues.add(getContext().createConfigIssue(null, null, Errors.UDP_00, addresses.toString(), ex.toString(), ex));
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
    maxBatchSize = Math.min(this.maxBatchSize, maxBatchSize);
    final long startingRecordCount = recordCount;
    long remainingTime = maxWaitTime;
    for (int i = 0; i < maxBatchSize; i++) {
      if (overrunQueue.isEmpty()) {
        try {
          long start = System.currentTimeMillis();
          DatagramPacket packet = incomingQueue.poll(remainingTime, TimeUnit.MILLISECONDS);
          long elapsedTime = System.currentTimeMillis() - start;
          if (elapsedTime > 0) {
            remainingTime -= elapsedTime;
          }
          if (packet != null) {
            try {
              List<Record> records = parser.parse(packet.content(), packet.recipient(), packet.sender());
              if (IS_TRACE_ENABLED) {
                LOG.trace("Found {} records", records.size());
              }
              overrunQueue.addAll(records);
              packet.release();
            } catch (OnRecordErrorException ex) {
              switch (getContext().getOnErrorRecord()) {
                case DISCARD:
                  break;
                case TO_ERROR:
                  getContext().reportError(ex);
                  break;
                case STOP_PIPELINE:
                  throw ex;
                default:
                  throw new IllegalStateException(Utils.format("It should never happen. OnError '{}'",
                    getContext().getOnErrorRecord(), ex));
              }
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
