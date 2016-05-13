/**
 * Copyright 2015 StreamSets Inc.
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
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.SocketException;
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
  private ErrorRecordHandler errorRecordHandler;
  private BlockingQueue<ParseResult> incomingQueue;
  private boolean privilegedPortUsage = false;

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
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    this.recordCount = 0;
    this.incomingQueue = new ArrayBlockingQueue<>(this.maxBatchSize * 10);
    if (ports.isEmpty()) {
      issues.add(getContext().createConfigIssue(Groups.UDP.name(), "ports",
        Errors.UDP_02));
    } else {
      for (String candidatePort : ports) {
        try {
          int port = Integer.parseInt(candidatePort.trim());
          if (port > 0 && port < 65536) {
            if (port < 1024) {
              privilegedPortUsage = true; // only for error handling purposes
            }
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
    switch (dataFormat) {
      case NETFLOW:
        parser = new NetflowParser(getContext());
        break;
      case SYSLOG:
        charset = validateCharset(Groups.SYSLOG.name(), issues);
        parser = new SyslogParser(getContext(), charset);
        break;
      case COLLECTD:
        charset = validateCharset(Groups.COLLECTD.name(), issues);
        checkCollectdParserConfigs(issues);
        if (issues.size() == 0) {
          parser = new CollectdParser(
              getContext(),
              parserConfig.getBoolean(CONVERT_TIME),
              parserConfig.getString(TYPES_DB_PATH),
              parserConfig.getBoolean(EXCLUDE_INTERVAL),
              parserConfig.getString(AUTH_FILE_PATH),
              charset
          );
        }
        break;
      default:
        issues.add(getContext().createConfigIssue(Groups.UDP.name(), "dataFormat",
          Errors.UDP_01, dataFormat));
        break;
    }
    if (issues.isEmpty()) {
      if (!addresses.isEmpty()) {
        QueuingUDPConsumer udpConsumer = new QueuingUDPConsumer(parser, incomingQueue);
        udpServer = new UDPConsumingServer(addresses, udpConsumer);
        try {
          udpServer.listen();
          udpServer.start();
        } catch (Exception ex) {
          udpServer.destroy();
          udpServer = null;

          if (ex instanceof SocketException && privilegedPortUsage) {
            issues.add(getContext().createConfigIssue(Groups.UDP.name(), "ports", Errors.UDP_07, ports, ex));
          } else {
            LOG.debug("Caught exception while starting up UDP server: {}", ex);
            issues.add(getContext().createConfigIssue(null, null, Errors.UDP_00, addresses.toString(), ex.toString(), ex));
          }
        }
      }
    }
    return issues;
  }

  private Charset validateCharset(String groupName, List<ConfigIssue> issues) {
    Charset charset;
    try {
      charset = Charset.forName(parserConfig.getString(CHARSET));
    } catch (UnsupportedCharsetException ex) {
      charset = StandardCharsets.UTF_8;
      issues.add(getContext().createConfigIssue(groupName, "charset", Errors.UDP_04, charset));
    }
    return charset;
  }

  private void checkCollectdParserConfigs(List<ConfigIssue> issues) {
    String typesDbLocation = parserConfig.getString(TYPES_DB_PATH);
    if (!typesDbLocation.isEmpty()) {
      File typesDbFile = new File(typesDbLocation);
      if (!typesDbFile.canRead() || !typesDbFile.isFile()) {
        issues.add(
            getContext().createConfigIssue(Groups.COLLECTD.name(), "typesDbPath", Errors.UDP_05, typesDbLocation)
        );
      }
    }
    String authFileLocation = parserConfig.getString(AUTH_FILE_PATH);
    if (!authFileLocation.isEmpty()) {
      File authFile = new File(authFileLocation);
      if (!authFile.canRead() || !authFile.isFile()) {
        issues.add(
            getContext().createConfigIssue(Groups.COLLECTD.name(), "authFilePath", Errors.UDP_06, authFileLocation)
        );
      }
    }
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
