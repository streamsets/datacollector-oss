/*
 * Copyright 2017 StreamSets Inc.
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

package com.streamsets.pipeline.stage.origin.tcp;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BasePushSource;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.net.DataFormatParserDecoder;
import com.streamsets.pipeline.lib.parser.net.DelimitedLengthFieldBasedFrameDecoder;
import com.streamsets.pipeline.lib.parser.net.netflow.NetflowDecoder;
import com.streamsets.pipeline.lib.parser.net.syslog.SyslogDecoder;
import com.streamsets.pipeline.lib.parser.net.syslog.SyslogFramingMode;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.ssl.SslHandler;
import org.apache.commons.lang3.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


public class TCPServerSource extends BasePushSource {
  private static final Logger LOG = LoggerFactory.getLogger(TCPServerSource.class);

  private final List<InetSocketAddress> addresses = new LinkedList<>();
  private final String recordSeparatorStr;
  private TCPConsumingServer tcpServer;
  private boolean privilegedPortUsage;
  private DataParserFactory parserFactory;

  private final TCPServerSourceConfig config;

  private final Map<String, OnRecordErrorException> pipelineIdsToFail = new HashMap<>();

  private static final long PRODUCE_LOOP_INTERVAL_MS = 1000;

  public TCPServerSource(TCPServerSourceConfig config) {
    this.config = config;

    this.recordSeparatorStr = config.tcpMode == TCPMode.SYSLOG
        ? config.nonTransparentFramingSeparatorCharStr
        : config.recordSeparatorStr;


    this.config.tlsConfigBean.hasKeyStore = true;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = new ArrayList<>();

    if (config.enableEpoll && !Epoll.isAvailable()) {
      issues.add(getContext().createConfigIssue(Groups.TCP.name(), "enableEpoll", Errors.TCP_08));
    }
    final String portsField = "ports";
    if (config.ports.isEmpty()) {
      issues.add(getContext().createConfigIssue(Groups.TCP.name(), portsField, Errors.TCP_02));
    } else {
      for (String candidatePort : config.ports) {
        try {
          int port = Integer.parseInt(candidatePort.trim());
          if (port > 0 && port < 65536) {
            if (port < 1024) {
              privilegedPortUsage = true; // only for error handling purposes
            }
            addresses.add(new InetSocketAddress(port));
          } else {
            issues.add(getContext().createConfigIssue(Groups.TCP.name(), portsField, Errors.TCP_03, port));
          }
        } catch (NumberFormatException ex) {
          issues.add(getContext().createConfigIssue(Groups.TCP.name(), portsField, Errors.TCP_03, candidatePort));
        }
      }
    }

    if (issues.isEmpty()) {
      if (addresses.isEmpty()) {
        issues.add(getContext().createConfigIssue(Groups.TCP.name(), portsField, Errors.TCP_12));
      } else {
        if (config.tlsEnabled) {
          boolean tlsValid = config.tlsConfigBean.init(getContext(), Groups.TLS.name(), "conf.tlsConfigBean.", issues);
          if (!tlsValid) {
            return issues;
          }
        }

        tcpServer = new TCPConsumingServer(
            config.enableEpoll,
            config.numThreads,
            addresses,
            new ChannelInitializer<SocketChannel>() {
              @Override
              public void initChannel(SocketChannel ch) throws Exception {
                if (config.tlsEnabled) {
                  // Add TLS handler into pipeline in the first position
                  ch.pipeline().addFirst("TLS", new SslHandler(config.tlsConfigBean.getSslEngine()));
                }

                ch.pipeline().addLast(
                    // first, decode the ByteBuf into some POJO type extending MessageToRecord
                    buildByteBufToMessageDecoderChain(issues).toArray(new ChannelHandler[0])
                );
                ch.pipeline().addLast(
                    // next, handle MessageToRecord instances to build SDC records and errors
                    new TCPObjectToRecordHandler(
                        getContext(),
                        config.batchSize,
                        config.maxWaitTime,
                        pipelineIdsToFail::put
                    )
                );
              }
            }
        );
        if (issues.isEmpty()) {
          try {
            tcpServer.listen();
            tcpServer.start();
          } catch (Exception ex) {
            tcpServer.destroy();
            tcpServer = null;

            if (ex instanceof SocketException && privilegedPortUsage) {
              issues.add(getContext().createConfigIssue(
                  Groups.TCP.name(),
                  portsField,
                  Errors.TCP_07,
                  config.ports,
                  ex
              ));
            } else {
              LOG.debug("Caught exception while starting up TCP server: {}", ex);
              issues.add(getContext().createConfigIssue(null,
                  null,
                  Errors.TCP_00,
                  addresses.toString(),
                  ex.toString(),
                  ex
              ));
            }
          }
        }
      }
    }
    return issues;
  }

  @VisibleForTesting
  protected List<ChannelHandler> buildByteBufToMessageDecoderChain(List<ConfigIssue> issues) {
    List<ChannelHandler> decoderChain = new LinkedList<>();

    final Charset charsetObj = Charset.forName(config.syslogCharset);
    switch (config.tcpMode) {
      case NETFLOW:
        decoderChain.add(new NetflowDecoder());
        break;
      case SYSLOG:
        if (config.syslogFramingMode == SyslogFramingMode.OCTET_COUNTING) {
          // first, a DelimitedLengthFieldBasedFrameDecoder to ensure we can capture a full message
          decoderChain.add(new DelimitedLengthFieldBasedFrameDecoder(config.maxMessageSize,
              0,
              false,
              Unpooled.copiedBuffer(" ", charsetObj),
              charsetObj,
              true
          ));
          // next, decode the syslog message itself
          decoderChain.add(new SyslogDecoder(charsetObj));
        } else if (config.syslogFramingMode == SyslogFramingMode.NON_TRANSPARENT_FRAMING) {
          // first, a DelimiterBasedFrameDecoder to ensure we can capture a full message
          decoderChain.add(new DelimiterBasedFrameDecoder(
              config.maxMessageSize,
              true,
              Unpooled.copiedBuffer(StringEscapeUtils.unescapeJava(recordSeparatorStr).getBytes())
          ));
          // next, decode the syslog message itself
          decoderChain.add(new SyslogDecoder(charsetObj));
        } else {
          throw new IllegalStateException("Unrecognized SyslogFramingMode: " + config.syslogFramingMode.name());
        }
        break;
      case DELIMITED_RECORDS:
        if (issues.isEmpty()) {
          config.dataFormatConfig.init(
              getContext(),
              config.dataFormat,
              Groups.TCP.name(),
              "dataFormatConfig",
              config.maxMessageSize,
              issues
          );
          parserFactory = config.dataFormatConfig.getParserFactory();

          // first, a DelimiterBasedFrameDecoder to ensure we can capture a full message
          decoderChain.add(new DelimiterBasedFrameDecoder(
              config.maxMessageSize,
              true,
              Unpooled.copiedBuffer(StringEscapeUtils.unescapeJava(recordSeparatorStr).getBytes()
          )));
          // next, decode the delimited message itself
          decoderChain.add(new DataFormatParserDecoder(parserFactory, getContext()));
        }
        break;
      default:
        issues.add(getContext().createConfigIssue(Groups.TCP.name(), "tcpMode", Errors.TCP_01, config.tcpMode));
        break;
    }
    return decoderChain;
  }

  @Override
  public void destroy() {
    if (tcpServer != null) {
      tcpServer.destroy();
    }
    tcpServer = null;
    super.destroy();
  }

  @Override
  public void produce(Map<String, String> lastOffsets, int maxBatchSize) throws StageException {
    while (!getContext().isStopped()) {
      stopPipelines();
      ThreadUtil.sleep(PRODUCE_LOOP_INTERVAL_MS);
    }
  }

  private void stopPipelines() throws StageException {
    for (Map.Entry<String, OnRecordErrorException> pipelineIdToError : pipelineIdsToFail.entrySet()) {
      final String pipelineId = pipelineIdToError.getKey();
      if (!pipelineId.equals(getContext().getPipelineId())) {
        LOG.error(
            "Unexpected pipeline ID {} requested stopped by a TCP server running in pipeline ID {}",
            pipelineId,
            getContext().getPipelineId()
        );
      } else {
        OnRecordErrorException error = pipelineIdToError.getValue();
        throw new StageException(Errors.TCP_09, error.getMessage(), error);
      }
    }
  }

  @Override
  public int getNumberOfThreads() {
    return config.numThreads;
  }

}
