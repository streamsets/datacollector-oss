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
package com.streamsets.pipeline.stage.origin.tcp;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BasePushSource;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.ELUtils;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.net.DataFormatParserDecoder;
import com.streamsets.pipeline.lib.parser.net.DelimitedLengthFieldBasedFrameDecoder;
import com.streamsets.pipeline.lib.parser.net.netflow.NetflowCommonDecoder;
import com.streamsets.pipeline.lib.parser.net.netflow.NetflowDataParserFactory;
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
import java.nio.charset.UnsupportedCharsetException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;


public class TCPServerSource extends BasePushSource {
  private static final Logger LOG = LoggerFactory.getLogger(TCPServerSource.class);
  public static final String RECORD_PROCESSED_EL_NAME = "recordProcessedAckMessage";
  public static final String BATCH_COMPLETED_EL_NAME = "batchCompletedAckMessage";

  private static final String CONF_PREFIX = "conf.";

  private final List<InetSocketAddress> addresses = new LinkedList<>();

  private TCPConsumingServer tcpServer;
  private boolean privilegedPortUsage;
  private DataParserFactory parserFactory;

  private final TCPServerSourceConfig config;

  private final Map<String, StageException> pipelineIdsToFail = new HashMap<>();

  private static final long PRODUCE_LOOP_INTERVAL_MS = 1000;

  public TCPServerSource(TCPServerSourceConfig config) {
    this.config = config;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = new ArrayList<>();

    if (config.enableEpoll && !Epoll.isAvailable()) {
      issues.add(getContext().createConfigIssue(Groups.TCP.name(), CONF_PREFIX + "enableEpoll", Errors.TCP_05));
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
        issues.add(getContext().createConfigIssue(Groups.TCP.name(), portsField, Errors.TCP_09));
      } else {
        if (config.tlsConfigBean.isEnabled()) {
          boolean tlsValid = config.tlsConfigBean.init(
              getContext(),
              Groups.TLS.name(),
              CONF_PREFIX + "tlsConfigBean.",
              issues
          );
          if (!tlsValid) {
            return issues;
          }
        }

        final boolean elValid = validateEls(issues);
        if (!elValid) {
          return issues;
        }

        validateTcpConfigs(issues);
        if (issues.size() > 0) {
          return issues;
        }

        tcpServer = new TCPConsumingServer(
            config.enableEpoll,
            config.numThreads,
            addresses,
            new ChannelInitializer<SocketChannel>() {
              @Override
              public void initChannel(SocketChannel ch) throws Exception {
                if (config.tlsConfigBean.isEnabled()) {
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
                        pipelineIdsToFail::put,
                        getContext().createELEval(RECORD_PROCESSED_EL_NAME),
                        getContext().createELVars(),
                        config.recordProcessedAckMessage,
                        getContext().createELEval(BATCH_COMPLETED_EL_NAME),
                        getContext().createELVars(),
                        config.batchCompletedAckMessage,
                        config.timeZoneID,
                        Charset.forName(config.ackMessageCharset)
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
                  Errors.TCP_04,
                  config.ports,
                  ex
              ));
            } else {
              LOG.debug("Caught exception while starting up TCP server: {}", ex);
              issues.add(getContext().createConfigIssue(
                  null,
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

  private boolean validateEls(List<ConfigIssue> issues) {

    final int numStartingIssues = issues.size();

    if (!Strings.isNullOrEmpty(config.recordProcessedAckMessage)) {
      final ELEval eval = getContext().createELEval("recordProcessedAckMessage");

      final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(ZoneId.of(config.timeZoneID)));
      TimeEL.setCalendarInContext(getContext().createELVars(), calendar);
      final ELVars vars = getContext().createELVars();
      Record validationRecord = getContext().createRecord("recordProcessedAckMessageValidationRecord");
      RecordEL.setRecordInContext(vars, validationRecord);

      ELUtils.validateExpression(
          eval,
          vars,
          config.recordProcessedAckMessage,
          getContext(),
          Groups.TCP.name(),
          CONF_PREFIX + "recordProcessedAckMessage",
          Errors.TCP_30,
          String.class,
          issues
      );
    }

    if (!Strings.isNullOrEmpty(config.batchCompletedAckMessage)) {
      final ELEval eval = getContext().createELEval("batchCompletedAckMessage");

      final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(ZoneId.of(config.timeZoneID)));
      TimeEL.setCalendarInContext(getContext().createELVars(), calendar);
      final ELVars vars = getContext().createELVars();
      vars.addVariable("batchSize", 0);
      Record validationRecord = getContext().createRecord("batchCompletedAckMessageValidationRecord");
      RecordEL.setRecordInContext(vars, validationRecord);

      ELUtils.validateExpression(
          eval,
          vars,
          config.batchCompletedAckMessage,
          getContext(),
          Groups.TCP.name(),
          CONF_PREFIX + "batchCompletedAckMessage",
          Errors.TCP_31,
          String.class,
          issues
      );
    }

    return issues.size() == numStartingIssues;
  }

  @VisibleForTesting
  void validateTcpConfigs(List<ConfigIssue> issues) {

    switch (config.tcpMode) {
      case NETFLOW:
        NetflowDataParserFactory.validateConfigs(
            getContext(),
            issues,
            Groups.NETFLOW_V9.name(),
            "conf.",
            config.maxTemplateCacheSize,
            config.templateCacheTimeoutMs
        );
        break;
      case SYSLOG:
        try {
          Charset.forName(config.syslogCharset);
        } catch (UnsupportedCharsetException e) {
          issues.add(getContext().createConfigIssue(
              Groups.TCP.name(),
              CONF_PREFIX + "syslogCharset",
              Errors.TCP_10,
              config.syslogCharset
          ));
        }
        if (config.syslogFramingMode == SyslogFramingMode.OCTET_COUNTING) {
          // nothing to validate
        } else if (config.syslogFramingMode == SyslogFramingMode.NON_TRANSPARENT_FRAMING) {
          validateDelimiterBasedFrameDecoder(
              issues,
              config.nonTransparentFramingSeparatorCharStr,
              "nonTransparentFramingSeparatorCharStr"
          );
        } else {
          issues.add(getContext().createConfigIssue(
              Groups.TCP.name(),
              CONF_PREFIX + "syslogFramingMode",
              Errors.TCP_20,
              config.syslogFramingMode.name()
          ));
        }
        break;
      case DELIMITED_RECORDS:
        validateDelimiterBasedFrameDecoder(
            issues,
            config.recordSeparatorStr,
            "recordSeparatorStr"
        );
        // SDC data format (text, json, etc.) separated by some configured sequence of bytes
        if (issues.isEmpty()) {
          config.dataFormatConfig.init(
              getContext(),
              config.dataFormat,
              Groups.TCP.name(),
              CONF_PREFIX + "dataFormatConfig",
              config.maxMessageSize,
              issues
          );
        }
        break;
      case CHARACTER_BASED_LENGTH_FIELD:
        try {
          Charset.forName(config.lengthFieldCharset);
        } catch (UnsupportedCharsetException e) {
          issues.add(getContext().createConfigIssue(
              Groups.TCP.name(),
              CONF_PREFIX + "lengthFieldCharset",
              Errors.TCP_10,
              config.lengthFieldCharset
          ));
        }
        // SDC data format (text, json, etc.) separated by some configured sequence of bytes
        if (issues.isEmpty()) {
          config.dataFormatConfig.init(
              getContext(),
              config.dataFormat,
              Groups.TCP.name(),
              CONF_PREFIX + "dataFormatConfig",
              config.maxMessageSize,
              issues
          );
        }
        break;
      default:
        issues.add(getContext().createConfigIssue(
            Groups.TCP.name(),
            CONF_PREFIX + "tcpMode",
            Errors.TCP_01,
            config.tcpMode
        ));
        break;
    }
  }

  @VisibleForTesting
  List<ChannelHandler> buildByteBufToMessageDecoderChain(List<ConfigIssue> issues) {
    List<ChannelHandler> decoderChain = new LinkedList<>();

    switch (config.tcpMode) {
      case NETFLOW:
        decoderChain.add(new NetflowCommonDecoder(
            config.netflowOutputValuesMode,
            config.maxTemplateCacheSize,
            config.templateCacheTimeoutMs
        ));
        break;
      case SYSLOG:
        final Charset syslogCharset = Charset.forName(config.syslogCharset);
        if (config.syslogFramingMode == SyslogFramingMode.OCTET_COUNTING) {
          // first, a DelimitedLengthFieldBasedFrameDecoder to ensure we can capture a full message
          decoderChain.add(buildDelimitedLengthFieldBasedFrameDecoder(syslogCharset));
          // next, decode the syslog message itself
          decoderChain.add(new SyslogDecoder(syslogCharset));
        } else if (config.syslogFramingMode == SyslogFramingMode.NON_TRANSPARENT_FRAMING) {
          // first, a DelimiterBasedFrameDecoder to ensure we can capture a full message
          decoderChain.add(buildDelimiterBasedFrameDecoder(config.nonTransparentFramingSeparatorCharStr));
          // next, decode the syslog message itself
          decoderChain.add(new SyslogDecoder(syslogCharset));
        } else {
          throw new IllegalStateException("Unrecognized SyslogFramingMode: " + config.syslogFramingMode.name());
        }
        break;
      case DELIMITED_RECORDS:
        // SDC data format (text, json, etc.) separated by some configured sequence of bytes
        config.dataFormatConfig.init(
            getContext(),
            config.dataFormat,
            Groups.TCP.name(),
            CONF_PREFIX + "dataFormatConfig",
            config.maxMessageSize,
            issues
        );
        parserFactory = config.dataFormatConfig.getParserFactory();

        // first, a DelimiterBasedFrameDecoder to ensure we can capture a full message
        decoderChain.add(buildDelimiterBasedFrameDecoder(config.recordSeparatorStr));
        // next, decode the delimited message itself
        decoderChain.add(new DataFormatParserDecoder(parserFactory, getContext()));
        break;
      case CHARACTER_BASED_LENGTH_FIELD:
        final Charset lengthFieldCharset = Charset.forName(config.lengthFieldCharset);
        // first, a DelimitedLengthFieldBasedFrameDecoder to ensure we can capture a full message
        decoderChain.add(buildDelimitedLengthFieldBasedFrameDecoder(lengthFieldCharset));
        // next, decode the length field framed message itself
        config.dataFormatConfig.init(
            getContext(),
            config.dataFormat,
            Groups.TCP.name(),
            CONF_PREFIX + "dataFormatConfig",
            config.maxMessageSize,
            issues
        );
        parserFactory = config.dataFormatConfig.getParserFactory();
        decoderChain.add(new DataFormatParserDecoder(parserFactory, getContext()));
        break;
      default:
        issues.add(getContext().createConfigIssue(Groups.TCP.name(), "conf.tcpMode", Errors.TCP_01, config.tcpMode));
        break;
    }
    return decoderChain;
  }

  private void validateDelimiterBasedFrameDecoder(
      List<ConfigIssue> issues,
      String recordSeparatorStr,
      String recordSeparatorStrField
  ) {
    if (recordSeparatorStr == null) {
      issues.add(getContext().createConfigIssue(
          Groups.TCP.name(),
          CONF_PREFIX + recordSeparatorStrField,
          Errors.TCP_41
      ));
      return;
    }
    final byte[] delimiterBytes = StringEscapeUtils.unescapeJava(recordSeparatorStr).getBytes();
    if (delimiterBytes.length == 0) {
      issues.add(getContext().createConfigIssue(
          Groups.TCP.name(),
          CONF_PREFIX + recordSeparatorStrField,
          Errors.TCP_40
      ));
    }
  }

  private DelimiterBasedFrameDecoder buildDelimiterBasedFrameDecoder(String recordSeparatorStr) {
    final byte[] delimiterBytes = StringEscapeUtils.unescapeJava(recordSeparatorStr).getBytes();
    return new DelimiterBasedFrameDecoder(
        config.maxMessageSize,
        true,
        Unpooled.copiedBuffer(delimiterBytes)
    );
  }

  private DelimitedLengthFieldBasedFrameDecoder buildDelimitedLengthFieldBasedFrameDecoder(Charset charset) {
    return new DelimitedLengthFieldBasedFrameDecoder(config.maxMessageSize,
        0,
        false,
        // length field characters are separated from the rest of the data by a space
        Unpooled.copiedBuffer(" ", charset),
        charset,
        true
    );
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
    for (Map.Entry<String, StageException> pipelineIdToError : pipelineIdsToFail.entrySet()) {
      final String pipelineId = pipelineIdToError.getKey();
      if (!pipelineId.equals(getContext().getPipelineId())) {
        LOG.error(
            "Unexpected pipeline ID {} requested stopped by a TCP server running in pipeline ID {}",
            pipelineId,
            getContext().getPipelineId()
        );
      } else {
        Exception error = pipelineIdToError.getValue();
        throw new StageException(Errors.TCP_06, error.getMessage(), error);
      }
    }
  }

  @Override
  public int getNumberOfThreads() {
    return config.numThreads;
  }

}
