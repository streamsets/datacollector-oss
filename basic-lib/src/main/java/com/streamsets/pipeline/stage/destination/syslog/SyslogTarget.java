/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.destination.syslog;

import com.cloudbees.syslog.Facility;
import com.cloudbees.syslog.Severity;
import com.cloudbees.syslog.SyslogMessage;
import com.cloudbees.syslog.sender.TcpSyslogMessageSender;
import com.cloudbees.syslog.sender.UdpSyslogMessageSender;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.RecordTarget;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.service.dataformats.DataFormatGeneratorService;
import com.streamsets.pipeline.api.service.dataformats.DataGenerator;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;

import java.io.ByteArrayOutputStream;
import java.util.Date;
import java.util.List;

public class SyslogTarget extends RecordTarget {
  // The Cloudbees Syslog library defaults to "\r\n" which adds a spurious control character to each message.
  private final static String tcpPostfix = "\n";

  private final SyslogTargetConfig config;

  private ELEval hostnameElEval;
  private ELEval appNameElEval;
  private ELEval timestampElEval;
  private ELEval facilityEval;
  private ELEval severityEval;
  private ELEval messageIdElEval;
  private ELEval processIdElEval;

  private ELVars elVars;

  private DataFormatGeneratorService generatorService = null;
  private ByteArrayOutputStream baos = null;

  private UdpSyslogMessageSender udpSender = null;
  private TcpSyslogMessageSender tcpSender = null;

  SyslogTarget(SyslogTargetConfig config) {
    this.config = config;
  }

  private static <T> T resolveEL(ELEval elEval, ELVars elVars, String configValue, Class<T> returnType) throws
      ELEvalException {
    return elEval.eval(elVars, configValue, returnType);
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    elVars = getContext().createELVars();

    generatorService = getContext().getService(DataFormatGeneratorService.class);
    baos = new ByteArrayOutputStream();

    hostnameElEval = getContext().createELEval("hostnameEL");
    appNameElEval = getContext().createELEval("appNameEL");
    timestampElEval = getContext().createELEval("timestampEL");
    facilityEval = getContext().createELEval("facilityEL");
    severityEval = getContext().createELEval("severityEL");
    messageIdElEval = getContext().createELEval("messageIdEL");
    processIdElEval = getContext().createELEval("processIdEL");

    if (config.protocol.equals("TCP")) {
      tcpSender = new TcpSyslogMessageSender();
      tcpSender.setSyslogServerHostname(config.serverName);
      tcpSender.setSyslogServerPort(config.serverPort);
      tcpSender.setSsl(config.useSsl);
      tcpSender.setSocketConnectTimeoutInMillis(config.timeout);
      tcpSender.setMaxRetryCount(config.retries);
      tcpSender.setPostfix(tcpPostfix);
    } else {
      udpSender = new UdpSyslogMessageSender();
      udpSender.setSyslogServerHostname(config.serverName);
      udpSender.setSyslogServerPort(config.serverPort);
      udpSender.setMessageFormat(config.messageFormat);
    }

    return issues;
  }

  @Override
  public void destroy() {
    super.destroy();
  }

  @Override
  protected void write(Record record) throws StageException {
    RecordEL.setRecordInContext(elVars, record);
    TimeNowEL.setTimeNowInContext(elVars, new Date());

    String messageVal;

    try {
      String hostname = resolveEL(hostnameElEval, elVars, config.hostnameEL, String.class);
      Date timestamp = resolveEL(timestampElEval, elVars, config.timestampEL, Date.class);
      String facilityStr = resolveEL(facilityEval, elVars, config.facilityEL, String.class);
      String severityStr = resolveEL(severityEval, elVars, config.severityEL, String.class);
      String appName = resolveEL(appNameElEval, elVars, config.appNameEL, String.class);
      String messageId = resolveEL(messageIdElEval, elVars, config.messageIdEL, String.class);
      String processId = resolveEL(processIdElEval, elVars, config.processIdEL, String.class);

      baos.reset();
      DataGenerator generator = generatorService.getGenerator(baos);
      generator.write(record);
      generator.close();

      messageVal = new String(baos.toByteArray());

      Facility facility = Facility.fromNumericalCode(Integer.parseInt(facilityStr));
      Severity severity = Severity.fromNumericalCode(Integer.parseInt(severityStr));

      SyslogMessage message = new SyslogMessage()
          .withMsg(messageVal)
          .withHostname(hostname)
          .withTimestamp(timestamp)
          .withFacility(facility)
          .withSeverity(severity);

      // optional header values
      if(!appName.equals("")) {
        message.setAppName(appName);
      }
      if(!messageId.equals("")) {
        message.setMsgId(messageId);
      }
      if(!processId.equals("")) {
        message.setProcId(processId);
      }

      if (config.protocol.equals("TCP")) {
        this.tcpSender.sendMessage(message);
      } else {
        this.udpSender.sendMessage(message);
      }
    } catch (Exception e) {
      throw new OnRecordErrorException(Errors.SYSLOG_01, e);
    }
  }
}
