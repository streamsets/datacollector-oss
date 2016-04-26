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
package com.streamsets.datacollector.execution.alerts;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.streamsets.datacollector.alerts.AlertsUtil;
import com.streamsets.datacollector.config.DataRuleDefinition;
import com.streamsets.datacollector.config.RuleDefinition;
import com.streamsets.datacollector.email.EmailException;
import com.streamsets.datacollector.email.EmailSender;
import com.streamsets.datacollector.execution.EventListenerManager;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.util.PipelineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;

public class AlertManager {

  private static Logger LOG = LoggerFactory.getLogger(AlertManager.class);

  private final String pipelineName;
  private final String revision;
  private final EmailSender emailSender;
  private final MetricRegistry metrics;
  private final RuntimeInfo runtimeInfo;
  private final EventListenerManager eventListenerManager;

  public AlertManager(@Named("name") String pipelineName,  @Named("rev") String revision, EmailSender emailSender,
                      MetricRegistry metrics, RuntimeInfo runtimeInfo, EventListenerManager eventListenerManager) {
    this.pipelineName = pipelineName;
    this.revision = revision;
    this.emailSender = emailSender;
    this.metrics = metrics;
    this.runtimeInfo = runtimeInfo;
    this.eventListenerManager = eventListenerManager;
  }

  public void alert(List<String> emailIds, Throwable throwable) {
    StringWriter stringWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter(stringWriter);
    throwable.printStackTrace(printWriter);
    String description = stringWriter.toString();
    String subject = "ERROR: " + throwable;
    long timestamp = System.currentTimeMillis();
    String errorCode = "UNKNOWN";
    if (throwable instanceof PipelineException) {
      PipelineException pipelineException = (PipelineException)throwable;
      timestamp = pipelineException.getErrorMessage().getTimestamp();
      subject =  "ERROR: " + pipelineException.getLocalizedMessage();
      errorCode = pipelineException.getErrorCode().getCode();
    }
    try {
      URL url = Resources.getResource(EmailConstants.ALERT_ERROR_EMAIL_TEMPLATE);
      String emailBody = Resources.toString(url, Charsets.UTF_8);
      java.text.DateFormat dateTimeFormat = new SimpleDateFormat(EmailConstants.DATE_MASK, Locale.ENGLISH);
      emailBody = emailBody.replace(EmailConstants.ERROR_CODE, errorCode)
        .replace(EmailConstants.TIME_KEY, dateTimeFormat.format(new Date(timestamp)))
        .replace(EmailConstants.PIPELINE_NAME_KEY, pipelineName)
        .replace(EmailConstants.DESCRIPTION_KEY, description)
        .replace(EmailConstants.URL_KEY, runtimeInfo.getBaseHttpUrl() + EmailConstants.PIPELINE_URL + pipelineName);
      subject = EmailConstants.STREAMSETS_DATA_COLLECTOR_ALERT + subject;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Email Alert: subject = " + subject + ", body = " + emailBody);
      }
      if(emailSender == null) {
        LOG.error("Email Sender is not configured. Alert with message '{}' will not be sent via email:",
          emailBody, throwable);
      } else {
        emailSender.send(emailIds, subject, emailBody);
      }
    } catch (EmailException | IOException e) {
      LOG.error("Error sending alert email, reason: {}", e.toString(), e);
      //Log error and move on. This should not stop the pipeline.
    }
  }

  public void alert(Object value, List<String> emailIds, RuleDefinition ruleDefinition) {
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics, AlertsUtil.getAlertGaugeName(ruleDefinition.getId()));
    if (gauge == null) {
      Gauge<Object> alertResponseGauge = AlertManagerHelper.createAlertResponseGauge(
          pipelineName,
          revision,
          metrics,
          value,
          ruleDefinition
      );

      eventListenerManager.broadcastAlerts(new AlertInfo(pipelineName, ruleDefinition, alertResponseGauge));

      //send email the first time alert is triggered
      if(ruleDefinition.isSendEmail()) {
        try {
          URL url = Resources.getResource(EmailConstants.METRIC_EMAIL_TEMPLATE);
          String emailBody = Resources.toString(url, Charsets.UTF_8);
          java.text.DateFormat dateTimeFormat = new SimpleDateFormat(EmailConstants.DATE_MASK, Locale.ENGLISH);
          emailBody = emailBody.replace(EmailConstants.ALERT_VALUE_KEY, String.valueOf(value))
            .replace(EmailConstants.TIME_KEY, dateTimeFormat.format(new Date((Long) System.currentTimeMillis())))
            .replace(EmailConstants.PIPELINE_NAME_KEY, pipelineName)
            .replace(EmailConstants.CONDITION_KEY, ruleDefinition.getCondition())
            .replace(EmailConstants.URL_KEY, runtimeInfo.getBaseHttpUrl() + EmailConstants.PIPELINE_URL + pipelineName);

          if(ruleDefinition instanceof DataRuleDefinition) {
            emailBody = emailBody.replace(EmailConstants.ALERT_NAME_KEY, ((DataRuleDefinition)ruleDefinition).getLabel());
          } else {
            emailBody = emailBody.replace(EmailConstants.ALERT_NAME_KEY, ruleDefinition.getAlertText());
          }

          if(emailSender == null) {
            LOG.error("Email Sender is not configured. Alert '{}' with message '{}' will not be sent via email.",
              ruleDefinition.getId(), emailBody);
          } else {
            emailSender.send(emailIds, EmailConstants.STREAMSETS_DATA_COLLECTOR_ALERT + ruleDefinition.getAlertText(), emailBody);
          }
        } catch (EmailException | IOException e) {
          LOG.error("Error sending alert email, reason: {}", e.toString(), e);
          //Log error and move on. This should not stop the pipeline.
        }
      }
    } else {
      AlertManagerHelper.updateAlertGauge(gauge, value, ruleDefinition);
    }
  }

  public void alertException(Object value, RuleDefinition ruleDefinition) {
    AlertManagerHelper.alertException(pipelineName, revision, metrics, value, ruleDefinition);
  }
}
