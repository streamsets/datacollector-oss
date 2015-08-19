/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.execution.alerts;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.streamsets.datacollector.alerts.AlertsUtil;
import com.streamsets.datacollector.config.RuleDefinition;
import com.streamsets.datacollector.email.EmailSender;
import com.streamsets.datacollector.execution.EventListenerManager;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.util.PipelineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

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
    String emailBody = EmailConstants.ERROR_EMAIL_TEMPLATE;
    java.text.DateFormat dateTimeFormat = new SimpleDateFormat(EmailConstants.DATE_MASK, Locale.ENGLISH);
    emailBody = emailBody.replace(EmailConstants.ERROR_CODE, errorCode)
      .replace(EmailConstants.TIME_KEY, dateTimeFormat.format(new Date(timestamp)))
      .replace(EmailConstants.PIPELINE_NAME_KEY, pipelineName)
      .replace(EmailConstants.DESCRIPTION_KEY, description)
      .replace(EmailConstants.URL_KEY, runtimeInfo.getBaseHttpUrl() + EmailConstants.PIPELINE_URL + pipelineName.replaceAll(" ", "%20"));
    subject = EmailConstants.STREAMSETS_DATA_COLLECTOR_ALERT + subject;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Email Alert: subject = " + subject + ", body = " + emailBody);
    }
    if(emailSender == null) {
      LOG.error("Email Sender is not configured. Alert with message '{}' will not be sent via email:",
        emailBody, throwable);
    } else {
      try {
        emailSender.send(emailIds, subject, emailBody);
      } catch (PipelineException e) {
        LOG.error("Error sending alert email, reason: {}", e.toString(), e);
        //Log error and move on. This should not stop the pipeline.
      }
    }
  }

  public void alert(Object value, List<String> emailIds, RuleDefinition ruleDefinition) {
    final Map<String, Object> alertResponse = new HashMap<>();
    alertResponse.put(EmailConstants.CURRENT_VALUE, value);
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(ruleDefinition.getId()));

    Gauge<Object> alertResponseGauge = new Gauge<Object>() {
      @Override
      public Object getValue() {
        return alertResponse;
      }
    };

    if (gauge == null) {
      alertResponse.put(EmailConstants.TIMESTAMP, System.currentTimeMillis());
      //send email the first time alert is triggered
      if(ruleDefinition.isSendEmail()) {

        String emailBody = EmailConstants.METRIC_EMAIL_TEMPLATE;
        java.text.DateFormat dateTimeFormat = new SimpleDateFormat(EmailConstants.DATE_MASK, Locale.ENGLISH);
        emailBody = emailBody.replace(EmailConstants.ALERT_VALUE_KEY, String.valueOf(value))
          .replace(EmailConstants.TIME_KEY, dateTimeFormat.format(new Date((Long) alertResponse.get(EmailConstants.TIMESTAMP))))
          .replace(EmailConstants.PIPELINE_NAME_KEY, pipelineName)
          .replace(EmailConstants.CONDITION_KEY, ruleDefinition.getCondition())
          .replace(EmailConstants.URL_KEY, runtimeInfo.getBaseHttpUrl() + EmailConstants.PIPELINE_URL + pipelineName.replaceAll(" ", "%20"));

        if(emailSender == null) {
          LOG.error("Email Sender is not configured. Alert '{}' with message '{}' will not be sent via email.",
            ruleDefinition.getId(), emailBody);
        } else {
          try {
            emailSender.send(emailIds, EmailConstants.STREAMSETS_DATA_COLLECTOR_ALERT + ruleDefinition.getAlertText(), emailBody);
          } catch (PipelineException e) {
            LOG.error("Error sending alert email, reason: {}", e.toString(), e);
            //Log error and move on. This should not stop the pipeline.
          }
        }
      }
      eventListenerManager.broadcastAlerts(new AlertInfo(pipelineName, ruleDefinition, alertResponseGauge));
    } else {
      //remove existing gauge
      MetricsConfigurator.removeGauge(metrics, AlertsUtil.getAlertGaugeName(ruleDefinition.getId()), pipelineName,
        revision);
      alertResponse.put(EmailConstants.TIMESTAMP, ((Map<String, Object>) gauge.getValue()).get(EmailConstants.TIMESTAMP));
    }
    MetricsConfigurator.createGauge(metrics, AlertsUtil.getAlertGaugeName(ruleDefinition.getId()),
      alertResponseGauge, pipelineName, revision);
  }

  public void alertException(Object value, RuleDefinition ruleDefinition) {
    final Map<String, Object> alertResponse = new HashMap<>();
    alertResponse.put(EmailConstants.EXCEPTION_MESSAGE, value);
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics, AlertsUtil.getAlertGaugeName(ruleDefinition.getId()));

    if (gauge != null) {
      //remove existing gauge
      MetricsConfigurator.removeGauge(metrics, AlertsUtil.getAlertGaugeName(ruleDefinition.getId()), pipelineName,
        revision);
    }

    Gauge<Object> alertResponseGauge = new Gauge<Object>() {
      @Override
      public Object getValue() {
        return alertResponse;
      }
    };

    MetricsConfigurator.createGauge(metrics, AlertsUtil.getAlertGaugeName(ruleDefinition.getId()),
      alertResponseGauge, pipelineName, revision);
  }
}
