/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.execution.alerts;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.datacollector.alerts.AlertEventListener;
import com.streamsets.datacollector.alerts.AlertsUtil;
import com.streamsets.datacollector.config.RuleDefinition;
import com.streamsets.datacollector.email.EmailSender;
import com.streamsets.datacollector.execution.EventListenerManager;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.util.PipelineException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class AlertManager {

  private static Logger LOG = LoggerFactory.getLogger(AlertManager.class);

  private static final String CURRENT_VALUE = "currentValue";
  private static final String EXCEPTION_MESSAGE = "exceptionMessage";
  private static final String TIMESTAMP = "timestamp";
  private static final String STREAMSETS_DATA_COLLECTOR_ALERT = "StreamsSets Data Collector Alert - ";
  private static final String ERROR_EMAIL_TEMPLATE = "Error Code \t\t: ERROR_CODE\n" +
    "Time \t\t: TIME_KEY\n" +
    "Pipeline\t: PIPELINE_NAME_KEY\n" +
    "URL\t\t: URL_KEY\n" +
  "Description\t: DESCRIPTION_KEY";
  private static final String METRIC_EMAIL_TEMPLATE = "Value \t\t: ALERT_VALUE_KEY\n" +
    "Time \t\t: TIME_KEY\n" +
    "Pipeline\t: PIPELINE_NAME_KEY\n" +
    "Condition\t: CONDITION_KEY\n" +
    "URL\t\t: URL_KEY";
  private static final String ALERT_VALUE_KEY = "ALERT_VALUE_KEY";
  private static final String TIME_KEY = "TIME_KEY";
  private static final String PIPELINE_NAME_KEY = "PIPELINE_NAME_KEY";
  private static final String DESCRIPTION_KEY = "DESCRIPTION_KEY";
  private static final String ERROR_CODE = "ERROR_CODE";
  private static final String CONDITION_KEY = "CONDITION_KEY";
  private static final String URL_KEY = "URL_KEY";
  private static final String DATE_MASK = "yyyy-MM-dd HH:mm:ss";
  private static final String PIPELINE_URL = "/collector/pipeline/";

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
    String emailBody = ERROR_EMAIL_TEMPLATE;
    java.text.DateFormat dateTimeFormat = new SimpleDateFormat(DATE_MASK, Locale.ENGLISH);
    emailBody = emailBody.replace(ERROR_CODE, errorCode)
      .replace(TIME_KEY, dateTimeFormat.format(new Date(timestamp)))
      .replace(PIPELINE_NAME_KEY, pipelineName)
      .replace(DESCRIPTION_KEY, description)
      .replace(URL_KEY, runtimeInfo.getBaseHttpUrl() + PIPELINE_URL + pipelineName.replaceAll(" ", "%20"));
    subject = STREAMSETS_DATA_COLLECTOR_ALERT + subject;
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
    alertResponse.put(CURRENT_VALUE, value);
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(ruleDefinition.getId()));

    Gauge<Object> alertResponseGauge = new Gauge<Object>() {
      @Override
      public Object getValue() {
        return alertResponse;
      }
    };

    if (gauge == null) {
      alertResponse.put(TIMESTAMP, System.currentTimeMillis());
      //send email the first time alert is triggered
      if(ruleDefinition.isSendEmail()) {

        String emailBody = METRIC_EMAIL_TEMPLATE;
        java.text.DateFormat dateTimeFormat = new SimpleDateFormat(DATE_MASK, Locale.ENGLISH);
        emailBody = emailBody.replace(ALERT_VALUE_KEY, String.valueOf(value))
          .replace(TIME_KEY, dateTimeFormat.format(new Date((Long) alertResponse.get(TIMESTAMP))))
          .replace(PIPELINE_NAME_KEY, pipelineName)
          .replace(CONDITION_KEY, ruleDefinition.getCondition())
          .replace(URL_KEY, runtimeInfo.getBaseHttpUrl() + PIPELINE_URL + pipelineName.replaceAll(" ", "%20"));

        if(emailSender == null) {
          LOG.error("Email Sender is not configured. Alert '{}' with message '{}' will not be sent via email.",
            ruleDefinition.getId(), emailBody);
        } else {
          try {
            emailSender.send(emailIds, STREAMSETS_DATA_COLLECTOR_ALERT + ruleDefinition.getAlertText(), emailBody);
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
      alertResponse.put(TIMESTAMP, ((Map<String, Object>) gauge.getValue()).get(TIMESTAMP));
    }
    MetricsConfigurator.createGauge(metrics, AlertsUtil.getAlertGaugeName(ruleDefinition.getId()),
      alertResponseGauge, pipelineName, revision);
  }

  public void alertException(Object value, RuleDefinition ruleDefinition) {
    final Map<String, Object> alertResponse = new HashMap<>();
    alertResponse.put(EXCEPTION_MESSAGE, value);
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(ruleDefinition.getId()));

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
