/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.alerts;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.streamsets.pipeline.config.RuleDefinition;
import com.streamsets.pipeline.email.EmailSender;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.metrics.MetricsConfigurator;
import com.streamsets.pipeline.util.PipelineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class AlertManager {

  private static Logger LOG = LoggerFactory.getLogger(AlertManager.class);

  private static final String CURRENT_VALUE = "currentValue";
  private static final String TIMESTAMP = "timestamp";
  private static final String STREAMSETS_DATA_COLLECTOR_ALERT = "StreamsSets Data Collector Alert - ";
  private static final String EMAIL_TEMPLATE = "Value \t\t: ALERT_VALUE_KEY\n" +
    "Time \t\t: TIME_KEY\n" +
    "Pipeline\t: PIPELINE_NAME_KEY\n" +
    "Condition\t: CONDITION_KEY\n" +
    "URL\t\t: URL_KEY";
  private static final String ALERT_VALUE_KEY = "ALERT_VALUE_KEY";
  private static final String TIME_KEY = "TIME_KEY";
  private static final String PIPELINE_NAME_KEY = "PIPELINE_NAME_KEY";
  private static final String CONDITION_KEY = "CONDITION_KEY";
  private static final String URL_KEY = "URL_KEY";
  private static final String DATE_MASK = "yyyy-MM-dd HH:mm:ss";
  private static final String PIPELINE_URL = "/collector/pipeline/";

  private final String pipelineName;
  private final String revision;
  private final EmailSender emailSender;
  private final MetricRegistry metrics;
  private final RuntimeInfo runtimeInfo;

  public AlertManager(String pipelineName, String revision, EmailSender emailSender, MetricRegistry metrics,
                      RuntimeInfo runtimeInfo) {
    this.pipelineName = pipelineName;
    this.revision = revision;
    this.emailSender = emailSender;
    this.metrics = metrics;
    this.runtimeInfo = runtimeInfo;
  }

  public void alert(Object value, List<String> emailIds, RuleDefinition ruleDefinition) {
    final Map<String, Object> alertResponse = new HashMap<>();
    alertResponse.put(CURRENT_VALUE, value);
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(ruleDefinition.getId()));
    if (gauge == null) {
      alertResponse.put(TIMESTAMP, System.currentTimeMillis());
      //send email the first time alert is triggered
      if(ruleDefinition.isSendEmail()) {

        String emailBody = new String(EMAIL_TEMPLATE);
        java.text.DateFormat dateTimeFormat = new SimpleDateFormat(DATE_MASK, Locale.ENGLISH);
        emailBody = emailBody.replace(ALERT_VALUE_KEY, String.valueOf(value))
          .replace(TIME_KEY, dateTimeFormat.format(new Date((Long) alertResponse.get(TIMESTAMP))))
          .replace(PIPELINE_NAME_KEY, pipelineName)
          .replace(CONDITION_KEY, ruleDefinition.getCondition())
          .replace(URL_KEY, runtimeInfo.getBaseHttpUrl() + PIPELINE_URL + pipelineName.replaceAll(" ", "%20"));

        if(emailSender == null) {
          LOG.warn("Email Sender is not configured. Alert '{}' with message '{}' will not be sent via email.",
            ruleDefinition.getId(), emailBody);
        } else {
          try {
            emailSender.send(emailIds, STREAMSETS_DATA_COLLECTOR_ALERT + ruleDefinition.getAlertText(), emailBody);
          } catch (PipelineException e) {
            LOG.error("Error sending alert email, reason: {}", e.getMessage(), e);
            //Log error and move on. This should not stop the pipeline.
          }
        }
      }
    } else {
      //remove existing gauge
      MetricsConfigurator.removeGauge(metrics, AlertsUtil.getAlertGaugeName(ruleDefinition.getId()));
      alertResponse.put(TIMESTAMP, ((Map<String, Object>)gauge.getValue()).get(TIMESTAMP));
    }
    Gauge<Object> alertResponseGauge = new Gauge<Object>() {
      @Override
      public Object getValue() {
        return alertResponse;
      }
    };
    MetricsConfigurator.createGauge(metrics, AlertsUtil.getAlertGaugeName(ruleDefinition.getId()),
      alertResponseGauge);
  }
}
