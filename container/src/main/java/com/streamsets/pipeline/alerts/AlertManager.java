/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.alerts;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.streamsets.pipeline.email.EmailSender;
import com.streamsets.pipeline.metrics.MetricsConfigurator;
import com.streamsets.pipeline.util.PipelineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AlertManager {

  private static Logger LOG = LoggerFactory.getLogger(AlertManager.class);

  private static final String CURRENT_VALUE = "currentValue";
  private static final String TIMESTAMP = "timestamp";
  private static final String STREAMSETS_DATA_COLLECTOR_ALERT = "StreamsSets Data Collector Alert - ";


  private final EmailSender emailSender;
  private final MetricRegistry metrics;

  public AlertManager(EmailSender emailSender, MetricRegistry metrics) {
    this.emailSender = emailSender;
    this.metrics = metrics;
  }

  public void alert(Object value,  boolean sendEmail, List<String> emailIds, String ruleId, String alertText) {
    final Map<String, Object> alertResponse = new HashMap<>();
    alertResponse.put(CURRENT_VALUE, value);
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(ruleId));
    if (gauge == null) {
      alertResponse.put(TIMESTAMP, System.currentTimeMillis());
      //send email the first time alert is triggered
      if(sendEmail) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(CURRENT_VALUE + " = " + value).append(", " + TIMESTAMP + " = " +
          alertResponse.get(TIMESTAMP));
        if(emailSender == null) {
          LOG.warn("Email Sender is not configured. Alert '{}' with message '{}' will not be sent via email.",
            ruleId, stringBuilder.toString());
        } else {
          try {
            emailSender.send(emailIds, STREAMSETS_DATA_COLLECTOR_ALERT + alertText,
              stringBuilder.toString());
          } catch (PipelineException e) {
            LOG.error("Error sending alert email, reason: {}", e.getMessage());
            //Log error and move on. This should not stop the pipeline.
          }
        }
      }
    } else {
      //remove existing gauge
      MetricsConfigurator.removeGauge(metrics, AlertsUtil.getAlertGaugeName(ruleId));
      alertResponse.put(TIMESTAMP, ((Map<String, Object>)gauge.getValue()).get(TIMESTAMP));
    }
    Gauge<Object> alertResponseGauge = new Gauge<Object>() {
      @Override
      public Object getValue() {
        return alertResponse;
      }
    };
    MetricsConfigurator.createGuage(metrics, AlertsUtil.getAlertGaugeName(ruleId),
      alertResponseGauge);
  }
}
