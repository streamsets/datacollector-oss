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
package com.streamsets.pipeline.stage.processor.statsaggregation;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.streamsets.datacollector.alerts.AlertsUtil;
import com.streamsets.datacollector.config.DataRuleDefinition;
import com.streamsets.datacollector.config.DriftRuleDefinition;
import com.streamsets.datacollector.config.RuleDefinition;
import com.streamsets.datacollector.execution.alerts.AlertManagerHelper;
import com.streamsets.datacollector.execution.alerts.EmailConstants;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class RulesEvaluator {

  private static final Logger LOG = LoggerFactory.getLogger(RulesEvaluator.class);

  private final String pipelineName;
  private final String revision;
  private final String pipelineUrl;
  private final Stage.Context context;
  private List<String> emails;

  public RulesEvaluator(
      String pipelineName,
      String revision,
      String pipelineUrl,
      Stage.Context context
  ) {
    this.pipelineName = pipelineName;
    this.revision = revision;
    this.pipelineUrl = pipelineUrl;
    this.context = context;
  }

  void setEmails(List<String> emails) {
    this.emails = emails;
  }

  void alert(
      MetricRegistry metrics,
      RuleDefinition ruleDefinition,
      Object value,
      Map<String, BoundedDeque<String>> ruleToAlertTextForMatchedRecords
  ) {
    if (ruleToAlertTextForMatchedRecords.containsKey(ruleDefinition.getId())) {
      if (ruleDefinition instanceof DriftRuleDefinition) {
        for (String alertText : ruleToAlertTextForMatchedRecords.get(ruleDefinition.getId())) {
          DataRuleDefinition dataRuleDefinition = AlertManagerHelper.cloneRuleWithResolvedAlertText(
            (DataRuleDefinition) ruleDefinition,
            alertText
          );
          raiseAlert(metrics, dataRuleDefinition, value);
        }
      } else if (ruleDefinition instanceof DataRuleDefinition) {
        DataRuleDefinition dataRuleDefinition = AlertManagerHelper.cloneRuleWithResolvedAlertText(
          (DataRuleDefinition) ruleDefinition,
          ruleToAlertTextForMatchedRecords.get(ruleDefinition.getId()).iterator().next()
        );
        raiseAlert(metrics, dataRuleDefinition, value);
      } else {
        raiseAlert(metrics, ruleDefinition, value);
      }
    }
  }

  void raiseAlert(
      MetricRegistry metrics,
      RuleDefinition ruleDefinition,
      Object value
  ) {
    Gauge<Object> gauge = MetricsConfigurator.getGauge(
      metrics,
      AlertsUtil.getAlertGaugeName(ruleDefinition.getId())
    );
    if (gauge == null) {
      AlertManagerHelper.createAlertResponseGauge(
        pipelineName,
        revision,
        metrics,
        value,
        ruleDefinition
      );

      if(ruleDefinition.isSendEmail()) {
        try {
          URL url = Resources.getResource(EmailConstants.METRIC_EMAIL_TEMPLATE);
          String emailBody = Resources.toString(url, Charsets.UTF_8);
          java.text.DateFormat dateTimeFormat = new SimpleDateFormat(EmailConstants.DATE_MASK, Locale.ENGLISH);
          emailBody = emailBody.replace(EmailConstants.ALERT_VALUE_KEY, String.valueOf(value))
              .replace(EmailConstants.TIME_KEY, dateTimeFormat.format(new Date(System.currentTimeMillis())))
              .replace(EmailConstants.PIPELINE_NAME_KEY, pipelineName)
              .replace(EmailConstants.CONDITION_KEY, ruleDefinition.getCondition())
              .replace(EmailConstants.URL_KEY, pipelineUrl);
          if(ruleDefinition instanceof DataRuleDefinition) {
            emailBody = emailBody.replace(EmailConstants.ALERT_NAME_KEY, ((DataRuleDefinition)ruleDefinition).getLabel());
          } else {
            emailBody = emailBody.replace(EmailConstants.ALERT_NAME_KEY, ruleDefinition.getAlertText());
          }
          ContextExtensions ext = (ContextExtensions) context;
          ext.notify(emails, EmailConstants.STREAMSETS_DATA_COLLECTOR_ALERT + ruleDefinition.getAlertText(), emailBody);
        } catch (IOException | StageException e) {
          LOG.error("Error sending alert email, reason: {}", e.toString(), e);
          //Log error and move on. This should not stop the pipeline.
        }
      }
    } else {
      AlertManagerHelper.updateAlertGauge(gauge, value, ruleDefinition);
    }
  }

}
