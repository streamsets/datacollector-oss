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
package com.streamsets.datacollector.execution.alerts;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.io.Resources;
import com.streamsets.datacollector.alerts.AlertsUtil;
import com.streamsets.datacollector.config.AuthenticationType;
import com.streamsets.datacollector.config.DataRuleDefinition;
import com.streamsets.datacollector.config.RuleDefinition;
import com.streamsets.datacollector.config.RuleDefinitionsWebhookConfig;
import com.streamsets.datacollector.config.WebhookCommonConfig;
import com.streamsets.datacollector.creation.RuleDefinitionsConfigBean;
import com.streamsets.datacollector.email.EmailException;
import com.streamsets.datacollector.email.EmailSender;
import com.streamsets.datacollector.execution.EventListenerManager;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.pipeline.api.StageException;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;

public class AlertManager {

  private static Logger LOG = LoggerFactory.getLogger(AlertManager.class);

  private final String pipelineId;
  private final String pipelineTitle;
  private final String revision;
  private final EmailSender emailSender;
  private final MetricRegistry metrics;
  private final RuntimeInfo runtimeInfo;
  private final EventListenerManager eventListenerManager;

  public AlertManager(
      String pipelineId,
      String pipelineTitle,
      String revision,
      EmailSender emailSender,
      MetricRegistry metrics,
      RuntimeInfo runtimeInfo,
      EventListenerManager eventListenerManager
  ) {
    this.pipelineId = pipelineId;
    this.pipelineTitle = pipelineTitle;
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
        .replace(EmailConstants.PIPELINE_NAME_KEY, Strings.nullToEmpty(pipelineTitle))
        .replace(EmailConstants.DESCRIPTION_KEY, Strings.nullToEmpty(description))
        .replace(EmailConstants.URL_KEY, runtimeInfo.getBaseHttpUrl(true) + EmailConstants.PIPELINE_URL + pipelineId);
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

  @SuppressWarnings("unchecked")
  public void alert(
      Object value,
      RuleDefinitionsConfigBean ruleDefinitionsConfigBean,
      RuleDefinition ruleDefinition
  ) {
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics, AlertsUtil.getAlertGaugeName(ruleDefinition.getId()));
    if (gauge == null) {
      Gauge<Object> alertResponseGauge = AlertManagerHelper.createAlertResponseGauge(
          pipelineId,
          revision,
          metrics,
          value,
          ruleDefinition
      );

      eventListenerManager.broadcastAlerts(new AlertInfo(pipelineId, ruleDefinition, alertResponseGauge));

      //send email the first time alert is triggered
      if(ruleDefinition.isSendEmail()) {
        try {
          URL url = Resources.getResource(EmailConstants.METRIC_EMAIL_TEMPLATE);
          String emailBody = Resources.toString(url, Charsets.UTF_8);
          java.text.DateFormat dateTimeFormat = new SimpleDateFormat(EmailConstants.DATE_MASK, Locale.ENGLISH);
          emailBody = emailBody.replace(EmailConstants.ALERT_VALUE_KEY, String.valueOf(value))
            .replace(EmailConstants.TIME_KEY, dateTimeFormat.format(new Date((Long) System.currentTimeMillis())))
            .replace(EmailConstants.PIPELINE_NAME_KEY, Strings.nullToEmpty(pipelineTitle))
            .replace(EmailConstants.CONDITION_KEY, Strings.nullToEmpty(ruleDefinition.getCondition()))
            .replace(EmailConstants.URL_KEY, runtimeInfo.getBaseHttpUrl(true) + EmailConstants.PIPELINE_URL + pipelineId);

          if(ruleDefinition instanceof DataRuleDefinition) {
            emailBody = emailBody.replace(EmailConstants.ALERT_NAME_KEY, ((DataRuleDefinition)ruleDefinition).getLabel());
          } else {
            emailBody = emailBody.replace(EmailConstants.ALERT_NAME_KEY, ruleDefinition.getAlertText());
          }

          if(emailSender == null) {
            LOG.error("Email Sender is not configured. Alert '{}' with message '{}' will not be sent via email.",
              ruleDefinition.getId(), emailBody);
          } else {
            emailSender.send(
                ruleDefinitionsConfigBean.emailIDs,
                EmailConstants.STREAMSETS_DATA_COLLECTOR_ALERT + ruleDefinition.getAlertText(),
                emailBody
            );
          }
        } catch (EmailException | IOException e) {
          LOG.error("Error sending alert email, reason: {}", e.toString(), e);
          //Log error and move on. This should not stop the pipeline.
        }
      }


      if (ruleDefinitionsConfigBean != null && ruleDefinitionsConfigBean.webhookConfigs != null &&
          !ruleDefinitionsConfigBean.webhookConfigs.isEmpty()) {
        invokeWebhook(value, ruleDefinitionsConfigBean, ruleDefinition);
      }


    } else {
      AlertManagerHelper.updateAlertGauge(gauge, value, ruleDefinition);
    }
  }

  public void alertException(Object value, RuleDefinition ruleDefinition) {
    AlertManagerHelper.alertException(pipelineId, revision, metrics, value, ruleDefinition);
  }

  private void invokeWebhook(
      Object value,
      RuleDefinitionsConfigBean ruleDefinitionsConfigBean,
      RuleDefinition ruleDefinition
  ) {
    DateFormat dateTimeFormat = new SimpleDateFormat(EmailConstants.DATE_MASK, Locale.ENGLISH);
    String alertName;
    if(ruleDefinition instanceof DataRuleDefinition) {
      alertName = ((DataRuleDefinition)ruleDefinition).getLabel();
    } else {
      alertName = ruleDefinition.getAlertText();
    }
    for (RuleDefinitionsWebhookConfig webhookConfig : ruleDefinitionsConfigBean.webhookConfigs) {
      if (!StringUtils.isEmpty(webhookConfig.webhookUrl)) {
        Response response = null;
        try {
          String payload = webhookConfig.payload
              .replace(WebhookConstants.ALERT_TEXT_KEY, Strings.nullToEmpty(ruleDefinition.getAlertText()))
              .replace(WebhookConstants.ALERT_NAME_KEY, Strings.nullToEmpty(alertName))
              .replace(WebhookConstants.ALERT_VALUE_KEY, String.valueOf(value))
              .replace(WebhookConstants.ALERT_CONDITION_KEY, Strings.nullToEmpty(ruleDefinition.getCondition()))
              .replace(WebhookConstants.TIME_KEY, dateTimeFormat.format(new Date((Long) System.currentTimeMillis())))
              .replace(WebhookConstants.PIPELINE_TITLE_KEY, Strings.nullToEmpty(pipelineTitle))
              .replace(WebhookConstants.PIPELINE_URL_KEY, runtimeInfo.getBaseHttpUrl(true) +
                  EmailConstants.PIPELINE_URL + pipelineId.replaceAll(" ", "%20"));

          WebTarget webTarget = ClientBuilder.newClient().target(webhookConfig.webhookUrl);
          configurePasswordAuth(webhookConfig, webTarget);
          Invocation.Builder builder = webTarget.request();
          for (String headerKey: webhookConfig.headers.keySet()) {
            builder.header(headerKey, webhookConfig.headers.get(headerKey));
          }
          response = builder.post(Entity.entity(payload, webhookConfig.contentType));

          if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            LOG.error(
                "Error calling Webhook URL, status code '{}': {}",
                response.getStatus(),
                response.readEntity(String.class)
            );
          }
        } catch (Exception e) {
          LOG.error("Error calling Webhook URL : {}", e.toString(), e);
        } finally {
          if (response != null) {
            response.close();
          }
        }
      }
    }
  }

  private void configurePasswordAuth(WebhookCommonConfig webhookConfig, WebTarget webTarget) throws StageException {
    if (webhookConfig.authType == AuthenticationType.BASIC) {
      webTarget.register(
          HttpAuthenticationFeature.basic(webhookConfig.username.get(), webhookConfig.password.get())
      );
    }

    if (webhookConfig.authType == AuthenticationType.DIGEST) {
      webTarget.register(
          HttpAuthenticationFeature.digest(webhookConfig.username.get(), webhookConfig.password.get())
      );
    }

    if (webhookConfig.authType == AuthenticationType.UNIVERSAL) {
      webTarget.register(
          HttpAuthenticationFeature.universal(webhookConfig.username.get(), webhookConfig.password.get())
      );
    }
  }
}
