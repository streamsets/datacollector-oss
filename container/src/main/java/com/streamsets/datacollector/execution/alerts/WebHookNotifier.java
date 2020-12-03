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

import com.google.common.base.Strings;
import com.streamsets.datacollector.config.AuthenticationType;
import com.streamsets.datacollector.config.PipelineWebhookConfig;
import com.streamsets.datacollector.config.WebhookCommonConfig;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.StateEventListener;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.event.json.MeterJson;
import com.streamsets.datacollector.event.json.MetricRegistryJson;
import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.dc.execution.manager.standalone.ThreadUsage;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.StageException;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class WebHookNotifier implements StateEventListener {

  private static final Logger LOG = LoggerFactory.getLogger(WebHookNotifier.class);
  private static final String APPLICATION_JSON = "application/json";

  private final String pipelineId;
  private final String pipelineTitle;
  private final String rev;
  private final PipelineConfigBean pipelineConfigBean;
  private final RuntimeInfo runtimeInfo;
  private Set<String> pipelineStates;
  private Map<String, Object> runtimeParameters;

  public WebHookNotifier(
      String pipelineId,
      String pipelineTitle,
      String rev,
      PipelineConfigBean pipelineConfigBean,
      RuntimeInfo runtimeInfo,
      Map<String, Object> runtimeParameters
  ) {
    this.pipelineId = pipelineId;
    this.pipelineTitle = pipelineTitle;
    this.rev = rev;
    this.pipelineConfigBean = pipelineConfigBean;
    this.runtimeInfo = runtimeInfo;
    this.runtimeParameters = runtimeParameters;

    pipelineStates = new HashSet<>();
    for(com.streamsets.datacollector.config.PipelineState s : pipelineConfigBean.notifyOnStates) {
      pipelineStates.add(s.name());
    }
  }

  public String getPipelineId() {
    return pipelineId;
  }

  public String getRev() {
    return rev;
  }

  @Override
  public void onStateChange(
      PipelineState fromState,
      PipelineState toState,
      String toStateJson,
      ThreadUsage threadUsage,
      Map<String, String> offset
  ) throws PipelineRuntimeException {
    //should not be active in slave mode
    if(toState.getExecutionMode() != ExecutionMode.SLAVE && pipelineId.equals(toState.getPipelineId())) {
      if (pipelineStates != null && pipelineStates.contains(toState.getStatus().name())) {
        for (PipelineWebhookConfig webhookConfig : pipelineConfigBean.webhookConfigs) {
          if (!StringUtils.isEmpty(webhookConfig.webhookUrl)) {
            Response response = null;
            try {
              DateFormat dateTimeFormat = new SimpleDateFormat(EmailConstants.DATE_MASK, Locale.ENGLISH);
              String payload = webhookConfig.payload
                  .replace(WebhookConstants.PIPELINE_TITLE_KEY,
                          escapeValue(Strings.nullToEmpty(pipelineTitle), webhookConfig.contentType))
                  .replace(WebhookConstants.PIPELINE_URL_KEY,
                          escapeValue(Strings.nullToEmpty(
                                  runtimeInfo.getBaseHttpUrl(true) + EmailConstants.PIPELINE_URL + toState.getPipelineId().replaceAll(" ", "%20")), webhookConfig.contentType))
                  .replace(WebhookConstants.PIPELINE_STATE_KEY,
                          escapeValue(Strings.nullToEmpty(toState.getStatus().toString()), webhookConfig.contentType))
                  .replace(WebhookConstants.TIME_KEY,
                          escapeValue(dateTimeFormat.format(new Date(toState.getTimeStamp())), webhookConfig.contentType))
                  .replace(WebhookConstants.PIPELINE_STATE_MESSAGE_KEY,
                          escapeValue(Strings.nullToEmpty(toState.getMessage()), webhookConfig.contentType))
                  .replace(WebhookConstants.PIPELINE_RUNTIME_PARAMETERS_KEY, Strings.nullToEmpty(
                      StringEscapeUtils.escapeJson(ObjectMapperFactory.get().writeValueAsString(runtimeParameters))))
                  .replace(WebhookConstants.PIPELINE_METRICS_KEY, Strings.nullToEmpty(
                          StringEscapeUtils.escapeJson(toState.getMetrics())));

              if (payload.contains(WebhookConstants.PIPELINE_INPUT_RECORDS_COUNT_KEY) ||
                  payload.contains(WebhookConstants.PIPELINE_OUTPUT_RECORDS_COUNT_KEY) ||
                  payload.contains(WebhookConstants.PIPELINE_ERROR_RECORDS_COUNT_KEY) ||
                  payload.contains(WebhookConstants.PIPELINE_ERROR_MESSAGES_COUNT_KEY)) {
                long inputRecordsCount = 0;
                long outputRecordsCount = 0;
                long errorRecordsCount = 0;
                long errorMessagesCount = 0;

                if (toState.getMetrics() != null) {
                  MetricRegistryJson metricRegistryJson =  ObjectMapperFactory.get()
                      .readValue(toState.getMetrics(), MetricRegistryJson.class);

                  if (metricRegistryJson != null && metricRegistryJson.getMeters() != null) {
                    MeterJson batchInputRecords = metricRegistryJson.getMeters()
                        .get("pipeline.batchInputRecords" + MetricsConfigurator.METER_SUFFIX);
                    if (batchInputRecords != null) {
                      inputRecordsCount = batchInputRecords.getCount();
                    }

                    MeterJson batchOutputRecords = metricRegistryJson.getMeters()
                        .get("pipeline.batchOutputRecords" + MetricsConfigurator.METER_SUFFIX);
                    if (batchOutputRecords != null) {
                      outputRecordsCount = batchOutputRecords.getCount();
                    }

                    MeterJson batchErrorRecords = metricRegistryJson.getMeters()
                        .get("pipeline.batchErrorRecords" + MetricsConfigurator.METER_SUFFIX);
                    if (batchErrorRecords != null) {
                      errorRecordsCount = batchErrorRecords.getCount();
                    }

                    MeterJson batchErrorMessagesRecords = metricRegistryJson.getMeters()
                        .get("pipeline.batchErrorMessages" + MetricsConfigurator.METER_SUFFIX);
                    if (batchErrorMessagesRecords != null) {
                      errorMessagesCount = batchErrorMessagesRecords.getCount();
                    }
                  }
                }

                payload = payload
                    .replace(WebhookConstants.PIPELINE_INPUT_RECORDS_COUNT_KEY, inputRecordsCount + "")
                    .replace(WebhookConstants.PIPELINE_OUTPUT_RECORDS_COUNT_KEY, outputRecordsCount + "")
                    .replace(WebhookConstants.PIPELINE_ERROR_RECORDS_COUNT_KEY, errorRecordsCount + "")
                    .replace(WebhookConstants.PIPELINE_ERROR_MESSAGES_COUNT_KEY, errorMessagesCount + "");
              }

              WebTarget webTarget = ClientBuilder.newClient().target(webhookConfig.webhookUrl);
              configurePasswordAuth(webhookConfig, webTarget);
              Invocation.Builder builder = webTarget.request();
              for (String headerKey: webhookConfig.headers.keySet()) {
                builder.header(headerKey, webhookConfig.headers.get(headerKey));
              }
              response = builder.post(Entity.entity(payload, webhookConfig.contentType));

              LOG.info("Received status code '{}' ", response.getStatus());

              if (response.getStatusInfo().getFamily() != Response.Status.Family.SUCCESSFUL) {
                LOG.error(
                    "Error calling Webhook URL, status code '{}': {}",
                    response.getStatus(),
                    response.readEntity(String.class)
                );
              }

            } catch (Exception e) {
              LOG.error("Error calling Webhook URL '{}' on state {}: {}", webhookConfig.webhookUrl, toState.getStatus().name(), e.toString(), e);
            } finally {
              if (response != null) {
                response.close();
              }
            }
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

  private String escapeValue(String value, String contentType) {
    if (APPLICATION_JSON.equals(contentType)) {
      return StringEscapeUtils.escapeJson(value);
    }
    return value;
  }
}
