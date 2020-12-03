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

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.io.Resources;
import com.streamsets.datacollector.email.EmailException;
import com.streamsets.datacollector.email.EmailSender;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.StateEventListener;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.dc.execution.manager.standalone.ThreadUsage;
import com.streamsets.pipeline.api.ExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class EmailNotifier implements StateEventListener {

  private static final Logger LOG = LoggerFactory.getLogger(EmailNotifier.class);

  private final String pipelineId;
  private final String pipelineTitle;
  private final String rev;
  private final RuntimeInfo runtimeInfo;
  private final EmailSender emailSender;
  private final List<String> emails;
  private final Set<String> pipelineStates;

  public EmailNotifier(
      String pipelineId,
      String pipelineTitle,
      String rev,
      RuntimeInfo runtimeInfo,
      EmailSender emailSender,
      List<String> emails,
      Set<String> pipelineStates
  ) {
    this.pipelineId = pipelineId;
    this.pipelineTitle = pipelineTitle;
    this.rev = rev;
    this.runtimeInfo = runtimeInfo;
    this.emailSender = emailSender;
    this.emails = emails;
    this.pipelineStates = pipelineStates;
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
        LOG.debug("Generating email notification for state {}", toState.getStatus().name());
        //pipeline switched to a terminal state. Send email
        String emailBody = null;
        String subject = null;
        URL url;
        try {
          switch (toState.getStatus()) {
            case START_ERROR:
            case RUNNING_ERROR:
            case STOP_ERROR:
            case RUN_ERROR:
              url = Resources.getResource(EmailConstants.NOTIFY_ERROR_EMAIL_TEMPLATE);
              emailBody = Resources.toString(url, Charsets.UTF_8);
              emailBody = emailBody.replace(EmailConstants.DESCRIPTION_KEY, Strings.nullToEmpty(toState.getMessage()));
              subject = EmailConstants.STREAMSETS_DATA_COLLECTOR_ALERT + pipelineTitle + " - ERROR";
              break;
            case STOPPED:
              url = Resources.getResource(EmailConstants.PIPELINE_STATE_CHANGE__EMAIL_TEMPLATE);
              emailBody = Resources.toString(url, Charsets.UTF_8);
              emailBody = emailBody.replace(EmailConstants.MESSAGE_KEY, "was stopped");
              subject = EmailConstants.STREAMSETS_DATA_COLLECTOR_ALERT + pipelineTitle + " - STOPPED";
              break;
            case FINISHED:
              url = Resources.getResource(EmailConstants.PIPELINE_STATE_CHANGE__EMAIL_TEMPLATE);
              emailBody = Resources.toString(url, Charsets.UTF_8);
              emailBody = emailBody.replace(EmailConstants.MESSAGE_KEY, "finished executing");
              subject = EmailConstants.STREAMSETS_DATA_COLLECTOR_ALERT + pipelineTitle + " - FINISHED";
              break;
            case RUNNING:
              url = Resources.getResource(EmailConstants.PIPELINE_STATE_CHANGE__EMAIL_TEMPLATE);
              emailBody = Resources.toString(url, Charsets.UTF_8);
              emailBody = emailBody.replace(EmailConstants.MESSAGE_KEY, "started executing");
              subject = EmailConstants.STREAMSETS_DATA_COLLECTOR_ALERT + pipelineTitle + " - RUNNING";
              break;
            case DISCONNECTED:
              url = Resources.getResource(EmailConstants.SDC_STATE_CHANGE__EMAIL_TEMPLATE);
              emailBody = Resources.toString(url, Charsets.UTF_8);
              emailBody = emailBody.replace(EmailConstants.MESSAGE_KEY, "was shut down");
              subject = EmailConstants.STREAMSETS_DATA_COLLECTOR_ALERT + pipelineTitle + " - DISCONNECTED";
              break;
            case CONNECTING:
              url = Resources.getResource(EmailConstants.SDC_STATE_CHANGE__EMAIL_TEMPLATE);
              emailBody = Resources.toString(url, Charsets.UTF_8);
              emailBody = emailBody.replace(EmailConstants.MESSAGE_KEY, "was started");
              subject = EmailConstants.STREAMSETS_DATA_COLLECTOR_ALERT + pipelineTitle + " - CONNECTING";
              break;
            default:
              throw new IllegalStateException("Unexpected PipelineState " + toState);
          }
        } catch (IOException e) {
          throw new PipelineRuntimeException(ContainerError.CONTAINER_01000, e.toString(), e);
        }
        java.text.DateFormat dateTimeFormat = new SimpleDateFormat(EmailConstants.DATE_MASK, Locale.ENGLISH);
        emailBody = emailBody.replace(EmailConstants.TIME_KEY, dateTimeFormat.format(new Date(toState.getTimeStamp())))
          .replace(EmailConstants.PIPELINE_NAME_KEY, Strings.nullToEmpty(pipelineTitle))
          .replace(EmailConstants.URL_KEY, runtimeInfo.getBaseHttpUrl(true) + EmailConstants.PIPELINE_URL +
            toState.getPipelineId().replaceAll(" ", "%20"));
        try {
          emailSender.send(emails, subject, emailBody);
        } catch (EmailException e) {
          LOG.error("Error sending email on status change {}: '{}'", toState.getStatus().name(), e.toString(), e);
        }
      }
    }
  }
}
