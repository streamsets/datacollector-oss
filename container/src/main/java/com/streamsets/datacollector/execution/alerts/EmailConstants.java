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

public class EmailConstants {

  public static final String CURRENT_VALUE = "currentValue";
  public static final String EXCEPTION_MESSAGE = "exceptionMessage";
  public static final String TIMESTAMP = "timestamp";
  public static final String STREAMSETS_DATA_COLLECTOR_ALERT = "StreamSets Data Collector Alert - ";
  public static final String ALERT_ERROR_EMAIL_TEMPLATE = "email-template-error-alert.txt";
  public static final String NOTIFY_ERROR_EMAIL_TEMPLATE = "email-template-error-notify.txt";
  public static final String METRIC_EMAIL_TEMPLATE = "email-template-metric.txt";
  public static final String ALERT_NAME_KEY = "${ALERT_NAME}";
  public static final String ALERT_VALUE_KEY = "${ALERT_VALUE}";
  public static final String TIME_KEY = "${TIME}";
  public static final String PIPELINE_NAME_KEY = "${PIPELINE_NAME}";
  public static final String DESCRIPTION_KEY = "${DESCRIPTION}";
  public static final String ERROR_CODE = "${ERROR_CODE}";
  public static final String CONDITION_KEY = "${CONDITION}";
  public static final String URL_KEY = "${URL}";
  public static final String DATE_MASK = "yyyy-MM-dd HH:mm:ss";
  public static final String PIPELINE_URL = "/collector/pipeline/";
  public static final String PIPELINE_STATE_CHANGE__EMAIL_TEMPLATE = "email-template-pipeline-state-change.txt";
  public static final String SDC_STATE_CHANGE__EMAIL_TEMPLATE = "email-template-sdc-state-change.txt";
  public static final String MESSAGE_KEY = "${MESSAGE}";

  public static final String ALERT_TEXTS = "alertTexts";

  private EmailConstants() {}
}
