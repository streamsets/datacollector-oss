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


class WebhookConstants {

  static final String PIPELINE_TITLE_KEY = "{{PIPELINE_TITLE}}";
  static final String PIPELINE_URL_KEY = "{{PIPELINE_URL}}";
  static final String PIPELINE_STATE_KEY = "{{PIPELINE_STATE}}";
  static final String PIPELINE_STATE_MESSAGE_KEY = "{{PIPELINE_STATE_MESSAGE}}";
  static final String PIPELINE_RUNTIME_PARAMETERS_KEY = "{{PIPELINE_RUNTIME_PARAMETERS}}";
  static final String TIME_KEY = "{{TIME}}";
  static final String ALERT_TEXT_KEY = "{{ALERT_TEXT}}";
  static final String ALERT_NAME_KEY = "{{ALERT_NAME}}";
  static final String ALERT_VALUE_KEY = "{{ALERT_VALUE}}";
  static final String ALERT_CONDITION_KEY = "{{ALERT_CONDITION}}";
  static final String PIPELINE_INPUT_RECORDS_COUNT_KEY = "{{PIPELINE_INPUT_RECORDS_COUNT}}";
  static final String PIPELINE_OUTPUT_RECORDS_COUNT_KEY = "{{PIPELINE_OUTPUT_RECORDS_COUNT}}";
  static final String PIPELINE_ERROR_RECORDS_COUNT_KEY = "{{PIPELINE_ERROR_RECORDS_COUNT}}";
  static final String PIPELINE_ERROR_MESSAGES_COUNT_KEY = "{{PIPELINE_ERROR_MESSAGES_COUNT}}";
  static final String PIPELINE_METRICS_KEY = "{{PIPELINE_METRICS}}";
}
