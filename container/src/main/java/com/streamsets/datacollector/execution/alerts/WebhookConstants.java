/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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


public class WebhookConstants {

  public static final String PIPELINE_TITLE_KEY = "{{PIPELINE_TITLE}}";
  public static final String PIPELINE_URL_KEY = "{{PIPELINE_URL}}";
  public static final String PIPELINE_STATE_KEY = "{{PIPELINE_STATE}}";
  public static final String TIME_KEY = "{{TIME}}";
  public static final String ALERT_TEXT_KEY = "{{ALERT_TEXT}}";
  public static final String ALERT_NAME_KEY = "{{ALERT_NAME}}";
  public static final String ALERT_VALUE_KEY = "{{ALERT_VALUE}}";
  public static final String ALERT_CONDITION_KEY = "{{ALERT_CONDITION}}";

}
