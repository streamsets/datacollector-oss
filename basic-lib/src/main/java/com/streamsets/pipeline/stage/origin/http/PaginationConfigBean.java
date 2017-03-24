/*
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.http;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.ValueChooserModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bean for configuring HTTP request pagination.
 */
public class PaginationConfigBean {
  private static final Logger LOG = LoggerFactory.getLogger(PaginationConfigBean.class);

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Pagination Mode",
      defaultValue = "NONE",
      group = "#0",
      displayPosition = 20
  )
  @ValueChooserModel(PaginationModeChooserValues.class)
  public PaginationMode mode = PaginationMode.NONE;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Initial Page/Offset",
      description = "Value of ${startAt} variable the first time the pipeline is run or Reset Origin is invoked",
      group = "#0",
      displayPosition = 40,
      dependsOn = "mode",
      triggeredByValue = { "BY_PAGE", "BY_OFFSET" }
  )
  @FieldSelectorModel(singleValued = true)
  public int startAt = 0;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Result Field Path",
      description = "Field path to parse as Records. The field type must be a list.",
      group = "#0",
      displayPosition = 40,
      dependsOn = "mode",
      triggeredByValue = { "LINK_HEADER", "BY_PAGE", "BY_OFFSET" }
  )
  @FieldSelectorModel(singleValued = true)
  public String resultFieldPath;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Keep All Fields",
      description = "Includes all fields in the output record, rather than only fields from Result Field Path",
      defaultValue = "false",
      group = "#0",
      displayPosition = 50,
      dependsOn = "mode",
      triggeredByValue = { "LINK_HEADER", "BY_PAGE", "BY_OFFSET" }
  )
  public boolean keepAllFields;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Wait Time Between Pages (ms)",
      defaultValue = "2000",
      description = "Time to wait between requests when paging (rate limit). E.g. 2000 would be 30 requests per minute",
      min = 0,
      group = "#0",
      displayPosition = 50,
      dependsOn = "mode",
      triggeredByValue = { "LINK_HEADER", "BY_PAGE", "BY_OFFSET" }
  )
  public long rateLimit = 2000;
}
