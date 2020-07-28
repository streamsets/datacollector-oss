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
package com.streamsets.pipeline.stage.origin.http;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.lib.el.RecordEL;
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
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  @ValueChooserModel(PaginationModeChooserValues.class)
  public PaginationMode mode = PaginationMode.NONE;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Next Page Link Prefix",
      description = "Prefix to concatenate with the value resolved from the Next Page Link Field. To be used in case the  Next Page Link is relative.",
      group = "#0",
      dependsOn = "mode",
      triggeredByValue = "LINK_FIELD",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String nextPageURLPrefix;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Next Page Link Field",
      description = "Field path in the response containing a URL to the next page.",
      group = "#0",
      dependsOn = "mode",
      triggeredByValue = "LINK_FIELD",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  @FieldSelectorModel(singleValued = true)
  public String nextPageFieldPath;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Stop Condition",
      description = "Expression that evaluates to true when there are no more pages to process.",
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      elDefs = { RecordEL.class },
      group = "#0",
      dependsOn = "mode",
      triggeredByValue = "LINK_FIELD",
      displayPosition = 35,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String stopCondition;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Initial Page/Offset",
      description = "Value of ${startAt} variable the first time the pipeline is run or Reset Origin is invoked",
      group = "#0",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC,
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
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependsOn = "mode",
      triggeredByValue = { "LINK_HEADER", "LINK_FIELD", "BY_PAGE", "BY_OFFSET" }
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
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependsOn = "mode",
      triggeredByValue = { "LINK_HEADER", "LINK_FIELD", "BY_PAGE", "BY_OFFSET" }
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
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      dependsOn = "mode",
      triggeredByValue = { "LINK_HEADER", "LINK_FIELD", "BY_PAGE", "BY_OFFSET" }
  )
  public long rateLimit = 2000;
}
