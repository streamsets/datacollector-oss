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
package com.streamsets.pipeline.stage.origin.s3;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.InterfaceAudience;
import com.streamsets.pipeline.api.InterfaceStability;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.common.DataFormatConstants;

import java.util.List;

@InterfaceAudience.LimitedPrivate
@InterfaceStability.Unstable
public class S3FileConfig {

  private static final int MIN_OVERRUN_LIMIT = 64 * 1024;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "Prefix Pattern",
    description = "An Ant-style path pattern that defines the remaining portion of prefix excluding the common prefix",
    displayPosition = 100,
    group = "#0"
  )
  public String prefixPattern;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "LEXICOGRAPHICAL",
      label = "Read Order",
      description = "Read objects based on the last-modified timestamp or lexicographically ascending key names. " +
          "When timestamp ordering is used, objects with the same timestamp are ordered based on key names.",
      displayPosition = 110,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0"
  )
  @ValueChooserModel(ObjectOrderingChooseValues.class)
  public ObjectOrdering objectOrdering = ObjectOrdering.LEXICOGRAPHICAL;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.NUMBER,
    label = "File pool size",
    defaultValue = "100",
    description = "When listing and sorting files for processing, this property controls how many files will be sorted and persisted." +
      " SDC will perform new file listing only after all the files in the pool have been transferred.",
    displayPosition = 115,
    group = "#0",
    displayMode = ConfigDef.DisplayMode.ADVANCED,
    min = 1,
    max = Integer.MAX_VALUE
  )
  public int poolSize = 100;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.NUMBER,
    label = "Buffer Limit (KB)",
    defaultValue = "128",
    description = "Low level reader buffer limit to avoid out of Memory errors",
    displayPosition = 120,
    group = "#0",
    displayMode = ConfigDef.DisplayMode.ADVANCED,
    min = 1,
    max = Integer.MAX_VALUE
  )
  public int overrunLimit;

  public void init(Stage.Context context, List<Stage.ConfigIssue> issues) {
    validate(context, issues);
  }

  private void validate(Stage.Context context, List<Stage.ConfigIssue> issues) {
    overrunLimit = overrunLimit * 1024; //convert to KB
    if (overrunLimit < MIN_OVERRUN_LIMIT || overrunLimit >= DataFormatConstants.MAX_OVERRUN_LIMIT) {
      issues.add(context.createConfigIssue(Groups.S3.name(), "overrunLimit", Errors.S3_SPOOLDIR_04, MIN_OVERRUN_LIMIT/1024 /* KB */, DataFormatConstants.MAX_OVERRUN_LIMIT/1024/1024 /* MB */));
    }
    if (prefixPattern == null || prefixPattern.isEmpty()) {
      issues.add(context.createConfigIssue(Groups.S3.name(), "prefixPattern", Errors.S3_SPOOLDIR_06));
    }
  }
}
