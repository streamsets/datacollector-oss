/**
 * Copyright 2015 StreamSets Inc.
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
package com.streamsets.pipeline.stage.destination.flume;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;

import java.util.List;

public class FlumeConfigBean {

  @ConfigDefBean(groups = {"FLUME"})
  public FlumeConfig flumeConfig;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "SDC_JSON",
    label = "Data Format",
    description = "",
    displayPosition = 140,
    group = "FLUME"
  )
  @ValueChooserModel(FlumeDestinationDataFormatChooserValues.class)
  public DataFormat dataFormat;

  @ConfigDefBean(groups = {"FLUME"})
  public DataGeneratorFormatConfig dataGeneratorFormatConfig;

  public boolean init (Stage.Context context, List<Stage.ConfigIssue> issues) {
    boolean valid = true;

    valid &= flumeConfig.init(context, issues);
    valid &= dataGeneratorFormatConfig.init(context, dataFormat, Groups.FLUME.name(), "dataGeneratorFormatConfig",
      issues);

    return valid;
  }

  public void destroy() {
    flumeConfig.destroy();
  }
}
