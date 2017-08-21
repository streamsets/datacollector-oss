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
package com.streamsets.pipeline.stage.origin.coapserver;

import com.streamsets.pipeline.common.DataFormatConstants;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;

import java.util.ArrayList;
import java.util.List;

public class CoapServerPushSource extends AbstractCoapServerPushSource<CoapReceiver> {

  private final CoapServerConfigs coAPServerConfigs;

  private final DataFormat dataFormat;

  private final DataParserFormatConfig dataFormatConfig;

  CoapServerPushSource(
      CoapServerConfigs coAPServerConfigs,
      DataFormat dataFormat,
      DataParserFormatConfig dataFormatConfig
  ) {
    super(coAPServerConfigs, new PushCoapReceiver(coAPServerConfigs, dataFormatConfig));
    this.coAPServerConfigs = coAPServerConfigs;
    this.dataFormat = dataFormat;
    this.dataFormatConfig = dataFormatConfig;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = new ArrayList<>();
    dataFormatConfig.stringBuilderPoolSize = coAPServerConfigs.maxConcurrentRequests;
    dataFormatConfig.init(
        getContext(),
        dataFormat,
        com.streamsets.pipeline.stage.origin.httpserver.Groups.DATA_FORMAT.name(),
        "dataFormatConfig",
        DataFormatConstants.MAX_OVERRUN_LIMIT,
        issues
    );
    issues.addAll(getReceiver().init(getContext()));
    if (issues.isEmpty()) {
      issues.addAll(super.init());
    }
    return issues;
  }
}
