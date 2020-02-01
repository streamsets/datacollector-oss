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
package com.streamsets.pipeline.stage.origin.eventhubs;

import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.eventhubs.EventHubConfigBean;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;

class EventHubConsumerSourceBuilder {
  private EventHubConfigBean commonConf;
  private EventHubConsumerConfigBean consumerConf;

  EventHubConsumerSourceBuilder() {
    commonConf = new EventHubConfigBean();
    consumerConf = new EventHubConsumerConfigBean();
    consumerConf.maxThreads = 1;
    consumerConf.dataFormat = DataFormat.TEXT;
    consumerConf.dataFormatConfig = new DataParserFormatConfig();
  }

  EventHubConsumerSourceBuilder namespaceName(String namespaceName) {
    commonConf.namespaceName = namespaceName;
    return this;
  }

  EventHubConsumerSourceBuilder eventHubName(String eventHubName) {
    commonConf.eventHubName = eventHubName;
    return this;
  }

  EventHubConsumerSourceBuilder sasKeyName(String sasKeyName) {
    commonConf.sasKeyName = sasKeyName;
    return this;
  }

  EventHubConsumerSourceBuilder sasKey(String sasKey) {
    commonConf.sasKey = () -> sasKey;
    return this;
  }

  EventHubConsumerSource build() {
    return new EventHubConsumerSource(commonConf, consumerConf);
  }

}
