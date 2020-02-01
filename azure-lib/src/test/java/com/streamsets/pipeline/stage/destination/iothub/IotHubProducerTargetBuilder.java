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
package com.streamsets.pipeline.stage.destination.iothub;

import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;

class IotHubProducerTargetBuilder {
  private IotHubProducerConfigBean producerConf;

  IotHubProducerTargetBuilder() {
    producerConf = new IotHubProducerConfigBean();
    producerConf.dataFormat = DataFormat.TEXT;
    producerConf.dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    producerConf.dataGeneratorFormatConfig.textFieldPath = "/text";
  }

  IotHubProducerTargetBuilder iotHubName(String iotHubName) {
    producerConf.iotHubName = iotHubName;
    return this;
  }

  IotHubProducerTargetBuilder deviceId(String deviceId) {
    producerConf.deviceId = deviceId;
    return this;
  }

  IotHubProducerTargetBuilder sasKey(String sasKey) {
    producerConf.sasKey = () -> sasKey;
    return this;
  }

  IotHubProducerTarget build() {
    return new IotHubProducerTarget(producerConf);
  }

}
