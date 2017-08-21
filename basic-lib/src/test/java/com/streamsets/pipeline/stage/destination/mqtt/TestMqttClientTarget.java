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
package com.streamsets.pipeline.stage.destination.mqtt;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.mqtt.Errors;
import com.streamsets.pipeline.lib.mqtt.MqttClientConfigBean;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

public class TestMqttClientTarget {

  @Test
  public void testTarget() throws Exception {
    MqttClientConfigBean commonConf = new MqttClientConfigBean();
    commonConf.brokerUrl = "tcp://localhost:1833";
    commonConf.clientId = UUID.randomUUID().toString();

    MqttClientTargetConfigBean publisherConf = new MqttClientTargetConfigBean();
    publisherConf.topic = "sample/topic";
    publisherConf.dataFormat = DataFormat.TEXT;
    publisherConf.dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    publisherConf.dataGeneratorFormatConfig.textFieldPath = "/text";

    MqttClientTarget target = new MqttClientTarget(commonConf, publisherConf);

    TargetRunner runner = new TargetRunner.Builder(MqttClientDTarget.class, target)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    try {
      runner.runValidateConfigs();
    } catch (Exception ex) {
      Assert.assertNotNull(ex.getCause());
      StageException stageException = (StageException) ex.getCause();
      Assert.assertNotNull(stageException.getErrorCode());
      Assert.assertEquals(stageException.getErrorCode(), Errors.MQTT_04);
    }
  }

}
