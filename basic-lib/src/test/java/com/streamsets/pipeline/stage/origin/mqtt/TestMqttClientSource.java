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
package com.streamsets.pipeline.stage.origin.mqtt;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.mqtt.Errors;
import com.streamsets.pipeline.lib.mqtt.MqttClientConfigBean;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class TestMqttClientSource {

  @Test
  public void testSource() throws Exception {
    MqttClientConfigBean commonConf = new MqttClientConfigBean();
    commonConf.brokerUrl = "tcp://localhost:1833";
    commonConf.clientId = UUID.randomUUID().toString();

    MqttClientSourceConfigBean subscriberConf = new MqttClientSourceConfigBean();
    subscriberConf.topicFilters = ImmutableList.of("sample/topic");
    subscriberConf.dataFormat = DataFormat.JSON;
    subscriberConf.dataFormatConfig = new DataParserFormatConfig();

    MqttClientSource source = new MqttClientSource(commonConf, subscriberConf);
    PushSourceRunner runner = new PushSourceRunner
        .Builder(MqttClientDSource.class, source)
        .addOutputLane("a").build();

    runner.runInit();
    try {
      List<Record> records = new ArrayList<>();
      runner.runProduce(Collections.<String, String>emptyMap(), 1, new PushSourceRunner.Callback() {
        @Override
        public void processBatch(StageRunner.Output output) {
          records.clear();
          records.addAll(output.getRecords().get("a"));
          runner.setStop();
        }
      });
      runner.waitOnProduce();
    } catch (Exception ex) {
      Assert.assertNotNull(ex.getCause());
      Assert.assertNotNull(ex.getCause().getCause());
      StageException stageException = (StageException) ex.getCause().getCause();
      Assert.assertNotNull(stageException.getErrorCode());
      Assert.assertEquals(stageException.getErrorCode(), Errors.MQTT_04);
    } finally {
      runner.runDestroy();
    }
  }

}
