/**
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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.rabbitmq;

import com.google.common.base.Joiner;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.config.DataFormat;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestRabbitSourceUpgrader {
  @Test
  public void testUpgradeV1ToV2() throws Exception{
    final Joiner JOINER = Joiner.on(".");

    List<Config> configs = new ArrayList<>();
    configs.add(new Config(JOINER.join("conf", "uri"), "amqp://localhost:5672"));
    configs.add(new Config(JOINER.join("conf", "consumerTag"), ""));
    configs.add(new Config(JOINER.join("conf", "dataFormat"), DataFormat.JSON));

    RabbitSourceUpgrader upgrader = new RabbitSourceUpgrader();
    upgrader.upgrade("a", "b", "c", 1, 2, configs);

    Assert.assertEquals(4, configs.size());
    boolean isValid = false;
    for (Config config : configs) {
      if (JOINER.join("conf", "produceSingleRecordPerMessage").equals(config.getName())) {
        isValid = !((boolean)config.getValue());
      }
    }
    Assert.assertTrue("Should contain produceSingleRecordPerMessage and its value set to false", isValid);

  }
}
