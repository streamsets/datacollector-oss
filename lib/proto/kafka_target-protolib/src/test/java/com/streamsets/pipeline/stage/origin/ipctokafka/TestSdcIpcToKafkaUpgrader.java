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
package com.streamsets.pipeline.stage.origin.ipctokafka;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.stage.util.tls.TlsConfigBeanUpgraderTestUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestSdcIpcToKafkaUpgrader {

  @Test
  public void testV2toV3() throws Exception {
    List<Config> configs = new ArrayList<>();
    configs.add(new Config("configs.maxRpcRequestSize", 10));
    new SdcIpcToKafkaUpgrader().upgrade("l", "s", "i", 2, 3, configs);
    Assert.assertEquals(1, configs.size());
    Assert.assertEquals("configs.maxRpcRequestSize", configs.get(0).getName());
    Assert.assertEquals(10 * 1000, configs.get(0).getValue());
  }

  @Test
  public void testV3toV4() throws StageException {
    TlsConfigBeanUpgraderTestUtil.testRawKeyStoreConfigsToTlsConfigBeanUpgrade(
        "configs.",
        new SdcIpcToKafkaUpgrader(),
        4
    );
  }
}
