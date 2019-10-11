/*
 * Copyright 2019 StreamSets Inc.
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

package com.streamsets.pipeline.stage.origin.maprstreams;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import com.streamsets.pipeline.stage.origin.multikafka.MultiKafkaSourceUpgrader;
import com.streamsets.pipeline.upgrader.SelectorStageUpgrader;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class TestMultiMapRStreamsSourceUpgrader {

  @Test
  public void testV2ToV3() {
    List<Config> configs = new ArrayList<>();

    final URL yamlResource = ClassLoader.getSystemClassLoader().getResource("upgrader/MultiMapRStreamsDSource.yaml");
    final SelectorStageUpgrader upgrader = new SelectorStageUpgrader(
        "stage",
        null,
        yamlResource
    );

    StageUpgrader.Context context = Mockito.mock(StageUpgrader.Context.class);
    Mockito.doReturn(2).when(context).getFromVersion();
    Mockito.doReturn(3).when(context).getToVersion();

    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, "conf.keyCaptureMode", "NONE");
    UpgraderTestUtils.assertExists(configs, "conf.keyCaptureAttribute", "kafkaMessageKey");
    UpgraderTestUtils.assertExists(configs, "conf.keyCaptureField", "/kafkaMessageKey");
  }

  @Test
  public void testV3ToV4() {
    List<Config> configs = new ArrayList<>();

    final URL yamlResource = ClassLoader.getSystemClassLoader().getResource("upgrader/MultiMapRStreamsDSource.yaml");
    final SelectorStageUpgrader upgrader = new SelectorStageUpgrader(
        "stage",
        null,
        yamlResource
    );

    StageUpgrader.Context context = Mockito.mock(StageUpgrader.Context.class);
    Mockito.doReturn(3).when(context).getFromVersion();
    Mockito.doReturn(4).when(context).getToVersion();

    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, "conf.dataFormatConfig.basicAuth", "");
  }
}
