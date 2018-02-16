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
package com.streamsets.pipeline.stage.destination.waveanalytics;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.lib.salesforce.SubscriptionType;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static com.streamsets.pipeline.lib.waveanalytics.WaveAnalyticsConfigBean.APPEND_TIMESTAMP;
import static com.streamsets.pipeline.stage.destination.waveanalytics.WaveAnalyticsDTarget
    .WAVE_ANALYTICS_DESTINATION_CONFIG_BEAN_PREFIX;

public class TestWaveAnalyticsUpgrader {
  private static final String WAVE_ANALYTICS_APPEND_TIMESTAMP = WAVE_ANALYTICS_DESTINATION_CONFIG_BEAN_PREFIX + "." + APPEND_TIMESTAMP;

  @Test
  public void testUpgradeV1toV2NoAppendTimestamp() throws StageException {
    List<Config> configs = new ArrayList<>();
    StageUpgrader.Context context = Mockito.mock(StageUpgrader.Context.class);
    Mockito.doReturn(1).when(context).getFromVersion();
    Mockito.doReturn(2).when(context).getToVersion();

    WaveAnalyticsUpgrader waveAnalyticsUpgrader = new WaveAnalyticsUpgrader();
    waveAnalyticsUpgrader.upgrade(configs, context);

    Assert.assertEquals(1, configs.size());
    Config config = configs.get(0);
    Assert.assertEquals(WAVE_ANALYTICS_APPEND_TIMESTAMP, config.getName());
    Assert.assertEquals(true, config.getValue());
  }

  public void testUpgradeV1toV2AppendTimestampTrue() throws StageException {
    List<Config> configs = new ArrayList<>();
    StageUpgrader.Context context = Mockito.mock(StageUpgrader.Context.class);
    Mockito.doReturn(1).when(context).getFromVersion();
    Mockito.doReturn(2).when(context).getToVersion();

    configs.add(new Config(WAVE_ANALYTICS_APPEND_TIMESTAMP, true));

    WaveAnalyticsUpgrader waveAnalyticsUpgrader = new WaveAnalyticsUpgrader();
    waveAnalyticsUpgrader.upgrade(configs, context);

    Assert.assertEquals(1, configs.size());
    Config config = configs.get(0);
    Assert.assertEquals(WAVE_ANALYTICS_APPEND_TIMESTAMP, config.getName());
    Assert.assertEquals(true, config.getValue());
  }

  public void testUpgradeV1toV2AppendTimestampFalse() throws StageException {
    List<Config> configs = new ArrayList<>();
    StageUpgrader.Context context = Mockito.mock(StageUpgrader.Context.class);
    Mockito.doReturn(1).when(context).getFromVersion();
    Mockito.doReturn(2).when(context).getToVersion();

    configs.add(new Config(WAVE_ANALYTICS_APPEND_TIMESTAMP, false));

    WaveAnalyticsUpgrader waveAnalyticsUpgrader = new WaveAnalyticsUpgrader();
    waveAnalyticsUpgrader.upgrade(configs, context);

    Assert.assertEquals(1, configs.size());
    Config config = configs.get(0);
    Assert.assertEquals(WAVE_ANALYTICS_APPEND_TIMESTAMP, config.getName());
    Assert.assertEquals(false, config.getValue());
  }
}
