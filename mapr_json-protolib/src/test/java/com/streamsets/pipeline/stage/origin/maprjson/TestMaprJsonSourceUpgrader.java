/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.maprjson;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageUpgrader;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

public final class TestMaprJsonSourceUpgrader {

  @Test
  public void testUpgradeV1ToV2() throws Exception {
    List<Config> configs = new ArrayList<>();
    MaprJsonSourceUpgrader maprJsonSourceUpgrader = new MaprJsonSourceUpgrader();

    StageUpgrader.Context context = Mockito.mock(StageUpgrader.Context.class);
    Mockito.doReturn(1).when(context).getFromVersion();
    Mockito.doReturn(2).when(context).getToVersion();

    maprJsonSourceUpgrader.upgrade(configs, context);

    Assert.assertEquals(1, configs.size());
    Config config = configs.get(0);
    Assert.assertEquals(
        MapRJsonOriginDSource.MAPR_JSON_ORIGIN_CONFIG_BEAN_PREFIX + "." + MapRJsonOriginConfigBean.JSON_MAX_OBJECT_LENGTH,
        config.getName()
    );
    Assert.assertEquals(4096, config.getValue());
  }

}
