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
package com.streamsets.pipeline.stage.origin.jdbc.cdc.oracle;

import com.streamsets.pipeline.api.Config;
import org.junit.Assert;
import org.junit.Test;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

public class TestOracleCDCSourceUpgrader {
  @Test
  public void upgradeV1ToV2() throws Exception {
    List<Config> configs = new ArrayList<>(1);

    configs = new OracleCDCSourceUpgrader().upgrade("a", "b", "v", 1, 2, configs);
    Assert.assertEquals(2, configs.size());
    Assert.assertEquals(configs.get(0).getName(), "oracleCDCConfigBean.txnWindow");
    Assert.assertEquals(configs.get(0).getValue(), "${1 * HOURS}");
    Assert.assertEquals(configs.get(1).getName(), "oracleCDCConfigBean.logminerWindow");
    Assert.assertEquals(configs.get(1).getValue(), "${2 * HOURS}");
  }

  @Test
  public void upgradeV2TOV3() throws Exception {
    List<Config> configs = new ArrayList<>(1);

    configs = new OracleCDCSourceUpgrader().upgrade("a", "b", "v", 2, 3, configs);
    Assert.assertEquals(6, configs.size());
    Assert.assertEquals(configs.get(0).getName(), "oracleCDCConfigBean.bufferLocally");
    Assert.assertEquals(configs.get(0).getValue(), false);
    Assert.assertEquals(configs.get(1).getName(), "oracleCDCConfigBean.discardExpired");
    Assert.assertEquals(configs.get(1).getValue(), false);
    Assert.assertEquals(configs.get(2).getName(), "oracleCDCConfigBean.unsupportedFieldOp");
    Assert.assertEquals(configs.get(2).getValue(), UnsupportedFieldTypeValues.TO_ERROR);
    Assert.assertEquals(configs.get(3).getName(), "oracleCDCConfigBean.keepOriginalQuery");
    Assert.assertEquals(configs.get(3).getValue(), false);
    Assert.assertEquals(configs.get(4).getName(), "oracleCDCConfigBean.dbTimeZone");
    Assert.assertEquals(configs.get(4).getValue(), ZoneId.systemDefault().getId());
    Assert.assertEquals(configs.get(5).getName(), "oracleCDCConfigBean.queryTimeout");
    Assert.assertEquals(configs.get(5).getValue(), "${5 * MINUTES}");
  }

  @Test
  public void upgradeV3TOV4() throws Exception {
    List<Config> configs = new ArrayList<>(1);

    configs = new OracleCDCSourceUpgrader().upgrade("a", "b", "v", 3, 4, configs);
    Assert.assertEquals(1, configs.size());
    Assert.assertEquals(configs.get(0).getName(), "oracleCDCConfigBean.jdbcFetchSize");
    Assert.assertEquals(configs.get(0).getValue(), 1);
  }

  @Test
  public void upgradeV4TOV5() throws Exception {
    List<Config> configs = new ArrayList<>(1);

    configs = new OracleCDCSourceUpgrader().upgrade("a", "b", "v", 4, 5, configs);
    Assert.assertEquals(1, configs.size());
    Assert.assertEquals(configs.get(0).getName(), "oracleCDCConfigBean.sendUnsupportedFields");
    Assert.assertEquals(configs.get(0).getValue(), false);
  }
}
