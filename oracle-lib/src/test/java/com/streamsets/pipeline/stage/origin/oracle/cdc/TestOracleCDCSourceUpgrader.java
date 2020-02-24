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
package com.streamsets.pipeline.stage.origin.oracle.cdc;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.lib.jdbc.parser.sql.UnsupportedFieldTypeValues;
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

  @Test
  public void upgradeV6TOV7() throws Exception {
    List<Config> configs = new ArrayList<>(1);

    configs = new OracleCDCSourceUpgrader().upgrade("a", "b", "v", 6, 7, configs);
    Assert.assertEquals(1, configs.size());
    Assert.assertEquals(configs.get(0).getName(), "oracleCDCConfigBean.parseQuery");
    Assert.assertEquals(configs.get(0).getValue(), true);
  }

  @Test
  public void upgradeV7TOV8() throws Exception {
    List<Config> configs = new ArrayList<>(2);

    configs = new OracleCDCSourceUpgrader().upgrade("a", "b", "v", 7, 8, configs);
    Assert.assertEquals(2, configs.size());
    Assert.assertEquals(configs.get(0).getName(), "oracleCDCConfigBean.useNewParser");
    Assert.assertEquals(configs.get(0).getValue(), false);
    Assert.assertEquals(configs.get(1).getName(), "oracleCDCConfigBean.parseThreadPoolSize");
    Assert.assertEquals(configs.get(1).getValue(), 1);
  }

  @Test
  public void upgradeV8TOV9() throws Exception {
    List<Config> configs = new ArrayList<>(1);
    configs.add(new Config("oracleCDCConfigBean.queryTimeout", 10));

    configs = new OracleCDCSourceUpgrader().upgrade("a", "b", "v", 8, 9, configs);
    Assert.assertTrue(configs.isEmpty());
  }

  @Test
  public void upgradeV9TOV10() throws Exception {
    List<Config> configs = new ArrayList<>(1);
    configs.add(new Config("oracleCDCConfigBean.jdbcFetchSize", 100));
    List<Config>  ret = new OracleCDCSourceUpgrader().upgrade("a", "b", "v", 9, 10, configs);
    Assert.assertEquals(2, ret.size());
    Assert.assertEquals("oracleCDCConfigBean.fetchSizeLatest", ret.get(1).getName());
    Assert.assertEquals(100, ret.get(1).getValue());
  }

  @Test
  public void upgradeV10TOV11() throws Exception {
    List<Config> configs = new ArrayList<>();
    String jdbcUrl = "jdbc:oracle:thin:@//[HOST][:PORT]/SERVICE";
    configs.add(new Config(OracleCDCSourceUpgrader.HIKARI_CONF +"connectionString", jdbcUrl));
    configs.add(new Config(OracleCDCSourceUpgrader.HIKARI_CONF + "useCredentials", true));
    List<Config>  ret = new OracleCDCSourceUpgrader().upgrade("a", "b", "v", 10, 11, configs);
    Assert.assertEquals(2, ret.size());
    configs.forEach(c -> {
      Assert.assertTrue(c.getName().startsWith(OracleCDCSourceUpgrader.HIKARI_CONFIG_BEAN));
      if (c.getName().endsWith("connectionString")) {
        Assert.assertEquals(c.getValue(), jdbcUrl);
      } else if (c.getName().endsWith("useCredentials")) {
        Assert.assertEquals(c.getValue(), Boolean.TRUE);
      } else {
        Assert.fail(String.format("Wrong config : %s and value: %s", c.getName(), c.getValue()));
      }
    });

  }

}
