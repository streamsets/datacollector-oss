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
package com.streamsets.datacollector.restapi.bean;

import com.streamsets.datacollector.restapi.bean.ConfigConfigurationJson;
import com.streamsets.pipeline.api.Config;

import org.junit.Assert;
import org.junit.Test;

public class TestConfigConfigurationBean {

  @Test(expected = NullPointerException.class)
  public void testConfigConfigurationBeanNull() {
    ConfigConfigurationJson configConfigurationJsonBean =
      new ConfigConfigurationJson(null);
  }

  @Test
  public void testConfigConfigurationBean() {
    Config config =
      new Config("url", "http://localhost:9090");
    ConfigConfigurationJson configConfigurationJsonBean =
      new ConfigConfigurationJson(config);

    Assert.assertEquals(config.getName(), configConfigurationJsonBean.getName());
    Assert.assertEquals(config.getValue(), configConfigurationJsonBean.getValue());

    Assert.assertEquals(config.getName(), configConfigurationJsonBean.getConfigConfiguration().getName());
    Assert.assertEquals(config.getValue(), configConfigurationJsonBean.getConfigConfiguration().getValue());

  }

  @Test
  public void testConfigConfigurationBeanConstructorWithArgs() {
    Config config =
      new Config("url", "http://localhost:9090");

    ConfigConfigurationJson configConfigurationJsonBean =
      new ConfigConfigurationJson("url", "http://localhost:9090");

    Assert.assertEquals(config.getName(), configConfigurationJsonBean.getName());
    Assert.assertEquals(config.getValue(), configConfigurationJsonBean.getValue());

    Assert.assertEquals(config.getName(), configConfigurationJsonBean.getConfigConfiguration().getName());
    Assert.assertEquals(config.getValue(), configConfigurationJsonBean.getConfigConfiguration().getValue());

  }
}
