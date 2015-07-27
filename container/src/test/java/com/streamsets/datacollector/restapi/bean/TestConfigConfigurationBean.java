/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
