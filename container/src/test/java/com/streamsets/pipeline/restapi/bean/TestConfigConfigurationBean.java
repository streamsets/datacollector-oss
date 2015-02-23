/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.bean;

import com.streamsets.pipeline.config.ConfigConfiguration;
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
    com.streamsets.pipeline.config.ConfigConfiguration configConfiguration =
      new ConfigConfiguration("url", "http://localhost:9090");
    ConfigConfigurationJson configConfigurationJsonBean =
      new ConfigConfigurationJson(configConfiguration);

    Assert.assertEquals(configConfiguration.getName(), configConfigurationJsonBean.getName());
    Assert.assertEquals(configConfiguration.getValue(), configConfigurationJsonBean.getValue());

    Assert.assertEquals(configConfiguration.getName(), configConfigurationJsonBean.getConfigConfiguration().getName());
    Assert.assertEquals(configConfiguration.getValue(), configConfigurationJsonBean.getConfigConfiguration().getValue());

  }

  @Test
  public void testConfigConfigurationBeanConstructorWithArgs() {
    com.streamsets.pipeline.config.ConfigConfiguration configConfiguration =
      new ConfigConfiguration("url", "http://localhost:9090");

    ConfigConfigurationJson configConfigurationJsonBean =
      new ConfigConfigurationJson("url", "http://localhost:9090");

    Assert.assertEquals(configConfiguration.getName(), configConfigurationJsonBean.getName());
    Assert.assertEquals(configConfiguration.getValue(), configConfigurationJsonBean.getValue());

    Assert.assertEquals(configConfiguration.getName(), configConfigurationJsonBean.getConfigConfiguration().getName());
    Assert.assertEquals(configConfiguration.getValue(), configConfigurationJsonBean.getConfigConfiguration().getValue());

  }
}
