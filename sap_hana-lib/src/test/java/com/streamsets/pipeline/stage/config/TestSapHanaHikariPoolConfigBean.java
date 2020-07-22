/*
 * Copyright 2020 StreamSets Inc.
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

package com.streamsets.pipeline.stage.config;

import com.streamsets.pipeline.lib.jdbc.ConnectionPropertyBean;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Properties;

public class TestSapHanaHikariPoolConfigBean {

  @Test
  public void testSplitBatchCommandsDriverProps(){
    SapHanaHikariPoolConfigBean configBean = new SapHanaHikariPoolConfigBean();
    configBean.splitBatchCommandsSAP = true;
    Properties props = configBean.getDriverProperties();
    String splitBatchCommands = props.getProperty(configBean.SPLIT_BATCH_COMMANDS);

    Assert.assertNotNull(splitBatchCommands);
    Assert.assertEquals(splitBatchCommands,"TRUE");

    configBean.splitBatchCommandsSAP = false;
    props = configBean.getDriverProperties();

    Assert.assertNull(props.getProperty(configBean.SPLIT_BATCH_COMMANDS));
  }


  @Test
  public void testSplitBatchCommandsAndAdditionalPropsDriverProps(){
    SapHanaHikariPoolConfigBean configBean = new SapHanaHikariPoolConfigBean();
    configBean.splitBatchCommandsSAP = false;
    configBean.driverProperties = new ArrayList<>();
    ConnectionPropertyBean prop = new ConnectionPropertyBean();
    prop.key = SapHanaHikariPoolConfigBean.SPLIT_BATCH_COMMANDS;
    prop.value = () -> "TRUE";
    configBean.driverProperties.add(prop);
    Properties props = configBean.getDriverProperties();

    Assert.assertNull(props.getProperty(configBean.SPLIT_BATCH_COMMANDS));
  }


  @Test
  public void testConnectionString(){
    String expected = "jdbc:sap://FOOO:1234/?databaseName=DB_Fooo";
    SapHanaHikariPoolConfigBean configBean = new SapHanaHikariPoolConfigBean();
    configBean.hostSAP = "FOOO";
    configBean.portSAP = 1234;
    configBean.databaseNameSAP = "DB_Fooo";
    String connString = configBean.getConnectionString();

    Assert.assertEquals(connString, expected);
  }

}
