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
package com.streamsets.pipeline.stage.origin.hdfs.cluster;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DataFormat;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TestClusterHdfsSourceUpgrader {

  @Test
  public void testUpgradeV3toV4() throws StageException {
    List<Config> configs = new ArrayList<>();
    configs.add(new Config("dataFormat", DataFormat.TEXT));
    configs.add(new Config("hdfsUri", "MY_URI"));
    configs.add(new Config("hdfsDirLocations", null));
    configs.add(new Config("recursive", true));
    configs.add(new Config("produceSingleRecordPerMessage", false));
    configs.add(new Config("hdfsKerberos", true));
    configs.add(new Config("hdfsConfDir", "MY_DIR"));
    configs.add(new Config("hdfsUser", "MY_USER"));
    configs.add(new Config("hdfsConfigs", null));
    configs.add(new Config("textMaxLineLen", 1024));

    ClusterHdfsSourceUpgrader clusterHdfsSourceUpgrader = new ClusterHdfsSourceUpgrader();
    clusterHdfsSourceUpgrader.upgrade("a", "b", "c", 3, 4, configs);

    Assert.assertEquals(10, configs.size());

    HashMap<String, Object> configValues = new HashMap<>();
    for (Config c : configs) {
      configValues.put(c.getName(), c.getValue());
    }

    Assert.assertTrue(configValues.containsKey("clusterHDFSConfigBean.dataFormat"));
    Assert.assertEquals(DataFormat.TEXT, configValues.get("clusterHDFSConfigBean.dataFormat"));

    Assert.assertTrue(configValues.containsKey("clusterHDFSConfigBean.hdfsUri"));
    Assert.assertEquals("MY_URI", configValues.get("clusterHDFSConfigBean.hdfsUri"));

    Assert.assertTrue(configValues.containsKey("clusterHDFSConfigBean.hdfsDirLocations"));
    Assert.assertEquals(null, configValues.get("clusterHDFSConfigBean.hdfsDirLocations"));

    Assert.assertTrue(configValues.containsKey("clusterHDFSConfigBean.recursive"));
    Assert.assertEquals(true, configValues.get("clusterHDFSConfigBean.recursive"));

    Assert.assertTrue(configValues.containsKey("clusterHDFSConfigBean.produceSingleRecordPerMessage"));
    Assert.assertEquals(false, configValues.get("clusterHDFSConfigBean.produceSingleRecordPerMessage"));

    Assert.assertTrue(configValues.containsKey("clusterHDFSConfigBean.hdfsKerberos"));
    Assert.assertEquals(true, configValues.get("clusterHDFSConfigBean.hdfsKerberos"));

    Assert.assertTrue(configValues.containsKey("clusterHDFSConfigBean.hdfsConfDir"));
    Assert.assertEquals("MY_DIR", configValues.get("clusterHDFSConfigBean.hdfsConfDir"));

    Assert.assertTrue(configValues.containsKey("clusterHDFSConfigBean.hdfsUser"));
    Assert.assertEquals("MY_USER", configValues.get("clusterHDFSConfigBean.hdfsUser"));

    Assert.assertTrue(configValues.containsKey("clusterHDFSConfigBean.hdfsConfigs"));
    Assert.assertEquals(null, configValues.get("clusterHDFSConfigBean.hdfsConfigs"));

    Assert.assertTrue(configValues.containsKey("clusterHDFSConfigBean.dataFormatConfig.textMaxLineLen"));
    Assert.assertEquals(1024, configValues.get("clusterHDFSConfigBean.dataFormatConfig.textMaxLineLen"));
  }

  @Test
  public void testUpgradeV5toV6() throws StageException {
    List<Config> configs = new ArrayList<>();

    ClusterHdfsSourceUpgrader clusterHdfsSourceUpgrader = new ClusterHdfsSourceUpgrader();
    clusterHdfsSourceUpgrader.upgrade("a", "b", "c", 5, 6, configs);

    Assert.assertEquals(2, configs.size());
    Assert.assertEquals("clusterHDFSConfigBean.awsAccessKey", configs.get(0).getName());
    Assert.assertEquals("", configs.get(0).getValue());
    Assert.assertEquals("clusterHDFSConfigBean.awsSecretKey", configs.get(1).getName());
    Assert.assertEquals("", configs.get(1).getValue());
  }

}
