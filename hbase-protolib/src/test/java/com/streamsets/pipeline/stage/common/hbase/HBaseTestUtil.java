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
package com.streamsets.pipeline.stage.common.hbase;

import com.streamsets.testing.NetworkUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;

public class HBaseTestUtil {

  private HBaseTestUtil() {
  }

  public static Configuration getHBaseTestConfiguration() {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase");
    conf.set(HConstants.ZOOKEEPER_QUORUM, "127.0.0.1");
    conf.set(HConstants.MASTER_INFO_PORT, String.valueOf(NetworkUtils.getRandomPort()));
    conf.set("hadoop.proxyuser." + System.getProperty("user.name") + ".hosts", "*");
    conf.set("hadoop.proxyuser." + System.getProperty("user.name") + ".groups", "*");
    return conf;
  }
}
