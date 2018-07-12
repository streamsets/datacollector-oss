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
package com.streamsets.pipeline.stage.destination.hbase;

import com.google.common.io.Files;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestHBaseTarget {

  private static final String tableName = "TestHBaseSink";

  @Test(timeout = 60000)
  public void getGetHBaseConfiguration() {
    HBaseDTarget dTarget = new ForTestHBaseTarget();
    configure(dTarget);
    HBaseTarget target = (HBaseTarget) dTarget.createTarget();
    try {
      target.init(null,
          ContextInfoCreator.createTargetContext("n", false, OnRecordError.TO_ERROR));
      assertNotNull(target.getHBaseConfiguration());
    } finally {
      target.destroy();
    }
  }

  @Test(timeout = 60000)
  public void testHBaseConfigs() {
    HBaseDTarget dTarget = new ForTestHBaseTarget();
    configure(dTarget);
    HBaseTarget target = (HBaseTarget) dTarget.createTarget();
    try {
      target.init(null,
          ContextInfoCreator.createTargetContext("n", false, OnRecordError.TO_ERROR));
      assertEquals("X", target.getHBaseConfiguration().get("x"));
    } finally {
      target.destroy();
    }
  }

  static class ForTestHBaseTarget extends HBaseDTarget {
    @Override
    protected Target createTarget() {
      return new HBaseTarget(
          hBaseConnectionConfig,
          hbaseRowKey,
          rowKeyStorageType,
          hbaseFieldColumnMapping,
          implicitFieldMapping,
          ignoreMissingFieldPath,
          ignoreInvalidColumn,
          timeDriver
      ) {
        @Override
        public void write(Batch batch) {
        }
      };
    }
  }

  @Test
  public void testGetHBaseConfigurationWithResources() throws IOException {
    File resourcesDir = new File("target", UUID.randomUUID().toString());
    File fooDir = new File(resourcesDir, "foo");
    Assert.assertTrue(fooDir.mkdirs());
    Files.write("<configuration><property><name>xx</name><value>XX</value></property></configuration>",
        new File(fooDir, "hbase-site.xml"), StandardCharsets.UTF_8);
    HBaseDTarget dTarget = new ForTestHBaseTarget();
    configure(dTarget);
    dTarget.hBaseConnectionConfig.hbaseConfDir = fooDir.getName();
    HBaseTarget target = (HBaseTarget) dTarget.createTarget();
    try {
      target.init(null, ContextInfoCreator.createTargetContext(HBaseDTarget.class, "n", false,
          OnRecordError.TO_ERROR,
          resourcesDir.getAbsolutePath()));
      Configuration conf = target.getHBaseConfiguration();
      Assert.assertEquals("XX", conf.get("xx"));
    } finally {
      target.destroy();
    }

    // Provide hbaseConfDir as an absolute path
    File absoluteFilePath = new File(new File("target", UUID.randomUUID().toString()), "foo");
    Assert.assertTrue(absoluteFilePath.mkdirs());
    Files.write("<configuration><property><name>zz</name><value>ZZ</value></property></configuration>",
        new File(absoluteFilePath, "hbase-site.xml"), StandardCharsets.UTF_8);
    dTarget.hBaseConnectionConfig.hbaseConfDir = absoluteFilePath.getAbsolutePath();
    dTarget.timeDriver = "${time:now()}";
    target = (HBaseTarget) dTarget.createTarget();
    try {
      target.init(null, ContextInfoCreator.createTargetContext(HBaseDTarget.class, "n", false,
          OnRecordError.TO_ERROR,
          resourcesDir.getAbsolutePath()));
      Configuration conf = target.getHBaseConfiguration();
      Assert.assertEquals("ZZ", conf.get("zz"));
    } finally {
      target.destroy();
    }
  }

  private void configure(HBaseDTarget target) {
    target.hBaseConnectionConfig.zookeeperQuorum = "127.0.0.1";
    target.hBaseConnectionConfig.zookeeperParentZNode = "/hbase";
    target.hBaseConnectionConfig.tableName = tableName;
    target.hbaseRowKey = "[0]";
    target.rowKeyStorageType = StorageType.BINARY;
    target.hBaseConnectionConfig.hbaseConfigs = new HashMap<>();
    target.hBaseConnectionConfig.hbaseConfigs.put("x", "X");
    target.hbaseFieldColumnMapping = new ArrayList<>();
    target.hbaseFieldColumnMapping
        .add(new HBaseFieldMappingConfig("cf:a", "[1]", StorageType.TEXT));
    target.hBaseConnectionConfig.kerberosAuth = false;
    target.hBaseConnectionConfig.hbaseConfDir = "";
    target.hBaseConnectionConfig.hbaseUser = "";
    target.timeDriver = "${time:now()}";
  }

}
