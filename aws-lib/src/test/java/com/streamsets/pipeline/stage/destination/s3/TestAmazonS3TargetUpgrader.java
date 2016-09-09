/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.destination.s3;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.JsonMode;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TestAmazonS3TargetUpgrader {

  @Test
  public void testAmazonS3TargetUpgrader() throws StageException {

    List<Config> configs = new ArrayList<>();
    configs.add(new Config("s3TargetConfigBean.charset", "UTF-8"));
    configs.add(new Config("s3TargetConfigBean.csvFileFormat", CsvMode.EXCEL));
    configs.add(new Config("s3TargetConfigBean.csvHeader", CsvHeader.WITH_HEADER));
    configs.add(new Config("s3TargetConfigBean.csvReplaceNewLines", false));
    configs.add(new Config("s3TargetConfigBean.jsonMode", JsonMode.ARRAY_OBJECTS));
    configs.add(new Config("s3TargetConfigBean.textFieldPath", "/myField"));
    configs.add(new Config("s3TargetConfigBean.textEmptyLineIfNull", true));
    configs.add(new Config("s3TargetConfigBean.avroSchema", "hello!!"));
    configs.add(new Config("s3TargetConfigBean.binaryFieldPath", "/binaryField"));

    AmazonS3TargetUpgrader amazonS3TargetUpgrader = new AmazonS3TargetUpgrader();
    amazonS3TargetUpgrader.upgrade("a", "b", "c", 1, 8, configs);

    Assert.assertEquals(21, configs.size());

    HashMap<String, Object> configValues = new HashMap<>();
    for (Config c : configs) {
      configValues.put(c.getName(), c.getValue());
    }

    Assert.assertEquals("UTF-8", configValues.get("s3TargetConfigBean.dataGeneratorFormatConfig.charset"));
    Assert.assertEquals(CsvMode.EXCEL, configValues.get("s3TargetConfigBean.dataGeneratorFormatConfig.csvFileFormat"));
    Assert.assertEquals(CsvHeader.WITH_HEADER, configValues.get("s3TargetConfigBean.dataGeneratorFormatConfig.csvHeader"));
    Assert.assertEquals(false, configValues.get("s3TargetConfigBean.dataGeneratorFormatConfig.csvReplaceNewLines"));
    Assert.assertEquals(JsonMode.ARRAY_OBJECTS, configValues.get("s3TargetConfigBean.dataGeneratorFormatConfig.jsonMode"));
    Assert.assertEquals("/myField", configValues.get("s3TargetConfigBean.dataGeneratorFormatConfig.textFieldPath"));
    Assert.assertEquals(true, configValues.get("s3TargetConfigBean.dataGeneratorFormatConfig.textEmptyLineIfNull"));
    Assert.assertEquals("hello!!", configValues.get("s3TargetConfigBean.dataGeneratorFormatConfig.avroSchema"));
    Assert.assertEquals("/binaryField", configValues.get("s3TargetConfigBean.dataGeneratorFormatConfig.binaryFieldPath"));

    //newly added configs

    Assert.assertEquals('|', configValues.get("s3TargetConfigBean.dataGeneratorFormatConfig.csvCustomDelimiter"));
    Assert.assertEquals('\\', configValues.get("s3TargetConfigBean.dataGeneratorFormatConfig.csvCustomEscape"));
    Assert.assertEquals('\"', configValues.get("s3TargetConfigBean.dataGeneratorFormatConfig.csvCustomQuote"));

    Assert.assertEquals("NULL", configValues.get("s3TargetConfigBean.dataGeneratorFormatConfig.avroCompression"));

    Assert.assertEquals("", configValues.get("s3TargetConfigBean.partitionTemplate"));

    Assert.assertEquals("false", configValues.get("s3TargetConfigBean.sseConfig.useSSE"));

    Assert.assertEquals("UTC", configValues.get("s3TargetConfigBean.timeZoneID"));
    Assert.assertEquals("${time:now()}", configValues.get("s3TargetConfigBean.timeDriverTemplate"));

    Assert.assertEquals("10", configValues.get("s3TargetConfigBean.tmConfig.threadPoolSize"));
    Assert.assertEquals("5242880", configValues.get("s3TargetConfigBean.tmConfig.minimumUploadPartSize"));
    Assert.assertEquals("268435456", configValues.get("s3TargetConfigBean.tmConfig.multipartUploadThreshold"));

    Assert.assertEquals("", configValues.get("s3TargetConfigBean.s3Config.endpoint"));

    //renamed configs

    configs = new ArrayList<>();
    configs.add(new Config("s3TargetConfigBean.s3Config.accessKeyId", "MY_KEY_ID"));
    configs.add(new Config("s3TargetConfigBean.s3Config.secretAccessKey", "MY_ACCESS_KEY"));
    configs.add(new Config("s3TargetConfigBean.s3Config.folder", "MY_COMMON_PREFIX"));

    configs.add(new Config("s3TargetConfigBean.advancedConfig.useProxy", "false"));

    amazonS3TargetUpgrader.upgrade("a", "b", "c", 1, 7, configs);

    configValues = new HashMap<>();
    for (Config c : configs) {
      configValues.put(c.getName(), c.getValue());
    }

    Assert.assertEquals("MY_KEY_ID", configValues.get("s3TargetConfigBean.s3Config.awsConfig.awsAccessKeyId"));
    Assert.assertEquals("MY_ACCESS_KEY", configValues.get("s3TargetConfigBean.s3Config.awsConfig.awsSecretAccessKey"));
    Assert.assertEquals("MY_COMMON_PREFIX", configValues.get("s3TargetConfigBean.s3Config.commonPrefix"));

    Assert.assertEquals("false", configValues.get("s3TargetConfigBean.proxyConfig.useProxy"));
  }
}
