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
    amazonS3TargetUpgrader.upgrade("a", "b", "c", 1, 2, configs);

    Assert.assertEquals(12, configs.size());

    HashMap<String, Object> configValues = new HashMap<>();
    for(Config c : configs) {
      configValues.put(c.getName(), c.getValue());
    }

    Assert.assertTrue(configValues.containsKey("s3TargetConfigBean.dataGeneratorFormatConfig.charset"));
    Assert.assertEquals("UTF-8", configValues.get("s3TargetConfigBean.dataGeneratorFormatConfig.charset"));

    Assert.assertTrue(configValues.containsKey("s3TargetConfigBean.dataGeneratorFormatConfig.csvFileFormat"));
    Assert.assertEquals(CsvMode.EXCEL, configValues.get("s3TargetConfigBean.dataGeneratorFormatConfig.csvFileFormat"));

    Assert.assertTrue(configValues.containsKey("s3TargetConfigBean.dataGeneratorFormatConfig.csvHeader"));
    Assert.assertEquals(CsvHeader.WITH_HEADER, configValues.get("s3TargetConfigBean.dataGeneratorFormatConfig.csvHeader"));

    Assert.assertTrue(configValues.containsKey("s3TargetConfigBean.dataGeneratorFormatConfig.csvReplaceNewLines"));
    Assert.assertEquals(false, configValues.get("s3TargetConfigBean.dataGeneratorFormatConfig.csvReplaceNewLines"));

    Assert.assertTrue(configValues.containsKey("s3TargetConfigBean.dataGeneratorFormatConfig.jsonMode"));
    Assert.assertEquals(JsonMode.ARRAY_OBJECTS, configValues.get("s3TargetConfigBean.dataGeneratorFormatConfig.jsonMode"));

    Assert.assertTrue(configValues.containsKey("s3TargetConfigBean.dataGeneratorFormatConfig.textFieldPath"));
    Assert.assertEquals("/myField", configValues.get("s3TargetConfigBean.dataGeneratorFormatConfig.textFieldPath"));

    Assert.assertTrue(configValues.containsKey("s3TargetConfigBean.dataGeneratorFormatConfig.textEmptyLineIfNull"));
    Assert.assertEquals(true, configValues.get("s3TargetConfigBean.dataGeneratorFormatConfig.textEmptyLineIfNull"));

    Assert.assertTrue(configValues.containsKey("s3TargetConfigBean.dataGeneratorFormatConfig.avroSchema"));
    Assert.assertEquals("hello!!", configValues.get("s3TargetConfigBean.dataGeneratorFormatConfig.avroSchema"));

    Assert.assertTrue(configValues.containsKey("s3TargetConfigBean.dataGeneratorFormatConfig.binaryFieldPath"));
    Assert.assertEquals("/binaryField", configValues.get("s3TargetConfigBean.dataGeneratorFormatConfig.binaryFieldPath"));

    //newly added configs

    Assert.assertTrue(configValues.containsKey("s3TargetConfigBean.dataGeneratorFormatConfig.csvCustomDelimiter"));
    Assert.assertEquals('|', configValues.get("s3TargetConfigBean.dataGeneratorFormatConfig.csvCustomDelimiter"));

    Assert.assertTrue(configValues.containsKey("s3TargetConfigBean.dataGeneratorFormatConfig.csvCustomEscape"));
    Assert.assertEquals('\\', configValues.get("s3TargetConfigBean.dataGeneratorFormatConfig.csvCustomEscape"));

    Assert.assertTrue(configValues.containsKey("s3TargetConfigBean.dataGeneratorFormatConfig.csvCustomQuote"));
    Assert.assertEquals('\"', configValues.get("s3TargetConfigBean.dataGeneratorFormatConfig.csvCustomQuote"));

  }
}
