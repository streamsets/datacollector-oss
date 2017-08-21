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
package com.streamsets.pipeline.stage.destination.flume;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TestFlumeTargetUpgrader {

  @Test
  public void testFlumeTargetUpgrader() throws StageException {

    List<Config> configs = new ArrayList<>();
    configs.add(new Config("flumeHostsConfig", new HashMap<String, String>().put("h1", "localhost:9001")));
    configs.add(new Config("clientType", ClientType.AVRO_FAILOVER));
    configs.add(new Config("backOff", true));
    configs.add(new Config("maxBackOff", 56));
    configs.add(new Config("hostSelectionStrategy", HostSelectionStrategy.RANDOM));
    configs.add(new Config("batchSize", 1000));
    configs.add(new Config("connectionTimeout", 5000));
    configs.add(new Config("requestTimeout", 5000));
    configs.add(new Config("maxRetryAttempts", 10));
    configs.add(new Config("waitBetweenRetries", 1000));
    configs.add(new Config("singleEventPerBatch", true));
    configs.add(new Config("dataFormat", DataFormat.DELIMITED));
    configs.add(new Config("charset", "UTF-8"));
    configs.add(new Config("csvFileFormat", CsvMode.EXCEL));
    configs.add(new Config("csvHeader", CsvHeader.WITH_HEADER));
    configs.add(new Config("csvReplaceNewLines", false));
    configs.add(new Config("jsonMode", JsonMode.ARRAY_OBJECTS));
    configs.add(new Config("textFieldPath", "/myField"));
    configs.add(new Config("textEmptyLineIfNull", true));
    configs.add(new Config("avroSchema", "hello!!"));
    configs.add(new Config("includeSchema", true));

    FlumeTargetUpgrader flumeTargetUpgrader = new FlumeTargetUpgrader();
    flumeTargetUpgrader.upgrade("a", "b", "c", 1, 2, configs);

    Assert.assertEquals(26, configs.size());

    HashMap<String, Object> configValues = new HashMap<>();
    for(Config c : configs) {
      configValues.put(c.getName(), c.getValue());
    }

    Assert.assertTrue(configValues.containsKey("flumeConfigBean.flumeConfig.flumeHostsConfig"));

    Assert.assertTrue(configValues.containsKey("flumeConfigBean.flumeConfig.clientType"));
    Assert.assertEquals(ClientType.AVRO_FAILOVER, configValues.get("flumeConfigBean.flumeConfig.clientType"));

    Assert.assertTrue(configValues.containsKey("flumeConfigBean.flumeConfig.backOff"));
    Assert.assertEquals(true, configValues.get("flumeConfigBean.flumeConfig.backOff"));

    Assert.assertTrue(configValues.containsKey("flumeConfigBean.flumeConfig.maxBackOff"));
    Assert.assertEquals(56, configValues.get("flumeConfigBean.flumeConfig.maxBackOff"));

    Assert.assertTrue(configValues.containsKey("flumeConfigBean.flumeConfig.hostSelectionStrategy"));
    Assert.assertEquals(HostSelectionStrategy.RANDOM, configValues.get("flumeConfigBean.flumeConfig.hostSelectionStrategy"));

    Assert.assertTrue(configValues.containsKey("flumeConfigBean.flumeConfig.batchSize"));
    Assert.assertEquals(1000, configValues.get("flumeConfigBean.flumeConfig.batchSize"));

    Assert.assertTrue(configValues.containsKey("flumeConfigBean.flumeConfig.connectionTimeout"));
    Assert.assertEquals(5000, configValues.get("flumeConfigBean.flumeConfig.connectionTimeout"));

    Assert.assertTrue(configValues.containsKey("flumeConfigBean.flumeConfig.requestTimeout"));
    Assert.assertEquals(5000, configValues.get("flumeConfigBean.flumeConfig.requestTimeout"));

    Assert.assertTrue(configValues.containsKey("flumeConfigBean.flumeConfig.maxRetryAttempts"));
    Assert.assertEquals(10, configValues.get("flumeConfigBean.flumeConfig.maxRetryAttempts"));

    Assert.assertTrue(configValues.containsKey("flumeConfigBean.flumeConfig.waitBetweenRetries"));
    Assert.assertEquals(1000, configValues.get("flumeConfigBean.flumeConfig.waitBetweenRetries"));

    Assert.assertTrue(configValues.containsKey("flumeConfigBean.flumeConfig.singleEventPerBatch"));
    Assert.assertEquals(true, configValues.get("flumeConfigBean.flumeConfig.singleEventPerBatch"));

    Assert.assertTrue(configValues.containsKey("flumeConfigBean.dataFormat"));
    Assert.assertEquals(DataFormat.DELIMITED, configValues.get("flumeConfigBean.dataFormat"));

    Assert.assertTrue(configValues.containsKey("flumeConfigBean.dataGeneratorFormatConfig.charset"));
    Assert.assertEquals("UTF-8", configValues.get("flumeConfigBean.dataGeneratorFormatConfig.charset"));

    Assert.assertTrue(configValues.containsKey("flumeConfigBean.dataGeneratorFormatConfig.csvFileFormat"));
    Assert.assertEquals(CsvMode.EXCEL, configValues.get("flumeConfigBean.dataGeneratorFormatConfig.csvFileFormat"));

    Assert.assertTrue(configValues.containsKey("flumeConfigBean.dataGeneratorFormatConfig.csvHeader"));
    Assert.assertEquals(CsvHeader.WITH_HEADER, configValues.get("flumeConfigBean.dataGeneratorFormatConfig.csvHeader"));

    Assert.assertTrue(configValues.containsKey("flumeConfigBean.dataGeneratorFormatConfig.csvReplaceNewLines"));
    Assert.assertEquals(false, configValues.get("flumeConfigBean.dataGeneratorFormatConfig.csvReplaceNewLines"));

    Assert.assertTrue(configValues.containsKey("flumeConfigBean.dataGeneratorFormatConfig.jsonMode"));
    Assert.assertEquals(JsonMode.ARRAY_OBJECTS, configValues.get("flumeConfigBean.dataGeneratorFormatConfig.jsonMode"));

    Assert.assertTrue(configValues.containsKey("flumeConfigBean.dataGeneratorFormatConfig.textFieldPath"));
    Assert.assertEquals("/myField", configValues.get("flumeConfigBean.dataGeneratorFormatConfig.textFieldPath"));

    Assert.assertTrue(configValues.containsKey("flumeConfigBean.dataGeneratorFormatConfig.textEmptyLineIfNull"));
    Assert.assertEquals(true, configValues.get("flumeConfigBean.dataGeneratorFormatConfig.textEmptyLineIfNull"));

    Assert.assertTrue(configValues.containsKey("flumeConfigBean.dataGeneratorFormatConfig.avroSchema"));
    Assert.assertEquals("hello!!", configValues.get("flumeConfigBean.dataGeneratorFormatConfig.avroSchema"));

    Assert.assertTrue(configValues.containsKey("flumeConfigBean.dataGeneratorFormatConfig.includeSchema"));
    Assert.assertEquals(true, configValues.get("flumeConfigBean.dataGeneratorFormatConfig.includeSchema"));

    //newly added configs

    Assert.assertTrue(configValues.containsKey("flumeConfigBean.dataGeneratorFormatConfig.csvCustomDelimiter"));
    Assert.assertEquals('|', configValues.get("flumeConfigBean.dataGeneratorFormatConfig.csvCustomDelimiter"));

    Assert.assertTrue(configValues.containsKey("flumeConfigBean.dataGeneratorFormatConfig.csvCustomEscape"));
    Assert.assertEquals('\\', configValues.get("flumeConfigBean.dataGeneratorFormatConfig.csvCustomEscape"));

    Assert.assertTrue(configValues.containsKey("flumeConfigBean.dataGeneratorFormatConfig.csvCustomQuote"));
    Assert.assertEquals('\"', configValues.get("flumeConfigBean.dataGeneratorFormatConfig.csvCustomQuote"));

    Assert.assertTrue(configValues.containsKey("flumeConfigBean.dataGeneratorFormatConfig.binaryFieldPath"));
    Assert.assertEquals("/", configValues.get("flumeConfigBean.dataGeneratorFormatConfig.binaryFieldPath"));

    Assert.assertEquals("NULL", configValues.get("flumeConfigBean.dataGeneratorFormatConfig.avroCompression"));
  }
}
