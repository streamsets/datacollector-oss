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
package com.streamsets.pipeline.stage.destination.hdfs;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import com.streamsets.pipeline.upgrader.SelectorStageUpgrader;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestHdfsTargetUpgrader {

  @Test
  public void testHdfsTargetUpgrader() throws StageException {

    Map<String, String> hdfsConfigs = new HashMap<>();
    hdfsConfigs.put("x", "X");
    hdfsConfigs.put("y", "Y");

    List<Config> configs = new ArrayList<>();

    configs.add(new Config("hdfsUri", "file:///"));
    configs.add(new Config("hdfsUser", "ss"));
    configs.add(new Config("hdfsKerberos", false));
    configs.add(new Config("hdfsConfDir", "/tmp"));
    configs.add(new Config("hdfsConfigs", hdfsConfigs));
    configs.add(new Config("uniquePrefix", "foo"));
    configs.add(new Config("timeZoneID", "UTC"));
    configs.add(new Config("dirPathTemplate", "/${YY()}${MM()}${DD()}${hh()}${every(10, mm())}"));
    configs.add(new Config("fileType", HdfsFileType.TEXT));
    configs.add(new Config("keyEl", "${uuid()}"));
    configs.add(new Config("compression", CompressionMode.NONE));
    configs.add(new Config("seqFileCompressionType", HdfsSequenceFileCompressionType.BLOCK));
    configs.add(new Config("maxRecordsPerFile", 1));
    configs.add(new Config("maxFileSize", 1));
    configs.add(new Config("timeDriver", "${record:value('/')}"));
    configs.add(new Config("lateRecordsLimit", "${30 * MINUTES}"));
    configs.add(new Config("lateRecordsAction", LateRecordsAction.SEND_TO_LATE_RECORDS_FILE));
    configs.add(new Config("lateRecordsDirPathTemplate", "/tmp"));
    configs.add(new Config("charset", "UTF-8"));
    configs.add(new Config("dataFormat", DataFormat.TEXT));
    configs.add(new Config("csvFileFormat", CsvMode.EXCEL));
    configs.add(new Config("csvHeader", CsvHeader.WITH_HEADER));
    configs.add(new Config("csvReplaceNewLines", false));
    configs.add(new Config("jsonMode", JsonMode.ARRAY_OBJECTS));
    configs.add(new Config("textFieldPath", "/myField"));
    configs.add(new Config("textEmptyLineIfNull", true));
    configs.add(new Config("avroSchema", "hello!!"));
    configs.add(new Config("includeSchema", true));

    HdfsTargetUpgrader hdfsTargetUpgrader = new HdfsTargetUpgrader();
    hdfsTargetUpgrader.upgrade("a", "b", "c", 1, 3, configs);

    Assert.assertEquals(36, configs.size());

    HashMap<String, Object> configValues = new HashMap<>();
    for(Config c : configs) {
      configValues.put(c.getName(), c.getValue());
    }

    Assert.assertTrue(configValues.containsKey("hdfsTargetConfigBean.hdfsUri"));
    Assert.assertEquals("file:///", configValues.get("hdfsTargetConfigBean.hdfsUri"));

    Assert.assertTrue(configValues.containsKey("hdfsTargetConfigBean.hdfsUser"));
    Assert.assertEquals("ss", configValues.get("hdfsTargetConfigBean.hdfsUser"));

    Assert.assertTrue(configValues.containsKey("hdfsTargetConfigBean.hdfsKerberos"));
    Assert.assertEquals(false, configValues.get("hdfsTargetConfigBean.hdfsKerberos"));

    Assert.assertTrue(configValues.containsKey("hdfsTargetConfigBean.hdfsConfDir"));
    Assert.assertEquals("/tmp", configValues.get("hdfsTargetConfigBean.hdfsConfDir"));

    Assert.assertTrue(configValues.containsKey("hdfsTargetConfigBean.hdfsConfigs"));
    Assert.assertEquals(hdfsConfigs, configValues.get("hdfsTargetConfigBean.hdfsConfigs"));

    Assert.assertTrue(configValues.containsKey("hdfsTargetConfigBean.uniquePrefix"));
    Assert.assertEquals("foo", configValues.get("hdfsTargetConfigBean.uniquePrefix"));

    Assert.assertTrue(configValues.containsKey("hdfsTargetConfigBean.timeZoneID"));
    Assert.assertEquals("UTC", configValues.get("hdfsTargetConfigBean.timeZoneID"));

    Assert.assertTrue(configValues.containsKey("hdfsTargetConfigBean.dirPathTemplate"));
    Assert.assertEquals("/${YY()}${MM()}${DD()}${hh()}${every(10, mm())}",
      configValues.get("hdfsTargetConfigBean.dirPathTemplate"));

    Assert.assertTrue(configValues.containsKey("hdfsTargetConfigBean.fileType"));
    Assert.assertEquals(HdfsFileType.TEXT, configValues.get("hdfsTargetConfigBean.fileType"));

    Assert.assertTrue(configValues.containsKey("hdfsTargetConfigBean.keyEl"));
    Assert.assertEquals("${uuid()}", configValues.get("hdfsTargetConfigBean.keyEl"));

    Assert.assertTrue(configValues.containsKey("hdfsTargetConfigBean.compression"));
    Assert.assertEquals(CompressionMode.NONE, configValues.get("hdfsTargetConfigBean.compression"));

    Assert.assertTrue(configValues.containsKey("hdfsTargetConfigBean.seqFileCompressionType"));
    Assert.assertEquals(HdfsSequenceFileCompressionType.BLOCK,
      configValues.get("hdfsTargetConfigBean.seqFileCompressionType"));

    Assert.assertTrue(configValues.containsKey("hdfsTargetConfigBean.maxRecordsPerFile"));
    Assert.assertEquals(1, configValues.get("hdfsTargetConfigBean.maxRecordsPerFile"));

    Assert.assertTrue(configValues.containsKey("hdfsTargetConfigBean.maxFileSize"));
    Assert.assertEquals(1, configValues.get("hdfsTargetConfigBean.maxFileSize"));

    Assert.assertTrue(configValues.containsKey("hdfsTargetConfigBean.timeDriver"));
    Assert.assertEquals("${record:value('/')}", configValues.get("hdfsTargetConfigBean.timeDriver"));

    Assert.assertTrue(configValues.containsKey("hdfsTargetConfigBean.lateRecordsLimit"));
    Assert.assertEquals("${30 * MINUTES}", configValues.get("hdfsTargetConfigBean.lateRecordsLimit"));

    Assert.assertTrue(configValues.containsKey("hdfsTargetConfigBean.lateRecordsAction"));
    Assert.assertEquals(LateRecordsAction.SEND_TO_LATE_RECORDS_FILE,
      configValues.get("hdfsTargetConfigBean.lateRecordsAction"));

    Assert.assertTrue(configValues.containsKey("hdfsTargetConfigBean.lateRecordsDirPathTemplate"));
    Assert.assertEquals("/tmp", configValues.get("hdfsTargetConfigBean.lateRecordsDirPathTemplate"));

    Assert.assertTrue(configValues.containsKey("hdfsTargetConfigBean.dataFormat"));
    Assert.assertEquals(DataFormat.TEXT, configValues.get("hdfsTargetConfigBean.dataFormat"));

    Assert.assertTrue(configValues.containsKey("hdfsTargetConfigBean.dataGeneratorFormatConfig.charset"));
    Assert.assertEquals("UTF-8", configValues.get("hdfsTargetConfigBean.dataGeneratorFormatConfig.charset"));

    Assert.assertTrue(configValues.containsKey("hdfsTargetConfigBean.dataGeneratorFormatConfig.csvFileFormat"));
    Assert.assertEquals(CsvMode.EXCEL,
      configValues.get("hdfsTargetConfigBean.dataGeneratorFormatConfig.csvFileFormat"));

    Assert.assertTrue(configValues.containsKey("hdfsTargetConfigBean.dataGeneratorFormatConfig.csvHeader"));
    Assert.assertEquals(CsvHeader.WITH_HEADER,
      configValues.get("hdfsTargetConfigBean.dataGeneratorFormatConfig.csvHeader"));

    Assert.assertTrue(configValues.containsKey("hdfsTargetConfigBean.dataGeneratorFormatConfig.csvReplaceNewLines"));
    Assert.assertEquals(false, configValues.get("hdfsTargetConfigBean.dataGeneratorFormatConfig.csvReplaceNewLines"));

    Assert.assertTrue(configValues.containsKey("hdfsTargetConfigBean.dataGeneratorFormatConfig.jsonMode"));
    Assert.assertEquals(JsonMode.ARRAY_OBJECTS,
      configValues.get("hdfsTargetConfigBean.dataGeneratorFormatConfig.jsonMode"));

    Assert.assertTrue(configValues.containsKey("hdfsTargetConfigBean.dataGeneratorFormatConfig.textFieldPath"));
    Assert.assertEquals("/myField", configValues.get("hdfsTargetConfigBean.dataGeneratorFormatConfig.textFieldPath"));

    Assert.assertTrue(configValues.containsKey("hdfsTargetConfigBean.dataGeneratorFormatConfig.textEmptyLineIfNull"));
    Assert.assertEquals(true, configValues.get("hdfsTargetConfigBean.dataGeneratorFormatConfig.textEmptyLineIfNull"));

    Assert.assertTrue(configValues.containsKey("hdfsTargetConfigBean.dataGeneratorFormatConfig.avroSchema"));
    Assert.assertEquals("hello!!", configValues.get("hdfsTargetConfigBean.dataGeneratorFormatConfig.avroSchema"));

    //newly added configs

    Assert.assertTrue(configValues.containsKey("hdfsTargetConfigBean.dataGeneratorFormatConfig.csvCustomDelimiter"));
    Assert.assertEquals('|', configValues.get("hdfsTargetConfigBean.dataGeneratorFormatConfig.csvCustomDelimiter"));

    Assert.assertTrue(configValues.containsKey("hdfsTargetConfigBean.dataGeneratorFormatConfig.csvCustomEscape"));
    Assert.assertEquals('\\', configValues.get("hdfsTargetConfigBean.dataGeneratorFormatConfig.csvCustomEscape"));

    Assert.assertTrue(configValues.containsKey("hdfsTargetConfigBean.dataGeneratorFormatConfig.csvCustomQuote"));
    Assert.assertEquals('\"', configValues.get("hdfsTargetConfigBean.dataGeneratorFormatConfig.csvCustomQuote"));

    Assert.assertEquals("NULL", configValues.get("hdfsTargetConfigBean.dataGeneratorFormatConfig.avroCompression"));

    // Version 3 new configs
    Assert.assertTrue(configValues.containsKey("hdfsTargetConfigBean.idleTimeout"));
    Assert.assertEquals("-1", configValues.get("hdfsTargetConfigBean.idleTimeout"));
  }

  @Test
  public void testUpgradeV2ToV3() throws StageException {
    List<Config> configs = new ArrayList<>();
    configs.add(new Config("hdfsTargetConfigBean.idleTimeout", "10"));

    HdfsTargetUpgrader hdfsTargetUpgrader = new HdfsTargetUpgrader();
    hdfsTargetUpgrader.upgrade("a", "b", "c", 2, 3, configs);

    Assert.assertEquals(4, configs.size());
    HashMap<String, Object> configValues = new HashMap<>();
    for(Config c : configs) {
      configValues.put(c.getName(), c.getValue());
    }

    Assert.assertTrue(configValues.containsKey("hdfsTargetConfigBean.idleTimeout"));
    Assert.assertEquals("10", configValues.get("hdfsTargetConfigBean.idleTimeout"));

    Assert.assertTrue(configValues.containsKey("hdfsTargetConfigBean.dirPathTemplateInHeader"));
    Assert.assertEquals(false, configValues.get("hdfsTargetConfigBean.dirPathTemplateInHeader"));

    Assert.assertTrue(configValues.containsKey("hdfsTargetConfigBean.rollIfHeader"));
    Assert.assertEquals(false, configValues.get("hdfsTargetConfigBean.rollIfHeader"));

    Assert.assertTrue(configValues.containsKey("hdfsTargetConfigBean.rollHeaderName"));
    Assert.assertEquals("roll", configValues.get("hdfsTargetConfigBean.rollHeaderName"));
  }

  @Test
  public void testUpgradeV4ToV5() throws StageException {
    List<Config> configs = new ArrayList<>();

    final URL yamlResource = ClassLoader.getSystemClassLoader().getResource("upgrader/HdfsDTarget.yaml");
    final SelectorStageUpgrader upgrader = new SelectorStageUpgrader(
        "stage",
        new HdfsTargetUpgrader(),
        yamlResource
    );

    StageUpgrader.Context context = Mockito.mock(StageUpgrader.Context.class);
    Mockito.doReturn(4).when(context).getFromVersion();
    Mockito.doReturn(5).when(context).getToVersion();

    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(
        configs,
        "hdfsTargetConfigBean.dataGeneratorFormatConfig.basicAuthUserInfo",
        ""
    );
    UpgraderTestUtils.assertExists(
        configs,
        "hdfsTargetConfigBean.dataGeneratorFormatConfig.basicAuthUserInfoForRegistration",
        ""
    );
  }
}
