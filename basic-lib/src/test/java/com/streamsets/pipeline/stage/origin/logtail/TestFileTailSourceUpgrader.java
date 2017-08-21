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
package com.streamsets.pipeline.stage.origin.logtail;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.LogMode;
import com.streamsets.pipeline.config.PostProcessingOptions;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TestFileTailSourceUpgrader {

  @Test
  public void testUpgradeV1toV3() throws StageException {
    List<Config> configs = new ArrayList<>();
    configs.add(new Config("dataFormat", DataFormat.LOG));
    configs.add(new Config("multiLineMainPattern", ""));
    configs.add(new Config("maxLineLength", 1024));
    configs.add(new Config("batchSize", 1000));
    configs.add(new Config("maxWaitTimeSecs", 5));
    configs.add(new Config("fileInfos", null));
    configs.add(new Config("postProcessing", PostProcessingOptions.NONE));
    configs.add(new Config("archiveDir", null));
    configs.add(new Config("charset", "UTF-8"));
    configs.add(new Config("removeCtrlChars", false));
    configs.add(new Config("logMode", LogMode.COMMON_LOG_FORMAT));
    configs.add(new Config("retainOriginalLine", true));
    configs.add(new Config("customLogFormat", null));
    configs.add(new Config("regex", null));
    configs.add(new Config("fieldPathsToGroupName", null));
    configs.add(new Config("grokPatternDefinition", null));
    configs.add(new Config("grokPattern", null));
    configs.add(new Config("enableLog4jCustomLogFormat", false));
    configs.add(new Config("log4jCustomLogFormat", null));

    FileTailSourceUpgrader fileTailSourceUpgrader = new FileTailSourceUpgrader();
    fileTailSourceUpgrader.upgrade("a", "b", "c", 1, 3, configs);

    // maxLineLength is converted to 3 configs: textMaxLineLen, jsonMaxObjectLen, logMaxObjectLen.
    // allowLateDirectory is added.
    Assert.assertEquals(19 - 1 + 3 + 1, configs.size());

    HashMap<String, Object> configValues = new HashMap<>();
    for (Config c : configs) {
      configValues.put(c.getName(), c.getValue());
    }

    Assert.assertTrue(configValues.containsKey("conf.dataFormat"));
    Assert.assertEquals(DataFormat.LOG, configValues.get("conf.dataFormat"));

    Assert.assertTrue(configValues.containsKey("conf.multiLineMainPattern"));
    Assert.assertEquals("", configValues.get("conf.multiLineMainPattern"));

    Assert.assertTrue(configValues.containsKey("conf.dataFormatConfig.logMaxObjectLen"));
    Assert.assertEquals(1024, configValues.get("conf.dataFormatConfig.logMaxObjectLen"));

    Assert.assertTrue(configValues.containsKey("conf.batchSize"));
    Assert.assertEquals(1000, configValues.get("conf.batchSize"));

    Assert.assertTrue(configValues.containsKey("conf.maxWaitTimeSecs"));
    Assert.assertEquals(5, configValues.get("conf.maxWaitTimeSecs"));

    Assert.assertTrue(configValues.containsKey("conf.fileInfos"));
    Assert.assertEquals(null, configValues.get("conf.fileInfos"));

    Assert.assertTrue(configValues.containsKey("conf.postProcessing"));
    Assert.assertEquals(PostProcessingOptions.NONE, configValues.get("conf.postProcessing"));

    Assert.assertTrue(configValues.containsKey("conf.archiveDir"));
    Assert.assertEquals(null, configValues.get("conf.archiveDir"));

    Assert.assertTrue(configValues.containsKey("conf.dataFormatConfig.charset"));
    Assert.assertEquals("UTF-8", configValues.get("conf.dataFormatConfig.charset"));

    Assert.assertTrue(configValues.containsKey("conf.dataFormatConfig.removeCtrlChars"));
    Assert.assertEquals(false, configValues.get("conf.dataFormatConfig.removeCtrlChars"));

    Assert.assertTrue(configValues.containsKey("conf.dataFormatConfig.logMode"));
    Assert.assertEquals(LogMode.COMMON_LOG_FORMAT, configValues.get("conf.dataFormatConfig.logMode"));

    Assert.assertTrue(configValues.containsKey("conf.dataFormatConfig.retainOriginalLine"));
    Assert.assertEquals(true, configValues.get("conf.dataFormatConfig.retainOriginalLine"));

    Assert.assertTrue(configValues.containsKey("conf.dataFormatConfig.customLogFormat"));
    Assert.assertEquals(null, configValues.get("conf.dataFormatConfig.customLogFormat"));

    Assert.assertTrue(configValues.containsKey("conf.dataFormatConfig.regex"));
    Assert.assertEquals(null, configValues.get("conf.dataFormatConfig.regex"));

    Assert.assertTrue(configValues.containsKey("conf.dataFormatConfig.fieldPathsToGroupName"));
    Assert.assertEquals(null, configValues.get("conf.dataFormatConfig.fieldPathsToGroupName"));

    Assert.assertTrue(configValues.containsKey("conf.dataFormatConfig.grokPatternDefinition"));
    Assert.assertEquals(null, configValues.get("conf.dataFormatConfig.grokPatternDefinition"));

    Assert.assertTrue(configValues.containsKey("conf.dataFormatConfig.grokPattern"));
    Assert.assertEquals(null, configValues.get("conf.dataFormatConfig.grokPattern"));

    Assert.assertTrue(configValues.containsKey("conf.dataFormatConfig.enableLog4jCustomLogFormat"));
    Assert.assertEquals(false, configValues.get("conf.dataFormatConfig.enableLog4jCustomLogFormat"));

    Assert.assertTrue(configValues.containsKey("conf.dataFormatConfig.log4jCustomLogFormat"));
    Assert.assertEquals(null, configValues.get("conf.dataFormatConfig.log4jCustomLogFormat"));

    Assert.assertTrue(configValues.containsKey("conf.allowLateDirectories"));
    Assert.assertEquals(false, configValues.get("conf.allowLateDirectories"));
  }

}
