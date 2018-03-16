/*
 * Copyright 2018 StreamSets Inc.
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

package com.streamsets.pipeline.stage.destination.mapreduce;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import com.streamsets.testing.pipeline.stage.TestUpgraderContext;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public class TestMapReduceExecutorUpgrader {

  @Test
  public void testUpgradeV1ToV2() throws StageException {
    final MapReduceExecutorUpgrader upgrader = new MapReduceExecutorUpgrader();

    final List<Config> configs = new LinkedList<>();
    configs.add(new Config("jobConfig.avroParquetConfig.dictionaryPageSize", 50));
    configs.add(new Config("jobConfig.avroParquetConfig.outputDirectory", "/output"));
    configs.add(new Config("jobConfig.avroParquetConfig.inputFile", "/input/something.avro"));
    configs.add(new Config("jobConfig.avroParquetConfig.keepInputFile", false));
    configs.add(new Config("jobConfig.avroParquetConfig.overwriteTmpFile", true));

    UpgraderTestUtils.UpgradeMoveWatcher watcher = UpgraderTestUtils.snapshot(configs);

    final TestUpgraderContext context = new TestUpgraderContext("l", "s", "i", 1, 2);

    upgrader.upgrade(configs, context);

    watcher.assertAllMoved(
        configs,
        "jobConfig.avroParquetConfig.outputDirectory",
        "jobConfig.avroConversionCommonConfig.outputDirectory",
        "jobConfig.avroParquetConfig.inputFile",
        "jobConfig.avroConversionCommonConfig.inputFile",
        "jobConfig.avroParquetConfig.keepInputFile",
        "jobConfig.avroConversionCommonConfig.keepInputFile",
        "jobConfig.avroParquetConfig.overwriteTmpFile",
        "jobConfig.avroConversionCommonConfig.overwriteTmpFile"
    );

  }
}
