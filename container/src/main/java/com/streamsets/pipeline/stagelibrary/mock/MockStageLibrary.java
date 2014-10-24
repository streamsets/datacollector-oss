/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.stagelibrary.mock;

import com.streamsets.pipeline.config.ConfigOption;
import com.streamsets.pipeline.config.ConfigType;
import com.streamsets.pipeline.config.ModuleType;
import com.streamsets.pipeline.config.StaticModuleConfiguration;
import com.streamsets.pipeline.stagelibrary.StageLibrary;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

public class MockStageLibrary implements StageLibrary {
  private List<StaticModuleConfiguration> stages;

  @Inject
  public MockStageLibrary() {
    stages = new ArrayList<StaticModuleConfiguration>();
    List<ConfigOption> sourceConfigOption = new ArrayList<ConfigOption>();
    StaticModuleConfiguration sourceConfig =
        new StaticModuleConfiguration("CsvSource",
                                      "1.0", "csv_source",
                                      "This is a CSV Source. This CSV source produces records in the a comma separated format.",
                                      ModuleType.SOURCE,
                                      sourceConfigOption);

    ConfigOption fileLocationOption = new ConfigOption(
        "fileLocation",
        ConfigType.TEXT,
        "file_location",
        "This is the location of the file from which the data must be read",
        "/etc/data",
        true,
        "FileOptions"
    );

    ConfigOption bufferSizeOption = new ConfigOption(
        "bufferSize",
        ConfigType.NUMBER,
        "buffer_size",
        "This is the number of bytes that must be read in one go",
        "10000",
        true,
        "FileOptions"
    );

    sourceConfigOption.add(fileLocationOption);
    sourceConfigOption.add(bufferSizeOption);
    stages.add(sourceConfig);

    List<ConfigOption> processorConfigOption = new ArrayList<ConfigOption>();
    StaticModuleConfiguration processorConfig =
        new StaticModuleConfiguration("MaskingProcessor",
                                      "1.0", "masking_processor",
                                      "This is a masking processor.",
                                      ModuleType.PROCESSOR,
                                      processorConfigOption);

    ConfigOption processorOption = new ConfigOption(
        "mask",
        ConfigType.TEXT,
        "masking_processor",
        "This is the character used to mask",
        "*",
        true,
        "Mask"
    );

    processorConfigOption.add(processorOption);
    stages.add(processorConfig);

    List<ConfigOption> targetConfigOption = new ArrayList<ConfigOption>();
    StaticModuleConfiguration targetConfig =
        new StaticModuleConfiguration("KafkaTarget",
                                      "1.0", "kafka_target",
                                      "This is a kafka target. This target writes to kafka cluster.",
                                      ModuleType.TARGET,
                                      targetConfigOption);

    ConfigOption topicOption = new ConfigOption(
        "kafkaTopic",
        ConfigType.TEXT,
        "kafka_topic",
        "This is the kafka topic to which the data must be written",
        "myTopic",
        true,
        "Kafka"
    );

    ConfigOption hostOption = new ConfigOption(
        "kafkaHost",
        ConfigType.TEXT,
        "kafka_host",
        "This is the host on which the kafka cluster is installed.",
        "localhost",
        true,
        "Kafka"
    );

    targetConfigOption.add(topicOption);
    targetConfigOption.add(hostOption);
    stages.add(targetConfig);

  }

  @Override public List<StaticModuleConfiguration> getStages() {
    return stages;
  }

}
