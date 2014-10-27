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
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.config.StaticStageConfiguration;
import com.streamsets.pipeline.serde.StageConfigurationDeserializer;
import com.streamsets.pipeline.stagelibrary.StageLibrary;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

public class MockStageLibrary implements StageLibrary {

  private static final String PIPELINE_STAGES_JSON = "PipelineStages.json";

  private List<StaticStageConfiguration> stages = null;

  @Inject
  public MockStageLibrary() {
    this.stages = new ArrayList<StaticStageConfiguration>();
  }

  @Override
  public List<StaticStageConfiguration> getStages() {
    //go over all the "PipelineStages.json" files and collect stage information

    //may have to go over multiple class loaders eventually
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Enumeration<URL> resources = null;
    try {
      resources = cl.getResources(PIPELINE_STAGES_JSON);
    } catch (IOException e) {
      //TODO<Hari>: Introduce a new  exception?
      e.printStackTrace();
    }

    if(!resources.hasMoreElements()) {
      //No PipelineStages.json file found
      //return mock stages
      populateDefaultStages(stages);
      return stages;
    }

    List<InputStream> inputStreams = new ArrayList<InputStream>();
    while(resources.hasMoreElements()) {
      try {
        inputStreams.add(resources.nextElement().openStream());
      } catch (IOException e) {
        //TODO<Hari>: Introduce a new  exception?
        e.printStackTrace();
      }
    }

    //get the StaticStageConfiguration objects from each of the streams
    for(InputStream in : inputStreams) {
      try {
        stages.addAll(
          StageConfigurationDeserializer.deserialize(in).getStaticStageConfigurations());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return stages;
  }

  public void populateDefaultStages(List<StaticStageConfiguration> stages) {
    List<ConfigOption> sourceConfigOption = new ArrayList<ConfigOption>();
    StaticStageConfiguration sourceConfig =
      new StaticStageConfiguration("CsvSource",
        "1.0", "csv_source",
        "This is a CSV Source. This CSV source produces records in the a comma separated format.",
        StageType.SOURCE,
        sourceConfigOption);

    ConfigOption fileLocationOption = new ConfigOption(
      "fileLocation",
      ConfigType.STRING,
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
    StaticStageConfiguration processorConfig =
      new StaticStageConfiguration("MaskingProcessor",
        "1.0", "masking_processor",
        "This is a masking processor.",
        StageType.PROCESSOR,
        processorConfigOption);

    ConfigOption processorOption = new ConfigOption(
      "mask",
      ConfigType.STRING,
      "masking_processor",
      "This is the character used to mask",
      "*",
      true,
      "Mask"
    );

    processorConfigOption.add(processorOption);
    stages.add(processorConfig);

    List<ConfigOption> targetConfigOption = new ArrayList<ConfigOption>();
    StaticStageConfiguration targetConfig =
      new StaticStageConfiguration("KafkaTarget",
        "1.0", "kafka_target",
        "This is a kafka target. This target writes to kafka cluster.",
        StageType.TARGET,
        targetConfigOption);

    ConfigOption topicOption = new ConfigOption(
      "kafkaTopic",
      ConfigType.STRING,
      "kafka_topic",
      "This is the kafka topic to which the data must be written",
      "myTopic",
      true,
      "Kafka"
    );

    ConfigOption hostOption = new ConfigOption(
      "kafkaHost",
      ConfigType.STRING,
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

}
