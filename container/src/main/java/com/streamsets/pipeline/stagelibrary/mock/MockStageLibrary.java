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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.agent.RuntimeInfo;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.config.ConfigDefinition;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.config.StageType;
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

  private final List<? extends ClassLoader> stageClassLoaders;
  private final List<StageDefinition> stages;
  private final ObjectMapper json;

  @Inject
  public MockStageLibrary(RuntimeInfo runtimeInfo) {
    stageClassLoaders = runtimeInfo.getStageLibraryClassLoaders();
    json = new ObjectMapper();
    json.enable(SerializationFeature.INDENT_OUTPUT);
    this.stages = loadStages();
  }

  @VisibleForTesting
  List<StageDefinition> loadStages() {
    List<StageDefinition> stages = new ArrayList<StageDefinition>();
    //go over all the "PipelineStages.json" files and collect stage information

    for (ClassLoader cl : stageClassLoaders) {
      Enumeration<URL> resources = null;
      try {
        resources = cl.getResources(PIPELINE_STAGES_JSON);
      } catch (IOException e) {
        //TODO<Hari>: Introduce a new  exception?
        e.printStackTrace();
      }

      if (!resources.hasMoreElements()) {
        //No PipelineStages.json file found
        //return mock stages
        populateDefaultStages(stages);
        return stages;
      }

      List<InputStream> inputStreams = new ArrayList<InputStream>();
      while (resources.hasMoreElements()) {
        try {
          inputStreams.add(resources.nextElement().openStream());
        } catch (IOException e) {
          //TODO<Hari>: Introduce a new  exception?
          e.printStackTrace();
        }
      }

      //get the StaticStageConfiguration objects from each of the streams
      for (InputStream in : inputStreams) {
        try {
          StageDefinition[] stageDefinitions = json.readValue(in,
            StageDefinition[].class);
          for(StageDefinition s : stageDefinitions) {
            stages.add(s);
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    return stages;
  }

  @Override
  public List<StageDefinition> getStages() {
    return stages;
  }

  public void populateDefaultStages(List<StageDefinition> stages) {
    List<ConfigDefinition> sourceConfigDefinition = new ArrayList<ConfigDefinition>();
    StageDefinition sourceConfig =
      new StageDefinition("CsvSource",
        "1.0", "csv_source",
        "This is a CSV Source. This CSV source produces records in the a comma separated format.",
        StageType.SOURCE,
        sourceConfigDefinition);

    ConfigDefinition fileLocationOption = new ConfigDefinition(
      "fileLocation",
      ConfigDef.Type.STRING,
      "file_location",
      "This is the location of the file from which the data must be read",
      "/etc/data",
      true,
      "FileOptions"
    );

    ConfigDefinition bufferSizeOption = new ConfigDefinition(
      "bufferSize",
      ConfigDef.Type.INTEGER,
      "buffer_size",
      "This is the number of bytes that must be read in one go",
      "10000",
      true,
      "FileOptions"
    );

    sourceConfigDefinition.add(fileLocationOption);
    sourceConfigDefinition.add(bufferSizeOption);
    stages.add(sourceConfig);

    List<ConfigDefinition> processorConfigDefinition = new ArrayList<ConfigDefinition>();
    StageDefinition processorConfig =
      new StageDefinition("MaskingProcessor",
        "1.0", "masking_processor",
        "This is a masking processor.",
        StageType.PROCESSOR,
        processorConfigDefinition);

    ConfigDefinition processorOption = new ConfigDefinition(
      "mask",
      ConfigDef.Type.STRING,
      "masking_processor",
      "This is the character used to mask",
      "*",
      true,
      "Mask"
    );

    processorConfigDefinition.add(processorOption);
    stages.add(processorConfig);

    List<ConfigDefinition> targetConfigDefinition = new ArrayList<ConfigDefinition>();
    StageDefinition targetConfig =
      new StageDefinition("KafkaTarget",
        "1.0", "kafka_target",
        "This is a kafka target. This target writes to kafka cluster.",
        StageType.TARGET,
        targetConfigDefinition);

    ConfigDefinition topicOption = new ConfigDefinition(
      "kafkaTopic",
      ConfigDef.Type.STRING,
      "kafka_topic",
      "This is the kafka topic to which the data must be written",
      "myTopic",
      true,
      "Kafka"
    );

    ConfigDefinition hostOption = new ConfigDefinition(
      "kafkaHost",
      ConfigDef.Type.STRING,
      "kafka_host",
      "This is the host on which the kafka cluster is installed.",
      "localhost",
      true,
      "Kafka"
    );

    targetConfigDefinition.add(topicOption);
    targetConfigDefinition.add(hostOption);
    stages.add(targetConfig);
  }

}
