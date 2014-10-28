package com.streamsets.pipeline.util;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDef.Type;
import com.streamsets.pipeline.config.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Created by harikiran on 10/20/14.
 */
public class MockConfigGenerator {

  public static PipelineConfiguration getRuntimePipelineConfiguration() {

    PipelineConfiguration r = new PipelineConfiguration();
    //set uuid
    r.setUuid(UUID.randomUUID().toString());

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
      Type.INTEGER,
      "buffer_size",
      "This is the number of bytes that must be read in one go",
      "10000",
      true,
      "FileOptions"
    );
    List<ConfigDefinition> sourceConfigDefinitions = new ArrayList<ConfigDefinition>();
    sourceConfigDefinitions.add(fileLocationOption);
    sourceConfigDefinitions.add(bufferSizeOption);
    List<String> inputLanes  = new ArrayList<String>();
    List<String> outputLanes = new ArrayList<String>();
    inputLanes.add("csv->mask");
    outputLanes.add("csv->mask");

    StageConfiguration sourceConfig = new StageConfiguration("myCsvSource",
              "CSVSource",
              "1.0",
              "This is CSV Source [comma separated]",
              sourceConfigDefinitions,
              100,
              100,
              inputLanes,
              outputLanes);

    r.getRuntimeModuleConfigurations().add(sourceConfig);

    ConfigDefinition maskingOption = new ConfigDefinition(
      "mask",
      Type.STRING,
      "mask",
      "This is the character used to mask the sensitive data.",
      "*",
      true,
      "MakingOption"
    );

    List<ConfigDefinition> processorConfigDefinitions = new ArrayList<ConfigDefinition>();
    processorConfigDefinitions.add(maskingOption);
    List<String> pInputLanes  = new ArrayList<String>();
    List<String> pOutputLanes = new ArrayList<String>();
    pInputLanes.add("csv->mask");
    pOutputLanes.add("mask->kafka");

    StageConfiguration processorConfig = new StageConfiguration("myMaskingProcessor",
      "MaskingProcessor",
      "1.0",
      "This is masking processor.",
      processorConfigDefinitions,
      500,
      500,
      pInputLanes,
      pOutputLanes);

    r.getRuntimeModuleConfigurations().add(processorConfig);

    ConfigDefinition topicOption = new ConfigDefinition(
      "kafkaTopic",
      Type.STRING,
      "kafka_topic",
      "This is the kafka topic to which the data must be written",
      "myTopic",
      true,
      "Kafka"
    );

    ConfigDefinition hostOption = new ConfigDefinition(
      "kafkaHost",
      Type.STRING,
      "kafka_host",
      "This is the host on which the kafka cluster is installed.",
      "localhost",
      true,
      "Kafka"
    );

    List<ConfigDefinition> targetConfigDefinitions = new ArrayList<ConfigDefinition>();
    targetConfigDefinitions.add(topicOption);
    targetConfigDefinitions.add(hostOption);
    List<String> tInputLanes  = new ArrayList<String>();
    List<String> tOutputLanes = new ArrayList<String>();
    tInputLanes.add("mask->kafka");
    tInputLanes.add("csv->kafka");
    tOutputLanes.add("kafka->hdfs");

    StageConfiguration targetConfig = new StageConfiguration("myKafkaTarget",
      "KafkaTarget",
      "1.0",
      "This is kafka target.",
      targetConfigDefinitions,
      900,
      900,
      tInputLanes,
      tOutputLanes);

    r.getRuntimeModuleConfigurations().add(targetConfig);

    //set errors
    List<String> sourceErrors = new ArrayList<String>();
    List<String> targetErrors = new ArrayList<String>();
    sourceErrors.add("Source cannot have input lanes");
    targetErrors.add("Target cannot have output lanes");
    targetErrors.add("Target topic does not exist");
    r.getErrorsMap().put("myCsvSource", sourceErrors);
    r.getErrorsMap().put("myKafkaTarget", targetErrors);

    return r;
  }

  public static StageRegistry getStaticModuleConfig() {
    StageRegistry moduleConfiguration = new StageRegistry();

    List<ConfigDefinition> sourceConfigDefinition = new ArrayList<ConfigDefinition>();
    StageDefinition sourceConfig =
      new StageDefinition("CsvSource",
        "1.0", "csv_source",
        "This is a CSV Source. This CSV source produces records in the a comma separated format.",
        StageType.SOURCE,
        sourceConfigDefinition);

    ConfigDefinition fileLocationOption = new ConfigDefinition(
      "fileLocation",
      Type.STRING,
      "file_location",
      "This is the location of the file from which the data must be read",
      "/etc/data",
      true,
      "FileOptions"
    );

    ConfigDefinition bufferSizeOption = new ConfigDefinition(
      "bufferSize",
      Type.INTEGER,
      "buffer_size",
      "This is the number of bytes that must be read in one go",
      "10000",
      true,
      "FileOptions"
    );

    sourceConfigDefinition.add(fileLocationOption);
    sourceConfigDefinition.add(bufferSizeOption);
    moduleConfiguration.getStageDefinitions().add(sourceConfig);

    List<ConfigDefinition> processorConfigDefinition = new ArrayList<ConfigDefinition>();
    StageDefinition processorConfig =
      new StageDefinition("MaskingProcessor",
        "1.0", "masking_processor",
        "This is a masking processor.",
        StageType.PROCESSOR,
        processorConfigDefinition);

    ConfigDefinition processorOption = new ConfigDefinition(
      "mask",
      Type.STRING,
      "masking_processor",
      "This is the character used to mask",
      "*",
      true,
      "Mask"
    );

    processorConfigDefinition.add(processorOption);
    moduleConfiguration.getStageDefinitions().add(processorConfig);

    List<ConfigDefinition> targetConfigDefinition = new ArrayList<ConfigDefinition>();
    StageDefinition targetConfig =
      new StageDefinition("KafkaTarget",
        "1.0", "kafka_target",
        "This is a kafka target. This target writes to kafka cluster.",
        StageType.TARGET,
        targetConfigDefinition);

    ConfigDefinition topicOption = new ConfigDefinition(
      "kafkaTopic",
      Type.STRING,
      "kafka_topic",
      "This is the kafka topic to which the data must be written",
      "myTopic",
      true,
      "Kafka"
    );

    ConfigDefinition hostOption = new ConfigDefinition(
      "kafkaHost",
      Type.STRING,
      "kafka_host",
      "This is the host on which the kafka cluster is installed.",
      "localhost",
      true,
      "Kafka"
    );

    targetConfigDefinition.add(topicOption);
    targetConfigDefinition.add(hostOption);
    moduleConfiguration.getStageDefinitions().add(targetConfig);

    return moduleConfiguration;
  }
}
