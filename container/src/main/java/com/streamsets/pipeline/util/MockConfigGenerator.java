package com.streamsets.pipeline.util;

import com.streamsets.pipeline.config.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Created by harikiran on 10/20/14.
 */
public class MockConfigGenerator {

  public static RuntimePipelineConfiguration getRuntimePipelineConfiguration() {

    RuntimePipelineConfiguration r = new RuntimePipelineConfiguration();
    //set uuid
    r.setUuid(UUID.randomUUID().toString());

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
    List<ConfigOption> sourceConfigOptions = new ArrayList<ConfigOption>();
    sourceConfigOptions.add(fileLocationOption);
    sourceConfigOptions.add(bufferSizeOption);
    List<String> inputLanes  = new ArrayList<String>();
    List<String> outputLanes = new ArrayList<String>();
    inputLanes.add("csv->mask");
    outputLanes.add("csv->mask");

    RuntimeStageConfiguration sourceConfig = new RuntimeStageConfiguration("myCsvSource",
              "CSVSource",
              "1.0",
              "This is CSV Source [comma separated]",
              sourceConfigOptions,
              100,
              100,
              inputLanes,
              outputLanes);

    r.getRuntimeModuleConfigurations().add(sourceConfig);

    ConfigOption maskingOption = new ConfigOption(
      "mask",
      ConfigType.STRING,
      "mask",
      "This is the character used to mask the sensitive data.",
      "*",
      true,
      "MakingOption"
    );

    List<ConfigOption> processorConfigOptions = new ArrayList<ConfigOption>();
    processorConfigOptions.add(maskingOption);
    List<String> pInputLanes  = new ArrayList<String>();
    List<String> pOutputLanes = new ArrayList<String>();
    pInputLanes.add("csv->mask");
    pOutputLanes.add("mask->kafka");

    RuntimeStageConfiguration processorConfig = new RuntimeStageConfiguration("myMaskingProcessor",
      "MaskingProcessor",
      "1.0",
      "This is masking processor.",
      processorConfigOptions,
      500,
      500,
      pInputLanes,
      pOutputLanes);

    r.getRuntimeModuleConfigurations().add(processorConfig);

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

    List<ConfigOption> targetConfigOptions = new ArrayList<ConfigOption>();
    targetConfigOptions.add(topicOption);
    targetConfigOptions.add(hostOption);
    List<String> tInputLanes  = new ArrayList<String>();
    List<String> tOutputLanes = new ArrayList<String>();
    tInputLanes.add("mask->kafka");
    tInputLanes.add("csv->kafka");
    tOutputLanes.add("kafka->hdfs");

    RuntimeStageConfiguration targetConfig = new RuntimeStageConfiguration("myKafkaTarget",
      "KafkaTarget",
      "1.0",
      "This is kafka target.",
      targetConfigOptions,
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
    moduleConfiguration.getStaticStageConfigurations().add(sourceConfig);

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
    moduleConfiguration.getStaticStageConfigurations().add(processorConfig);

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
    moduleConfiguration.getStaticStageConfigurations().add(targetConfig);

    return moduleConfiguration;
  }
}
