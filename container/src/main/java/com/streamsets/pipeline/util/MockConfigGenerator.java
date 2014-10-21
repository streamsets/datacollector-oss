package com.streamsets.pipeline.util;

import com.streamsets.pipeline.config.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by harikiran on 10/20/14.
 */
public class MockConfigGenerator {

  public static RuntimePipelineConfiguration getRuntimePipelineConfiguration() {

    RuntimePipelineConfiguration r = new RuntimePipelineConfiguration();

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
    List<ConfigOption> sourceConfigOptions = new ArrayList<ConfigOption>();
    sourceConfigOptions.add(fileLocationOption);
    sourceConfigOptions.add(bufferSizeOption);
    List<String> inputLanes  = new ArrayList<String>();
    List<String> outputLanes = new ArrayList<String>();
    outputLanes.add("csv->mask");

    RuntimeModuleConfiguration sourceConfig = new RuntimeModuleConfiguration("myCsvSource",
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
      ConfigType.TEXT,
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

    RuntimeModuleConfiguration processorConfig = new RuntimeModuleConfiguration("myMaskingProcessor",
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

    List<ConfigOption> targetConfigOptions = new ArrayList<ConfigOption>();
    targetConfigOptions.add(topicOption);
    targetConfigOptions.add(hostOption);
    List<String> tInputLanes  = new ArrayList<String>();
    List<String> tOutputLanes = new ArrayList<String>();
    tInputLanes.add("mask->kafka");

    RuntimeModuleConfiguration targetConfig = new RuntimeModuleConfiguration("myKafkaTarget",
      "KafkaTarget",
      "1.0",
      "This is kafka target.",
      targetConfigOptions,
      900,
      900,
      tInputLanes,
      tOutputLanes);

    r.getRuntimeModuleConfigurations().add(targetConfig);

    return r;
  }

  public static ModuleRegistry getStaticModuleConfig() {
    ModuleRegistry moduleConfiguration = new ModuleRegistry();

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
    moduleConfiguration.getStaticModuleConfigurations().add(sourceConfig);

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
    moduleConfiguration.getStaticModuleConfigurations().add(processorConfig);

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
    moduleConfiguration.getStaticModuleConfigurations().add(targetConfig);

    return moduleConfiguration;
  }
}
