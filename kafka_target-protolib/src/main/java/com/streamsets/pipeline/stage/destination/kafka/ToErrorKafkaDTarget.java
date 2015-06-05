/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.kafka;


import com.streamsets.pipeline.api.ErrorStage;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfig;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.config.DataFormat;

@StageDef(
    version = "1.0.0",
    label = "Write to Kafka",
    description = "Writes records to Kafka as SDC Records",
    icon = "")
@ErrorStage
@HideConfig(preconditions = true, onErrorRecord = true, value = {"dataFormat", "charset"})
@GenerateResourceBundle
public class ToErrorKafkaDTarget extends KafkaDTarget {

  @Override
  protected Target createTarget() {
    return new KafkaTarget(metadataBrokerList, false, topic, null, null, partitionStrategy, partition, DataFormat.SDC_JSON, null,
                           singleMessagePerBatch, kafkaProducerConfigs, null, null, false, null,
                           null, false, null, false);
  }

}
