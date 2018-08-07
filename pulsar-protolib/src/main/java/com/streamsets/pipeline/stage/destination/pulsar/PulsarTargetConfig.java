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

package com.streamsets.pipeline.stage.destination.pulsar;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.pulsar.config.BasePulsarConfig;
import org.apache.pulsar.client.api.HashingScheme;

import java.util.HashMap;
import java.util.Map;

public class PulsarTargetConfig extends BasePulsarConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Topic",
      description = "Pulsar destination topic. It can be just a topic name or a full destination path like" +
          "non-persistent://tenant/namespace/topic",
      displayPosition = 20,
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      elDefs = RecordEL.class,
      group = "PULSAR"
  )
  public String destinationTopic;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "SINGLE",
      label = "Partition Type",
      description = "Specify the Pulsar partition type to be used when writing messages to selected topic",
      displayPosition = 10,
      group = "ADVANCED"
  )
  @ValueChooserModel(PulsarPartitionTypeChooserValues.class)
  public PulsarPartitionType partitionType;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "JAVA_STRING_HASH",
      label = "Hashing Scheme",
      description = "Specify the Pulsar hashing scheme to be used when selecting the partition where to write messages",
      displayPosition = 20,
      group = "ADVANCED"
  )
  @ValueChooserModel(PulsarHashingSchemeChooserValues.class)
  public PulsarHashingScheme hashingScheme;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Message Key",
      description = "Pulsar Message Key that will be used when computing the hash to select the partition where to " +
          "write the corresponding message. Expressions like ${record:value(\"/<field name>\")} are accepted in this " +
          "field",
      displayPosition = 30,
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      elDefs = RecordEL.class,
      group = "ADVANCED"
  )
  public String messageKey;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "NONE",
      label = "Compression type",
      description = "Type of compression to be applied to Pulsar messages",
      displayPosition = 40,
      group = "ADVANCED"
  )
  @ValueChooserModel(PulsarCompressionTypeChooserValues.class)
  public PulsarCompressionType compressionType;

}
