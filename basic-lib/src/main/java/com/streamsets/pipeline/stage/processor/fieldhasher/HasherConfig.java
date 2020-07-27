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
package com.streamsets.pipeline.stage.processor.fieldhasher;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ListBeanModel;

import java.util.List;

public class HasherConfig {

  @ConfigDefBean(groups = {"RECORD_HASHING"})
  public RecordHasherConfig recordHasherConfig;


  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Use Field Separator",
      description = "Separator character to insert between fields before hashing",
      displayPosition = 5,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "FIELD_HASHING"
  )
  public boolean useSeparator;

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.CHARACTER,
          defaultValue = "\u0000",
          dependsOn = "useSeparator",
          triggeredByValue = "true",
          label = "Field Separator",
          description = "Character to separate fields",
          displayPosition = 8,
          displayMode = ConfigDef.DisplayMode.BASIC,
          group = "FIELD_HASHING"
  )
  public Character separatorCharacter = '\u0000';

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "Hash in Place",
      description="Replaces data in the specified fields with the hashed values",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "FIELD_HASHING"
  )
  @ListBeanModel
  public List<FieldHasherConfig> inPlaceFieldHasherConfigs;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "Hash to Target",
      description="Hashes the specified fields and writes to a target field or header attribute." +
          " Multiple fields are hashed together.",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "FIELD_HASHING"
  )
  @ListBeanModel
  public List<TargetFieldHasherConfig> targetFieldHasherConfigs;

}
