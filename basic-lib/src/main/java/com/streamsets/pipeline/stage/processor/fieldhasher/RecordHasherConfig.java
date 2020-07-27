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
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.ValueChooserModel;

public class RecordHasherConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue="false",
      label = "Hash Entire Record",
      description = "",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "RECORD_HASHING"
  )
  public boolean hashEntireRecord;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Include Record Header",
      description = "Include the record header for hashing",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependsOn = "hashEntireRecord",
      triggeredByValue = "true",
      group = "RECORD_HASHING"
  )
  public boolean includeRecordHeaderForHashing;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Use Field Separator",
      description = "Separator character to insert between fields before hashing",
      displayPosition = 25,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependsOn = "hashEntireRecord",
      triggeredByValue = "true",
      group = "RECORD_HASHING"
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
      displayPosition = 28,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "RECORD_HASHING"
  )
  public Character separatorCharacter = '\u0000';

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="MD5",
      label = "Hash Type",
      description = "",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependsOn = "hashEntireRecord",
      triggeredByValue = "true",
      group = "RECORD_HASHING"
  )
  @ValueChooserModel(HashTypeChooserValues.class)
  public HashType hashType;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "Target Field",
      description = "String field to store the hashed value. Creates the field if it does not exist.",
      group = "RECORD_HASHING",
      dependsOn = "hashEntireRecord",
      triggeredByValue = "true",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  @FieldSelectorModel(singleValued = true)
  public String targetField;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Header Attribute",
      description = "Header attribute to store the hashed value. Creates the attribute if it does not exist.",
      group = "RECORD_HASHING",
      dependsOn = "hashEntireRecord",
      triggeredByValue = "true",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String headerAttribute;

}
