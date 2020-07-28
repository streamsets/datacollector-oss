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
package com.streamsets.pipeline.stage.origin.opcua;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;

public class NodeIdConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Field Name",
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String field;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Identifier",
      defaultValue = "",
      description = "The identifier for a node in the address space of an OPC UA server",
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String identifier = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Identifier Type",
      description = "The format and data type of the identifier",
      defaultValue = "NUMERIC",
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  @ValueChooserModel(IdentifierTypeChooserValues.class)
  public IdentifierType identifierType = IdentifierType.NUMERIC;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "0",
      label = "Namespace Index",
      description = "The index an OPC UA server uses for a namespace URI",
      min = 0,
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public int namespaceIndex = 0;


  public String toString() {
    return "field:" + field +
        ", identifier:" + identifier +
        ", identifierType: " + identifierType +
        ",namespaceIndex: " + namespaceIndex;
  }
}
