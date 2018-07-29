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
package com.streamsets.pipeline.stage.origin.maprjson;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.stage.origin.lib.BasicConfig;

public class MapRJsonOriginConfigBean {

  public static final String JSON_MAX_OBJECT_LENGTH = "jsonMaxObjectLen";

  @ConfigDefBean(groups = "MAPR_JSON_ORIGIN")
  public BasicConfig basic = new BasicConfig();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Table Name",
      description = "MapR DB JSON source table name",
      displayPosition = 10,
      group = "MAPR_JSON_ORIGIN"
  )
  public String tableName;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Initial Offset",
      description = "Optionally, specify the Initial Offset for the _id column. ",
      displayPosition = 20,
      group = "MAPR_JSON_ORIGIN"
  )
  public String startValue;


  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "4096",
      label = "Max Object Length (chars)",
      description = "Larger objects are not processed",
      displayPosition = 30,
      group = "MAPR_JSON_ORIGIN",
      min = 1,
      max = Integer.MAX_VALUE
  )
  public int jsonMaxObjectLen = 4096;

}
