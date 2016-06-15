/*
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.snmp;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.ValueChooserModel;

import java.util.List;

@GenerateResourceBundle
public class SnmpHostConfig {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "CIDR Range",
      description = "A CIDR range such as 10.0.0.0/8 or 172.31.0.1/32",
      displayPosition = 10,
      group = "#@"
  )
  public String range;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.LIST,
      label = "Global OIDs",
      description = "List of supplemental OIDs to fetch from this range",
      displayPosition = 20,
      group = "#@"
  )
  public List<String> oids;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.LIST,
      label = "Global OIDs",
      description = "List of OIDs to exclude from this range",
      displayPosition = 30,
      group = "#@"
  )
  public List<String> exclusions;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Transport",
      description = "Method in which to connect to the specified hosts",
      displayPosition = 40,
      group = "#0"
  )
  @ValueChooserModel(TransportTypeChooserValues.class)
  public TransportType transportType;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "V2C",
      label = "Version",
      displayPosition = 50,
      group = "#0"
  )
  @ValueChooserModel(SnmpVersionChooserValues.class)
  public SnmpVersion version = SnmpVersion.V2C;
}
