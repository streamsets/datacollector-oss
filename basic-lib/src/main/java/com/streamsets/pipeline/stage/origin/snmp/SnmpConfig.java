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
package com.streamsets.pipeline.stage.origin.snmp;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.lib.snmp.SnmpHostConfig;

import java.util.List;

@GenerateResourceBundle
public class SnmpConfig {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.LIST,
      label = "Global OIDs",
      description = "List of OIDs to fetch from all hosts",
      displayPosition = 10,
      group = "#@"
  )
  public List<String> oids;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Hosts",
      description = "Hosts in CIDR notation to poll",
      displayPosition = 20,
      group = "#@"
  )
  @ListBeanModel
  public List<SnmpHostConfig> hosts;

}
