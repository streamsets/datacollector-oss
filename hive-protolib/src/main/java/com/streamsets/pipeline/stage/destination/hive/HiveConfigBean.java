/**
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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.destination.hive;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.stage.lib.hive.HiveMetastoreUtil;

import java.util.Map;

public class HiveConfigBean {
  @ConfigDef(
      required = true,
      label = "Hive JDBC URL",
      type = ConfigDef.Type.STRING,
      description = "Hive JDBC URL",
      defaultValue = "jdbc:hive2://<host>:<port>/" +
          "${record:value('"+ HiveMetastoreUtil.SEP + HiveMetastoreUtil.DATABASE_FIELD+"')}",
      displayPosition= 10,
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      elDefs = {RecordEL.class},
      group = "HIVE"
  )
  public String hiveJDBCUrl;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "org.apache.hive.jdbc.HiveDriver",
      label = "Hive JDBC Driver Name",
      description = "The Fully Qualifed Hive JDBC Drive Class Name",
      displayPosition = 20,
      group = "HIVE"
  )
  public String hiveJDBCDriver;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "/etc/hive/conf",
      label = "Configuration Directory",
      description = "An absolute path or a directory under SDC resources directory to load core-site.xml, hdfs-site.xml and" +
          " hive-site.xml files to configure the Hive Metastore.",
      displayPosition = 20,
      group = "HIVE"
  )
  public String confDir;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      label = "Additional Hadoop Configuration",
      description = "Additional configuration properties. Values here override values loaded from config files.",
      displayPosition = 90,
      group = "ADVANCED"
  )
  public Map<String, String> additionalConfigProperties;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "-1",
      label = "Hive Metastore Cache Size",
      description = "Cache Size",
      displayPosition = 30,
      group = "ADVANCED"
  )
  public long maxCacheSize = -1L;
}
