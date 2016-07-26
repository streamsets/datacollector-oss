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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.destination.mapreduce.config;

import com.google.common.base.Joiner;
import com.streamsets.datacollector.security.HadoopSecurityUtil;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.stage.destination.mapreduce.Groups;
import com.streamsets.pipeline.stage.destination.mapreduce.MapReduceErrors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MapReduceConfig {
  private static final Logger LOG = LoggerFactory.getLogger(MapReduceConfig.class);

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "/etc/hadoop/conf/",
    label = "MapReduce Configuration Directory",
    description = "Directory containing configuration files for MapReduce (core-site.xml, yarn-site.xml and mapreduce-site.xml)",
    displayPosition = 10,
    group = "MAPREDUCE"
  )
  public String mapReduceConfDir;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.MAP,
    label = "MapReduce Configuration",
    description = "Additional Hadoop properties to pass to the underlying Hadoop Configuration. These properties " +
      "have precedence over properties loaded via the 'MapReduce Configuration Directory' property.",
    displayPosition = 20,
    group = "MAPREDUCE"
  )
  public Map<String, String> mapreduceConfigs;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.STRING,
    label = "MapReduce User",
    description = "If set, the data collector will start the MapReduce job as this user. " +
      "The data collector user must be configured as a proxy user in the cluster.",
    displayPosition = 30,
    group = "MAPREDUCE"
  )
  public String mapreduceUser;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.BOOLEAN,
    label = "Kerberos Authentication",
    defaultValue = "false",
    description = "",
    displayPosition = 40,
    group = "MAPREDUCE"
  )
  public boolean kerberos;

  private Configuration configuration;
  public Configuration getConfiguration() {
    return configuration;
  }
  private UserGroupInformation loginUgi;
  public UserGroupInformation getUGI() {
    return mapreduceUser.isEmpty() ? loginUgi : HadoopSecurityUtil.getProxyUser(mapreduceUser, loginUgi);
  }

  public List<Stage.ConfigIssue> init(Stage.Context context, String prefix) {
    List<Stage.ConfigIssue> issues = new ArrayList<>();

    // Prepare configuration object
    File confDir = new File(mapReduceConfDir);
    if (!confDir.isAbsolute()) {
      confDir = new File(context.getResourcesDirectory(), mapReduceConfDir).getAbsoluteFile();
    }

    configuration = new Configuration();

    if (!confDir.exists()) {
      issues.add(context.createConfigIssue(
        Groups.MAPREDUCE.name(),
        Joiner.on(".").join(prefix, "mapReduceConfDir"),
        MapReduceErrors.MAPREDUCE_0003,
        confDir.getAbsolutePath()
      ));
      return issues;
    }

    addResourceFileToConfig(prefix, confDir, "core-site.xml", context, issues);
    addResourceFileToConfig(prefix, confDir, "yarn-site.xml", context, issues);
    addResourceFileToConfig(prefix, confDir, "mapred-site.xml", context, issues);

    // Add any additional configuration overrides
    for (Map.Entry<String, String> entry : mapreduceConfigs.entrySet()) {
      configuration.set(entry.getKey(), entry.getValue());
    }

    // We're doing here the same as HDFS (which should be at some point refactored to shared code)
    try {
      loginUgi = HadoopSecurityUtil.getLoginUser(configuration);
      if(kerberos) {
        if (loginUgi.getAuthenticationMethod() != UserGroupInformation.AuthenticationMethod.KERBEROS) {
          issues.add(context.createConfigIssue(
            Groups.MAPREDUCE.name(),
            Joiner.on(".").join(prefix, "kerberos"),
            MapReduceErrors.MAPREDUCE_0006,
            loginUgi.getAuthenticationMethod(),
            UserGroupInformation.AuthenticationMethod.KERBEROS
          ));
        }
      } else {
        configuration.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, UserGroupInformation.AuthenticationMethod.SIMPLE.name());
      }
    } catch (IOException e) {
      LOG.error("Can't initialize kerberos", e);
      issues.add(context.createConfigIssue(
        Groups.MAPREDUCE.name(),
        null,
        MapReduceErrors.MAPREDUCE_0000,
        e.getMessage()
      ));
    }

    return issues;
  }

  private void addResourceFileToConfig(String prefix, File confDir, String file, Stage.Context context, List<Stage.ConfigIssue> issues) {
    File confFile = new File(confDir.getAbsolutePath(), file);
    if (!confFile.exists()) {
      issues.add(context.createConfigIssue(
        Groups.MAPREDUCE.name(),
        Joiner.on(".").join(prefix, "mapReduceConfDir"),
        MapReduceErrors.MAPREDUCE_0002,
        confFile.getAbsolutePath())
      );
    } else {
      configuration.addResource(new Path(confFile.getAbsolutePath()));
    }
  }

}
