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
package com.streamsets.pipeline.stage.destination.mapreduce.config;

import com.google.common.base.Joiner;
import com.streamsets.datacollector.security.HadoopSecurityUtil;
import com.streamsets.datacollector.stage.HadoopConfigurationUtils;
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
    description = "Directory containing configuration files for MapReduce (core-site.xml, yarn-site.xml, hdfs-site.xml and mapred-site.xml)",
    displayPosition = 10,
    displayMode = ConfigDef.DisplayMode.BASIC,
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
    displayMode = ConfigDef.DisplayMode.BASIC,
    group = "MAPREDUCE"
  )
  public Map<String, String> mapreduceConfigs;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.STRING,
    label = "MapReduce User",
    description = "If set, Data Collector will start the MapReduce job as this user. " +
      "The Data Collector user must be configured as a proxy user in the cluster.",
    displayPosition = 30,
    displayMode = ConfigDef.DisplayMode.BASIC,
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
    displayMode = ConfigDef.DisplayMode.BASIC,
    group = "MAPREDUCE"
  )
  public boolean kerberos;

  private Configuration configuration;
  public Configuration getConfiguration() {
    return configuration;
  }
  private UserGroupInformation loginUgi;
  private UserGroupInformation userUgi;
  public UserGroupInformation getUGI() {
    return userUgi;
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
    addResourceFileToConfig(prefix, confDir, "hdfs-site.xml", context, issues);
    addResourceFileToConfig(prefix, confDir, "yarn-site.xml", context, issues);
    addResourceFileToConfig(prefix, confDir, "mapred-site.xml", context, issues);

    // Add any additional configuration overrides
    for (Map.Entry<String, String> entry : mapreduceConfigs.entrySet()) {
      configuration.set(entry.getKey(), entry.getValue());
    }

    // See SDC-5451, where we set "hadoop.treat.subject.external" automatically
    // in order to take advantage of HADOOP-13805
    HadoopConfigurationUtils.configureHadoopTreatSubjectExternal(configuration);

    // We're doing here the same as HDFS (which should be at some point refactored to shared code)
    try {
      loginUgi = HadoopSecurityUtil.getLoginUser(configuration);
      userUgi = HadoopSecurityUtil.getProxyUser(
        mapreduceUser,
        context,
        loginUgi,
        issues,
        Groups.MAPREDUCE.name(),
        Joiner.on(".").join(prefix, "mapreduceUser")
      );
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
      LOG.debug("Loading configuration file: {}", confFile.getAbsoluteFile());
      configuration.addResource(new Path(confFile.getAbsolutePath()));
    }
  }

}
