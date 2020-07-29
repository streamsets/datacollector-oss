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
package com.streamsets.pipeline.stage.destination.hdfs.metadataexecutor;

import com.google.common.base.Joiner;
import com.streamsets.datacollector.security.HadoopSecurityUtil;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Stage;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class HdfsConnectionConfig {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsConnectionConfig.class);
  private static Joiner JOIN = Joiner.on(".");

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.STRING,
    label = "Hadoop FS URI",
    description = "",
    displayPosition = 10,
    displayMode = ConfigDef.DisplayMode.BASIC,
    group = "HDFS"
  )
  public String hdfsUri;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.STRING,
    label = "HDFS User",
    description = "If set, the data collector will write to HDFS as this user. " +
      "The data collector user must be configured as a proxy user in HDFS.",
    displayPosition = 20,
    displayMode = ConfigDef.DisplayMode.BASIC,
    group = "HDFS"
  )
  public String hdfsUser = "";

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.BOOLEAN,
    label = "Kerberos Authentication",
    defaultValue = "false",
    description = "",
    displayPosition = 30,
    displayMode = ConfigDef.DisplayMode.BASIC,
    group = "HDFS"
  )
  public boolean hdfsKerberos;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.STRING,
    defaultValue = "",
    label = "Hadoop FS Configuration Directory",
    description = "An SDC resource directory or symbolic link with HDFS configuration files core-site.xml and hdfs-site.xml",
    displayPosition = 50,
    displayMode = ConfigDef.DisplayMode.BASIC,
    group = "HDFS"
  )
  public String hdfsConfDir;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.MAP,
    label = "Hadoop FS Configuration",
    description = "Additional Hadoop properties to pass to the underlying Hadoop FileSystem. These properties " +
      "have precedence over properties loaded via the 'Hadoop FS Configuration Directory' property.",
    displayPosition = 60,
    displayMode = ConfigDef.DisplayMode.BASIC,
    group = "HDFS"
  )
  public Map<String, String> hdfsConfigs = Collections.emptyMap();

  protected Configuration conf;
  protected UserGroupInformation loginUgi;
  protected UserGroupInformation userUgi;
  protected FileSystem fs;

  public UserGroupInformation getUGI() {
    return userUgi;
  }

  public FileSystem getFs() {
    return fs;
  }

  public void init(Stage.Context context, String prefix, List<Stage.ConfigIssue> issues) {
    conf = new Configuration();
    conf.setClass("fs.file.impl", RawLocalFileSystem.class, FileSystem.class);

    if (hdfsKerberos) {
      conf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION,
        UserGroupInformation.AuthenticationMethod.KERBEROS.name());
      try {
        conf.set(DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY, "hdfs/_HOST@" + HadoopSecurityUtil.getDefaultRealm());
      } catch (Exception ex) {
        if (!hdfsConfigs.containsKey(DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY)) {
          issues.add(context.createConfigIssue(Groups.HDFS.name(), null, HdfsMetadataErrors.HDFS_METADATA_001, ex.toString()));
        }
      }
    }

    if (hdfsConfDir != null && !hdfsConfDir.isEmpty()) {
      File hadoopConfigDir = new File(hdfsConfDir);
      if (!hadoopConfigDir.isAbsolute()) {
        hadoopConfigDir = new File(context.getResourcesDirectory(), hdfsConfDir).getAbsoluteFile();
      }
      if (!hadoopConfigDir.exists()) {
        issues.add(
          context.createConfigIssue(
            Groups.HDFS.name(),
            JOIN.join(prefix, "hdfsConfDir"),
            HdfsMetadataErrors.HDFS_METADATA_002,
            hadoopConfigDir.getPath()
          )
        );
      } else if (!hadoopConfigDir.isDirectory()) {
        issues.add(
          context.createConfigIssue(
            Groups.HDFS.name(),
            JOIN.join(prefix, "hdfsConfDir"),
            HdfsMetadataErrors.HDFS_METADATA_003,
            hadoopConfigDir.getPath()
          )
        );
      } else {
        File coreSite = new File(hadoopConfigDir, "core-site.xml");
        if (coreSite.exists()) {
          if (!coreSite.isFile()) {
            issues.add(
              context.createConfigIssue(
                Groups.HDFS.name(),
                JOIN.join(prefix, "hdfsConfDir"),
                HdfsMetadataErrors.HDFS_METADATA_004,
                coreSite.getPath()
              )
            );
          }
          conf.addResource(new Path(coreSite.getAbsolutePath()));
        }
        File hdfsSite = new File(hadoopConfigDir, "hdfs-site.xml");
        if (hdfsSite.exists()) {
          if (!hdfsSite.isFile()) {
            issues.add(
              context.createConfigIssue(
                Groups.HDFS.name(),
                JOIN.join(prefix, "hdfsConfDir"),
                HdfsMetadataErrors.HDFS_METADATA_004,
                hdfsSite.getPath()
              )
            );
          }
          conf.addResource(new Path(hdfsSite.getAbsolutePath()));
        }
      }
    }

    // Unless user specified non-empty, non-null HDFS URI, we need to retrieve it's value
    if(StringUtils.isEmpty(hdfsUri)) {
      hdfsUri = conf.get("fs.defaultFS");
    }

    for (Map.Entry<String, String> config : hdfsConfigs.entrySet()) {
      conf.set(config.getKey(), config.getValue());
    }

    try {
      loginUgi = HadoopSecurityUtil.getLoginUser(conf);
      userUgi = HadoopSecurityUtil.getProxyUser(
        hdfsUser,
        context,
        loginUgi,
        issues,
        Groups.HDFS.name(),
        JOIN.join(prefix, "hdfsUser")
      );
    } catch (IOException e) {
      LOG.error("Can't create UGI", e);
      issues.add(context.createConfigIssue(Groups.HDFS.name(), null, HdfsMetadataErrors.HDFS_METADATA_005, e.getMessage(), e));
    }

    if(!issues.isEmpty()) {
      return;
    }

    try {
      fs = getUGI().doAs((PrivilegedExceptionAction<FileSystem>) () -> FileSystem.newInstance(new URI(hdfsUri), conf));
    } catch (Exception ex) {
      LOG.error("Can't retrieve FileSystem instance", ex);
      issues.add(context.createConfigIssue(Groups.HDFS.name(), null, HdfsMetadataErrors.HDFS_METADATA_005, ex.getMessage(), ex));
    }
  }

  public void destroy() {
    try {
      if(fs != null) {
        getUGI().doAs((PrivilegedExceptionAction<Void>) () -> {
          fs.close();
          return null;
        });
      }
    } catch (IOException|InterruptedException e) {
      LOG.error("Ignoring exception when closing HDFS client", e);
    }
  }


}
