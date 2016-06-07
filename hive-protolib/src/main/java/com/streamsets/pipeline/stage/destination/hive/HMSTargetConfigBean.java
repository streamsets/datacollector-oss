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

import com.google.common.base.Joiner;
import com.streamsets.datacollector.security.HadoopSecurityUtil;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.stage.lib.hive.Errors;
import com.streamsets.pipeline.stage.lib.hive.Groups;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PrivilegedExceptionAction;
import java.util.List;

public class HMSTargetConfigBean {
  private static final Logger LOG = LoggerFactory.getLogger(HMSTargetConfigBean.class.getCanonicalName());
  private static final Joiner JOINER = Joiner.on(".");
  private static final String HIVE_CONFIG_BEAN = "hiveConfigBean";
  private static final String CONF_DIR = "confDir";


  @ConfigDefBean
  public HiveConfigBean hiveConfigBean;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Data Format",
      displayPosition = 20,
      group = "HIVE"
  )
  @ValueChooserModel(HMSDataFormatChooserValues.class)
  public DataFormat dataFormat = DataFormat.AVRO;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Use as Avro",
      description = "Specifies Whether the table properties should not contain the schema url",
      defaultValue = "true",
      dependsOn = "dataFormat",
      triggeredByValue = "AVRO",
      displayPosition = 30,
      group = "ADVANCED"
  )
  public boolean useAsAvro = true;

  //Same as in HDFS origin.
  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "HDFS User",
      description = "If set, the data collector will serialize avro " +
          "schemas in HDFS with specified hdfs user. The data collector" +
          " user must be configured as a proxy user in HDFS.",
      displayPosition = 40,
      group = "ADVANCED",
      dependsOn = "useAsAvro",
      triggeredByValue = "false"
  )
  public String hdfsUser;

  private FileSystem fs;

  public FileSystem getFileSystem() {
    return fs;
  }

  public UserGroupInformation getHDFSUgi() {
    return (hdfsUser == null || hdfsUser.isEmpty())?
        hiveConfigBean.getUgi() : HadoopSecurityUtil.getProxyUser(hdfsUser, hiveConfigBean.getUgi());
  }

  public void destroy() {
    if (useAsAvro) {
      return;
    }
    try {
      getHDFSUgi().doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          if (fs != null) {
            fs.close();
          }
          return null;
        }
      });
    } catch (Exception e) {
      LOG.warn("Error when closing hdfs file system:", e);
    }
  }

  public void init(final Stage.Context context, final String prefix, final List<Stage.ConfigIssue> issues) {
    hiveConfigBean.init(context, JOINER.join(prefix, HIVE_CONFIG_BEAN), issues);
    if (useAsAvro) {
      return;
    }
    //use ugi.
    try {
      fs = getHDFSUgi().doAs(new PrivilegedExceptionAction<FileSystem>() {
        @Override
        public FileSystem run() throws Exception{
          return FileSystem.get(hiveConfigBean.getConfiguration());
        }
      });
    } catch (Exception e) {
      LOG.error("Error accessing HDFS", e);
      issues.add(
          context.createConfigIssue(
              Groups.HIVE.name(),
              JOINER.join(prefix, HIVE_CONFIG_BEAN, CONF_DIR),
              Errors.HIVE_01,
              e.getMessage()
          )
      );
    }
  }
}