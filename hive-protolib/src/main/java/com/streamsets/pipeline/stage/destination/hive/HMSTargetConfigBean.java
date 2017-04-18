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
import com.streamsets.datacollector.security.HadoopConfigConstants;
import com.streamsets.datacollector.security.HadoopSecurityUtil;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.StringEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.stage.lib.hive.Errors;
import com.streamsets.pipeline.stage.lib.hive.HiveConfigBean;
import com.streamsets.pipeline.stage.lib.hive.HiveMetastoreUtil;
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
      type = ConfigDef.Type.BOOLEAN,
      label = "Stored as Avro",
      description = "If the table is Avro, then use to include the Stored as Avro clause in the table creation SQL." +
          " When selected, the Avro schema URL will not be included in the query.",
      defaultValue = "true",
      displayPosition = 30,
      group = "ADVANCED"
  )
  public boolean storedAsAvro = true;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Schema Folder Location",
      description = "If specified, the data collector will use the hdfs location for serializing avro schemas." +
          " If the path does not start with '/' (relative) it will be relative to table data location in hdfs",
      displayPosition = 40,
      group = "ADVANCED",
      defaultValue = ".schemas",
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      elDefs = {RecordEL.class, StringEL.class, TimeEL.class},
      dependsOn = "storedAsAvro",
      triggeredByValue = "false"
  )
  public String schemaFolderLocation;

  //Same as in HDFS origin.
  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "HDFS User",
      description = "If specified, the data collector will serialize avro " +
          "schemas in HDFS with specified hdfs user. The data collector" +
          " user must be configured as a proxy user in HDFS.",
      displayPosition = 50,
      group = "ADVANCED",
      dependsOn = "storedAsAvro",
      triggeredByValue = "false"
  )
  public String hdfsUser;

  private UserGroupInformation userUgi;
  private FileSystem fs;
  private ELEval schemaFolderELEval;

  public FileSystem getFileSystem() {
    return fs;
  }

  public String getSchemaFolderLocation(Stage.Context context, Record metadataRecord) throws ELEvalException {
    ELVars vars = context.createELVars();
    RecordEL.setRecordInContext(vars, metadataRecord);
    return HiveMetastoreUtil.resolveEL(schemaFolderELEval, vars, schemaFolderLocation);
  }

  public UserGroupInformation getHDFSUgi() {
    return userUgi;
  }

  public void destroy() {
    hiveConfigBean.destroy();
    if (storedAsAvro) {
      return;
    }
    try {
      getHDFSUgi().doAs((PrivilegedExceptionAction<Void>) () -> {
        if (fs != null) {
          fs.close();
        }
        return null;
      });
    } catch (Exception e) {
      LOG.warn("Error when closing hdfs file system:", e);
    }
  }

  public void init(final Stage.Context context, final String prefix, final List<Stage.ConfigIssue> issues) {
    hiveConfigBean.init(context, JOINER.join(prefix, HIVE_CONFIG_BEAN), issues);
    userUgi = HadoopSecurityUtil.getProxyUser(
      hdfsUser,
      context,
      hiveConfigBean.getUgi(),
      issues,
      Groups.HIVE.name(),
      JOINER.join(prefix, HIVE_CONFIG_BEAN, "hdfsUser")
    );
    schemaFolderELEval = context.createELEval("schemaFolderLocation");
    if (storedAsAvro) {
      return;
    }
    //use ugi.
    try {
      fs = getHDFSUgi().doAs((PrivilegedExceptionAction<FileSystem>) () -> FileSystem.get(hiveConfigBean.getConfiguration()));
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