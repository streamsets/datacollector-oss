/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.pipeline.stage.conf.connection;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.ConnectionVerifier;
import com.streamsets.pipeline.api.ConnectionVerifierDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.HideStage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.stage.conf.DataLakeGen2BaseConfig;
import com.streamsets.pipeline.stage.destination.datalake.Errors;
import com.streamsets.pipeline.stage.destination.hdfs.HadoopConfigBean;
import com.streamsets.pipeline.stage.origin.datalake.gen2.DataLakeGen2SourceConfigBean;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

@StageDef(
    version = 1,
    label = "Azure Data Lake Storage Gen2 Connection Verifier",
    description = "Verifies connection to Azure Data Lake Storage Gen2",
    upgraderDef = "upgrader/DataLakeGen2ConnectionVerifierUpgrader.yaml",
    onlineHelpRefUrl = ""
)
@HideStage(HideStage.Type.CONNECTION_VERIFIER)
@ConfigGroups(AzureConnectionGroups.class)
@ConnectionVerifierDef(
    verifierType = ADLSGen2Connection.TYPE,
    connectionFieldName = "connection",
    connectionSelectionFieldName = "connectionSelection"
)
public class DataLakeGen2ConnectionVerifier extends ConnectionVerifier {

  private final static Logger LOG = LoggerFactory.getLogger(ADLSGen2Connection.class);

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      connectionType = ADLSGen2Connection.TYPE,
      defaultValue = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL,
      label = "Connection"
  )
  @ValueChooserModel(ConnectionDef.Constants.ConnectionChooserValues.class)
  public String connectionSelection = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL;

  @ConfigDefBean(
      dependencies = {
          @Dependency(
              configName = "connectionSelection",
              triggeredByValues = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL
          )
      }
  )
  public ADLSGen2Connection connection;

  @Override
  protected List<ConfigIssue> initConnection() {
    List<ConfigIssue> issues = new ArrayList<>();

    Path testPath = new Path("ss-" + UUID.randomUUID().toString());

    DataLakeGen2BaseConfig conf = new DataLakeGen2BaseConfig();
    conf.connection = connection;

    // Adding dummy configuration for Advanced Configuration. Not including anything will throw an NPE
    // when running sourceConf.init below.
    conf.advancedConfiguration = Arrays.asList(new HadoopConfigBean("sample.config", "ok"));

    DataLakeGen2SourceConfigBean sourceConf = new DataLakeGen2SourceConfigBean();
    sourceConf.dataLakeConfig = conf;
    sourceConf.init(getContext(), issues);

    // An incorrect hostname will time out dynamic preview, so we will perform a basic validation that the host is
    // reachable. This is in lieu of using the Hadoop client to configure a timeout, which we have not been
    // successful at implementing.
    if (issues.isEmpty()) {
      try {
        URL endpoint = new URL("https://" + conf.connection.accountFQDN.get());
        HttpURLConnection urlConnection = (HttpURLConnection) endpoint.openConnection();
        urlConnection.connect();
      } catch (Exception e) {
        LOG.debug(Errors.ADLS_02.getMessage(), e.getMessage(), e);
        issues.add(getContext().createConfigIssue("ADLS", "connection", Errors.ADLS_02, e.toString()));
      }
    }

    if (issues.isEmpty()) {
      try {
        FileSystem fs = sourceConf.getFileSystem();
        fs.create(testPath);
        fs.delete(testPath, true);
      } catch (Exception e) {
        LOG.debug(Errors.ADLS_02.getMessage(), e.getMessage(), e);
        issues.add(getContext().createConfigIssue("ALDS", "connection", Errors.ADLS_02, e.toString()));
      }
    }

    return issues;
  }

}
