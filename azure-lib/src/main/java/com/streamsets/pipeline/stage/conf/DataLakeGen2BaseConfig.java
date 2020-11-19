/*
 * Copyright 2019 StreamSets Inc.
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

package com.streamsets.pipeline.stage.conf;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.lib.AzureUtils;
import com.streamsets.pipeline.lib.hdfs.common.HdfsBaseConfigBean;
import com.streamsets.pipeline.stage.conf.connection.ADLSGen2Connection;
import com.streamsets.pipeline.stage.destination.datalake.Errors;
import com.streamsets.pipeline.stage.destination.datalake.gen2.DataLakeGen2TargetGroups;
import com.streamsets.pipeline.stage.destination.hdfs.HadoopConfigBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.streamsets.pipeline.stage.conf.connection.ADLSGen2Properties;
import static com.streamsets.pipeline.stage.conf.connection.ADLSGen2Properties.AUTH_TOKEN_URL;
import static com.streamsets.pipeline.stage.conf.connection.ADLSGen2Properties.FS_AZURE_ACCOUNT_SHARED_KEY_PROPERTY_NAME;

import java.util.ArrayList;
import java.util.List;

/**
 * Contains configurations common to all Generation 2 Data Lake Connectors
 */
public class DataLakeGen2BaseConfig {

  private static final Logger LOG = LoggerFactory.getLogger(DataLakeGen2BaseConfig.class);

  private static final String ADLS_CONFIG_BEAN_PREFIX = "dataLakeConfig.";
  private static final String ADLS_CONFIG_ACCOUNT_FQDN = ADLS_CONFIG_BEAN_PREFIX + "accountFQDN";
  private static final String ADLS_CONFIG_STORAGE_CONTAINER = ADLS_CONFIG_BEAN_PREFIX + "storageContainer";
  private static final String ADLS_CONFIG_OAUTH_CLIENT_CREDS_TOKEN_PROVIDER_VALUE = "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider";
  private static final String ADLS_USER_AGENT_STRING_KEY = "fs.azure.user.agent.prefix";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      connectionType = ADLSGen2Connection.TYPE,
      defaultValue = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL,
      label = "Connection",
      group = "#0",
      displayPosition = -500
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

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "Advanced Configuration",
      description = "Additional HDFS properties to pass to the underlying file system. " +
          "These properties take precedence over those defined in HDFS configuration files.",
      displayPosition = 80,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "DATALAKE"
  )
  @ListBeanModel
  public List<HadoopConfigBean> advancedConfiguration;


  public void init(HdfsBaseConfigBean hdfsBaseConfigBean, final Stage.Context context, List<Stage.ConfigIssue> issues) {
    initHiddenDefaults(hdfsBaseConfigBean);

    String tenantIdString = resolveCredentialValue(context,
        this.connection.tenantId,
        "Tenant ID",
        issues
    );
    String storageContainerString = resolveCredentialValue(context,
        this.connection.storageContainer,
        ADLS_CONFIG_STORAGE_CONTAINER,
        issues
    );
    String accountFQDNString = resolveCredentialValue(context, this.connection.accountFQDN, ADLS_CONFIG_ACCOUNT_FQDN, issues);
    hdfsBaseConfigBean.hdfsUri = buildAbfsUri(storageContainerString, accountFQDNString);

    switch (this.connection.authMethod) {
      case CLIENT:
        hdfsBaseConfigBean.hdfsConfigs.add(new HadoopConfigBean(
            ADLSGen2Properties.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME,
            ADLSGen2Properties.FS_AZURE_ACCOUNT_AUTH_TYPE_DEFAULT_VALUE
        ));
        hdfsBaseConfigBean.hdfsConfigs.add(new HadoopConfigBean(
            ADLSGen2Properties.FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME,
            ADLS_CONFIG_OAUTH_CLIENT_CREDS_TOKEN_PROVIDER_VALUE
        ));
        hdfsBaseConfigBean.hdfsConfigs.add(new HadoopConfigBean(
            ADLSGen2Properties.FS_AZURE_ACCOUNT_OAUTH_CLIENT_ENDPOINT,
            String.format(AUTH_TOKEN_URL, tenantIdString)
        ));
        hdfsBaseConfigBean.hdfsConfigs.add(new HadoopConfigBean(
            ADLSGen2Properties.FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID,
            this.connection.clientId
        ));
        hdfsBaseConfigBean.hdfsConfigs.add(new HadoopConfigBean(
            ADLSGen2Properties.FS_AZURE_ACCOUNT_OAUTH_CLIENT_SECRET,
            this.connection.clientKey
        ));
        break;

      case SHARED_KEY:
        hdfsBaseConfigBean.hdfsConfigs.add(new HadoopConfigBean(
            String.format(FS_AZURE_ACCOUNT_SHARED_KEY_PROPERTY_NAME, accountFQDNString),
            this.connection.accountKey
        ));
        break;
    }

    hdfsBaseConfigBean.hdfsConfigs.addAll(this.advancedConfiguration);

    hdfsBaseConfigBean.hdfsConfigs.add(new HadoopConfigBean(
        ADLS_USER_AGENT_STRING_KEY,
        AzureUtils.buildUserAgentString(context)
    ));
  }

  private String resolveCredentialValue(final Stage.Context context, CredentialValue credentialValue, String configName, List<Stage.ConfigIssue> issues) {
    try {
      return credentialValue.get();
    } catch (StageException e) {
      LOG.error(Errors.ADLS_15.getMessage(), e.toString(), e);
      issues.add(context.createConfigIssue(
          DataLakeGen2TargetGroups.DATALAKE.name(),
          configName,
          Errors.ADLS_15,
          e.toString()
      ));
    }
    return null;
  }

  private void initHiddenDefaults(HdfsBaseConfigBean hdfsBaseConfigBean) {
    hdfsBaseConfigBean.hdfsUser = "";
    hdfsBaseConfigBean.hdfsKerberos = false;
    hdfsBaseConfigBean.hdfsConfDir = "";
    hdfsBaseConfigBean.hdfsConfigs = new ArrayList<>();
  }

  private String buildAbfsUri(String container, String accountFQDN) {
    String abfsProtocol = this.connection.secureConnection ?
        DataLakeConnectionProtocol.ABFS_PROTOCOL_SECURE.getProtocol() :
        DataLakeConnectionProtocol.ABFS_PROTOCOL.getProtocol();
    return abfsProtocol + container + "@" + accountFQDN;
  }
}
