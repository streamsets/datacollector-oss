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
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.lib.AzureUtils;
import com.streamsets.pipeline.lib.hdfs.common.HdfsBaseConfigBean;
import com.streamsets.pipeline.stage.destination.datalake.Errors;
import com.streamsets.pipeline.stage.destination.datalake.gen2.DataLakeGen2TargetGroups;
import com.streamsets.pipeline.stage.destination.hdfs.HadoopConfigBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final String ADLS_CONFIG_AUTH_TYPE_KEY = "fs.azure.account.auth.type";
  private static final String ADLS_CONFIG_AUTH_TYPE_DEFAULT_VALUE = "OAuth";

  private static final String ADLS_CONFIG_OAUTH_PROVIDER_TYPE_KEY = "fs.azure.account.oauth.provider.type";
  private static final String ADLS_CONFIG_OAUTH_CLIENT_CREDS_TOKEN_PROVIDER_VALUE = "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider";

  private static final String ADLS_CONFIG_AUTH_ENDPOINT_KEY = "fs.azure.account.oauth2.client.endpoint";
  private static final String ADLS_CONFIG_CLIENT_ID_KEY = "fs.azure.account.oauth2.client.id";
  private static final String ADLS_CONFIG_CLIENT_SECRET_KEY = "fs.azure.account.oauth2.client.secret";

  private static final String ADLS_USER_AGENT_STRING_KEY = "fs.azure.user.agent.prefix";

  private static final String ABFS_CONFIG_ACCOUNT_PREFIX = "fs.azure.account.key.";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      defaultValue = "example.dfs.core.windows.net",
      label = "Account FQDN",
      description = "The fully qualified domain name of the Data Lake Storage account",
      displayPosition = 10,
      group = "DATALAKE"
  )
  public CredentialValue accountFQDN;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      defaultValue = "example-blob-container",
      label = "Storage Container / File System",
      description = "Name of the storage container or file system in the storage account",
      displayPosition = 20,
      group = "DATALAKE"
  )
  public CredentialValue storageContainer;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "OAUTH",
      label = "Authentication Method",
      description = "Method used to authenticate connections to Azure",
      displayPosition = 30,
      group = "DATALAKE"
  )
  @ValueChooserModel(AuthMethodGen2ChooserValues.class)
  public AuthMethodGen2 authMethod;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      defaultValue = "",
      label = "Application ID",
      description = "Azure application ID",
      displayPosition = 40,
      group = "DATALAKE",
      dependsOn = "authMethod",
      triggeredByValue = "OAUTH"
  )
  public CredentialValue clientId;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      defaultValue = "https://login.microsoftonline.com/example-example",
      label = "Auth Token Endpoint",
      description = "Azure auth token endpoint",
      displayPosition = 50,
      group = "DATALAKE",
      dependsOn = "authMethod",
      triggeredByValue = "OAUTH"
  )
  public CredentialValue authTokenEndpoint;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      defaultValue = "",
      label = "Application Key",
      description = "Azure application key",
      displayPosition = 60,
      group = "DATALAKE",
      dependsOn = "authMethod",
      triggeredByValue = "OAUTH"
  )
  public CredentialValue clientKey;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      defaultValue = "",
      label = "Account Shared Key",
      description = "Azure storage account shared key",
      displayPosition = 70,
      group = "DATALAKE",
      dependsOn = "authMethod",
      triggeredByValue = "SHARED_KEY"
  )
  public CredentialValue accountKey;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Secure Connection",
      defaultValue = "false",
      description = "Enable a secure connection using abfss",
      displayPosition = 75,
      group = "DATALAKE"
  )
  public boolean secureConnection;

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

    String storageContainerString = resolveCredentialValue(context,
        this.storageContainer,
        ADLS_CONFIG_STORAGE_CONTAINER,
        issues
    );
    String accountFQDNString = resolveCredentialValue(context, this.accountFQDN, ADLS_CONFIG_ACCOUNT_FQDN, issues);
    hdfsBaseConfigBean.hdfsUri = buildAbfsUri(storageContainerString, accountFQDNString);

    switch (this.authMethod) {
      case OAUTH:
        hdfsBaseConfigBean.hdfsConfigs.add(new HadoopConfigBean(ADLS_CONFIG_AUTH_TYPE_KEY,
            ADLS_CONFIG_AUTH_TYPE_DEFAULT_VALUE
        ));
        hdfsBaseConfigBean.hdfsConfigs.add(new HadoopConfigBean(ADLS_CONFIG_OAUTH_PROVIDER_TYPE_KEY,
            ADLS_CONFIG_OAUTH_CLIENT_CREDS_TOKEN_PROVIDER_VALUE
        ));
        hdfsBaseConfigBean.hdfsConfigs.add(new HadoopConfigBean(ADLS_CONFIG_AUTH_ENDPOINT_KEY, this.authTokenEndpoint));
        hdfsBaseConfigBean.hdfsConfigs.add(new HadoopConfigBean(ADLS_CONFIG_CLIENT_ID_KEY, this.clientId));
        hdfsBaseConfigBean.hdfsConfigs.add(new HadoopConfigBean(ADLS_CONFIG_CLIENT_SECRET_KEY, this.clientKey));
        break;
      case SHARED_KEY:
        String propertyName = ABFS_CONFIG_ACCOUNT_PREFIX + accountFQDNString;
        hdfsBaseConfigBean.hdfsConfigs.add(new HadoopConfigBean(propertyName, this.accountKey));
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
    String abfsProtocol = secureConnection ?
        DataLakeConnectionProtocol.ABFS_PROTOCOL_SECURE.getProtocol() :
        DataLakeConnectionProtocol.ABFS_PROTOCOL.getProtocol();
    return abfsProtocol + container + "@" + accountFQDN;
  }
}
